package run.ecommerce.cdc.commands;


import lombok.SneakyThrows;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.shell.command.annotation.Option;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import run.ecommerce.cdc.connection.RedisSource;
import run.ecommerce.cdc.connection.RedisTarget;
import run.ecommerce.cdc.model.ConfigParser;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

@ShellComponent
public class WatchStream {

    protected Integer SOURCE_READ_COUNT;
    protected Integer SOURCE_READ_TIME;
    protected Integer DEDUPLICATION_SIZE;
    protected Duration DEDUPLICATION_TIME;
    protected Integer TARGET_BUFFER_SIZE;
    protected Duration TARGET_BUFFER_TIME;
    protected String streamPrefix = "";
    protected String group = "";
    protected String consumer = "";
    protected String targetPrefix = "target.";

    public final RedisTarget redisTarget;
    public final RedisSource redisSource;
    public CountDownLatch ready = new CountDownLatch(1);
    public CountDownLatch gracefulShutdown;
    private final Logger logger = LoggerFactory.getLogger(WatchStream.class);


    WatchStream(
            RedisSource redisSource,
            RedisTarget redisTarget) {
        this.redisSource = redisSource;
        this.redisTarget = redisTarget;
        this.gracefulShutdown = null;
    }

    @SneakyThrows
    @ShellMethod(key = "watch", value = "Watch stream")
    public String watch(
            @Option(longNames = {"config"}, shortNames = {'c'}, defaultValue = "./config.json") String config
    ) {
        gracefulShutdown = new CountDownLatch(1);
        var configObj = ConfigParser.loadConfig(config);

        this.group = configObj.source().group();
        this.consumer = configObj.source().consumer();
        this.streamPrefix = configObj.source().prefix();
        this.targetPrefix = configObj.target().prefix();

        this.SOURCE_READ_COUNT = configObj.buffers().source().size();
        this.SOURCE_READ_TIME = configObj.buffers().source().time();
        this.DEDUPLICATION_SIZE = configObj.buffers().dedupe().size();
        this.DEDUPLICATION_TIME = Duration.ofMillis(configObj.buffers().dedupe().time());
        this.TARGET_BUFFER_SIZE = configObj.buffers().target().size();
        this.TARGET_BUFFER_TIME = Duration.ofMillis(configObj.buffers().target().time());

        var sourceRedis = new RedisStandaloneConfiguration(
                configObj.source().connection().host(),
                configObj.source().connection().port()
        );
        sourceRedis.setDatabase(configObj.source().connection().db());
        this.redisSource.configure(sourceRedis);

        var targetRedis = new RedisStandaloneConfiguration(
                configObj.target().connection().host(),
                configObj.target().connection().port()
        );
        targetRedis.setDatabase(configObj.target().connection().db());
        this.redisTarget.configure(targetRedis);

        var targetSinks = generateSinks(configObj);
        var targetFluxes = generateTargetStreams(targetSinks);


        logger.info("Starting Producers");
        var targetDisposables = new ArrayList<Disposable>();
        targetFluxes.forEach((key, target) -> {
            targetDisposables.add(target.subscribe());
        });

        logger.info("Starting Consumers");
        var sourceDisposables = new ArrayList<Disposable>();
        var sourceFluxes = generateSourceStreamConsumers(targetSinks, configObj);
        sourceFluxes.forEach(source -> sourceDisposables.add(source.subscribe()));

        logger.info("Started");
        ready.countDown();

        gracefulShutdown.await();

        logger.info("Shutting down");
        sourceDisposables.forEach(Disposable::dispose);
        targetDisposables.forEach(Disposable::dispose);
        logger.info("Stopped");

        return "";
    }


    /**
     * Prepare map of all the Sinks(Processing entry points) destinations
     *
     * @param configuration {@link ConfigParser.Config}
     */
    protected Map<String, Sinks.Many<UnifiedMessage>> generateSinks(ConfigParser.Config configuration) {
        var sinks = new HashMap<String, Sinks.Many<UnifiedMessage>>();

        var targetNames = new HashSet<String>();
        configuration.mapping().forEach((source, columns) -> {
            columns.forEach((key, targetNamesForColumn) -> {
                targetNames.addAll(targetNamesForColumn);
            });
        });

        targetNames.forEach(targetName -> {
            Sinks.Many<UnifiedMessage> sink = Sinks.many().unicast().onBackpressureBuffer();
            sinks.put(targetName, sink);
        });
        return sinks;
    }


    protected Map<String, Flux<UnifiedMessage>> generateTargetStreams(
            Map<String, Sinks.Many<UnifiedMessage>> sinks
    ) {
        var fluxes = new HashMap<String, Flux<UnifiedMessage>>();
        for (var record : sinks.entrySet()) {
            var sink = sinks.get(record.getKey());
            var flux = sink.asFlux();

            var targetStreamName = targetPrefix + record.getKey();
            // Create outgoing stream if not present already.
            redisTarget.operations.opsForStream()
                    .add(targetStreamName, Map.of("ids", "[]"))
                    .block();

            var targetFlux = flux
                    .doOnNext(unifiedMessage -> {
                    })
                    .bufferTimeout(DEDUPLICATION_SIZE, DEDUPLICATION_TIME)
                    .map(recordList -> {
                        var nonDuplicateCollection = recordList.stream()
                                .collect(Collectors.toMap(UnifiedMessage::entityId, Function.identity(), (a, b) -> a));
                        recordList.forEach(message -> {
                            if (nonDuplicateCollection.get(message.entityId()) != message) {
                                // send to acknowledgement flux;
                            }
                        });

                        return nonDuplicateCollection.values();
                    })
                    .flatMap(Flux::fromIterable)
                    .bufferTimeout(TARGET_BUFFER_SIZE, TARGET_BUFFER_TIME)
                    .map(recordList -> {
                        var ids = new JSONArray(
                                recordList.stream()
                                        .map(UnifiedMessage::entityId)
                                        .toList()
                        ).toString();
                        return redisTarget.operations.opsForStream()
                                .add(targetStreamName, Map.of("ids", ids))
                                .then(Mono.just(recordList));
                    })
                    .flatMap(Function.identity())
                    .flatMap(Flux::fromIterable)
                    .map(message -> {
                        logger.debug(message.toString());
                        return message;
                    });
            fluxes.put(record.getKey(), targetFlux);
        }
        return fluxes;
    }

    /**
     * Generate Source job pipelines that are passing data to Target pipelines.
     *
     * @param targetSinks map of all the target Sinks
     * @param config      config to pass to next flux
     * @return list of source stream processors
     */
    protected List<Flux<UnifiedMessage>> generateSourceStreamConsumers(
            Map<String, Sinks.Many<UnifiedMessage>> targetSinks,
            ConfigParser.Config config) {
        var streamConsumers = new ArrayList<Flux<UnifiedMessage>>();
        for (var streamRecord : config.mapping().entrySet()) {

            var streamName = streamPrefix + streamRecord.getKey();
            var fieldToMap = streamRecord.getValue().entrySet().stream().toList().getFirst().getKey();

            var baseStream =
                    redisSource.getStream(streamName, fieldToMap,
                            new RedisSource.Config(
                                    group, consumer,
                                    SOURCE_READ_TIME, SOURCE_READ_COUNT,
                                    SOURCE_READ_TIME, SOURCE_READ_COUNT, SOURCE_READ_TIME * 10
                            )
                    );


            var unifiedFlux = baseStream
                    .map(record -> {
                        // Send to Acknowledgement flux that there will be incoming
                        for (var columRecordMap : config.mapping().get(streamRecord.getKey()).entrySet()) {
                            for (var processorName : columRecordMap.getValue()) {
                                var targetSink = targetSinks.get(processorName);
                                var res = targetSink.tryEmitNext(record);
                                if (res.isSuccess()) {
                                    // sendto acknowledgement sync
                                } else {
                                    System.out.println("Message NOT processed by target Flux: " + record);
                                }
                            }
                        }
                        return record;
                    })
                    .map(record -> {
                        redisSource.operations.opsForStream().acknowledge(streamName, group, record.id()).subscribe();
                        return record;
                    });

            streamConsumers.add(unifiedFlux);
        }
        return streamConsumers;
    }
}

