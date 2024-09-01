package run.ecommerce.cdc.commands;


import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.shell.command.annotation.Option;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import run.ecommerce.cdc.model.ConfigParser;
import run.ecommerce.cdc.model.EnvPhp;
import run.ecommerce.cdc.model.MviewXML;

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


    private final ReactiveRedisOperations<String, String> redisOperations;
    private final ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;
    WatchStream(
            ReactiveRedisOperations<String, String> redisOperations,
            ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {
        this.redisOperations = redisOperations;
        this.reactiveRedisConnectionFactory = reactiveRedisConnectionFactory;
    }

    protected String streamPrefix = "";
    protected String group = "";
    protected String consumer = "";
    protected String targetPrefix = "target.";


    @SneakyThrows
    @ShellMethod(key = "watch", value = "Watch stream")
    public String watch(
            @Option(longNames = {"config"}, shortNames = {'c'}, defaultValue = "./config.json") String config
    ) {

        var configObj = ConfigParser.loadConfig(config);

        this.group = configObj.source().group();
        this.consumer = configObj.source().consumer();

        this.targetPrefix = configObj.target().prefix();

        this.SOURCE_READ_COUNT = configObj.buffers().source().size();
        this.SOURCE_READ_TIME = configObj.buffers().source().time();
        this.DEDUPLICATION_SIZE = configObj.buffers().dedupe().size();
        this.DEDUPLICATION_TIME = Duration.ofMillis(configObj.buffers().dedupe().time());
        this.TARGET_BUFFER_SIZE = configObj.buffers().target().size();
        this.TARGET_BUFFER_TIME = Duration.ofMillis(configObj.buffers().target().time());

        this.streamPrefix = streamPrefix;
        this.group = group;
        this.consumer = consumer;

        var targetSinks = generateSinks(configObj);
        var targetFluxes = generateTargetStreams(targetSinks);
        System.out.println("Starting...");
        for(var fluxRec: targetFluxes.entrySet()) {
            fluxRec.getValue().subscribe();
        }
        System.out.println("Starting Redis Consumers");
        var sourceFluxes = generateSourceStreamConsumers(targetSinks, configObj);
        for (var fluxRec : sourceFluxes) {
            fluxRec.subscribe();
        }

        System.out.println("Started");

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
        return "";
    }


    /**
     * Prepare map of all the Skinks(Processing entry points) destinations
     *
     * @param configuration {@link ConfigParser.Config}
     * @return {@link Map<String,Sinks.Many<UnifiedMessage>>}
     */
    protected Map<String, Sinks.Many<UnifiedMessage>> generateSinks(ConfigParser.Config configuration) {
        var sinks = new HashMap<String, Sinks.Many<UnifiedMessage>>();
        for (var processor: configuration.mapping().entrySet()) {
            Sinks.Many<UnifiedMessage> sink = Sinks.many().unicast().onBackpressureBuffer();
            sinks.put(processor.getKey(), sink);
        }
        return sinks;
    }


    protected Map<String,Flux<UnifiedMessage>> generateTargetStreams(
            Map<String, Sinks.Many<UnifiedMessage>> sinks
    ) {
        var fluxes = new HashMap<String,Flux<UnifiedMessage>>();
        for (var record: sinks.entrySet()) {
            var sink = sinks.get(record.getKey());
            var flux = sink.asFlux();

            var targetStreamName = targetPrefix+record.getKey();
            //Create outgoing stream
            redisOperations.opsForStream()
                    .add(targetStreamName,Map.of("ids","[]"))
                    .subscribe();

            var targetFlux = flux
                    .doOnNext(unifiedMessage -> {
                    })
                    .bufferTimeout(DEDUPLICATION_SIZE, DEDUPLICATION_TIME)
                    .map(recordList -> {
                        var nonDuplicateCollection = recordList.stream()
                                .collect(Collectors.toMap(UnifiedMessage::targetId, Function.identity(), (a, b) -> a))
                                .values();

                        return nonDuplicateCollection;
                    })
                    .flatMap(Flux::fromIterable)
                    .bufferTimeout(TARGET_BUFFER_SIZE, TARGET_BUFFER_TIME)
                    .publishOn(Schedulers.boundedElastic())
                    .map(recordList -> {
                        var ids = new JSONArray(
                            recordList.stream()
                                .map(UnifiedMessage::targetId)
                                .toList()
                        ).toString();
                        redisOperations.opsForStream()
                                .add(targetStreamName,Map.of("ids",ids))
                                .subscribe();
                        return recordList;
                    })
                    .flatMap(Flux::fromIterable);
            fluxes.put(record.getKey(), targetFlux);
        }
        return fluxes;
    }

    /**
     * Generate Source job pipelines that are passing data to Target pipelines.
     *
     * @param targetSinks map of all the target Sinks
     * @param config
     * @return {@link List<Flux<UnifiedMessage>>} of source stream processors
     */
    protected List<Flux<UnifiedMessage>> generateSourceStreamConsumers(
            Map<String, Sinks.Many<UnifiedMessage>> targetSinks,
            ConfigParser.Config config) {
        var streamConsumers = new ArrayList<Flux<UnifiedMessage>>();
        for (var streamRecord: config.mapping().entrySet()) {

            var streamName = streamPrefix + streamRecord.getKey();
            var fieldToMap = streamRecord.getValue().entrySet().stream().toList().getFirst().getKey();

            redisOperations.opsForStream().createGroup(streamName, ReadOffset.from("0-0"), group).subscribe();

            var consumerInstance = Consumer.from(group, consumer);

            var options =
                    StreamReceiver.StreamReceiverOptions.builder()
                            .pollTimeout(Duration.ofMillis(SOURCE_READ_TIME))
                            .batchSize(SOURCE_READ_COUNT)
                            .build();

            var receiver = StreamReceiver.create(reactiveRedisConnectionFactory, options);

            Flux<MapRecord<String, String, String>> messages = receiver
                    .receive(consumerInstance, StreamOffset.create(streamName, ReadOffset.lastConsumed()));
            System.out.println(streamRecord.getKey() + ": " );
            var unifiedFlux =  messages
                    .map( record -> {
                        var value = new JSONObject(record.getValue().entrySet().stream().toList().getFirst().getValue());
                        var idToPass = value.getJSONObject("after").get(fieldToMap);

                        return new UnifiedMessage(record.getId(), streamRecord.getKey(), (Integer) idToPass);
                    })
                    .map( record -> {
                        for (var columRecordMap : config.mapping().get(streamRecord.getKey()).entrySet()){
                            for (var processorName: columRecordMap.getValue()) {
                                var targetSink = targetSinks.get(processorName);
                                var res =  targetSink.tryEmitNext(record);
                                if (res.isSuccess()) {
                                } else {
                                    System.out.println("Message NOT processed by target Flux: " + record);
                                }
                            }
                        }
                        return record;
                    })
                    .publishOn(Schedulers.boundedElastic())
                    .map(record -> {
                        redisOperations.opsForStream().acknowledge(streamName, group, record.id()).subscribe();
                        return record;
                    });

            streamConsumers.add(unifiedFlux);
            var msg = "Watching stream " + streamName + " in group " + group + " with consumer " + consumer;
            System.out.println(msg);
        }
        return streamConsumers;
    }
}

