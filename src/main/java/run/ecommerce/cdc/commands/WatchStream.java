package run.ecommerce.cdc.commands;


import lombok.Setter;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.springframework.shell.command.annotation.Option;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import run.ecommerce.cdc.model.ConfigParser;
import run.ecommerce.cdc.source.Redis;

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

    private final Redis redis;
    public CountDownLatch latch;
    WatchStream(
            Redis redis) {
        this.redis = redis;
        this.latch = null;
    }
    @SneakyThrows
    @ShellMethod(key = "watch", value = "Watch stream")
    public String watch(
            @Option(longNames = {"config"}, shortNames = {'c'}, defaultValue = "./config.json") String config
    ) {
        latch = new CountDownLatch(1);
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

        this.latch.await();

        return "";
    }


    /**
     * Prepare map of all the Sinks(Processing entry points) destinations
     *
     * @param configuration {@link ConfigParser.Config}
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
            // Create outgoing stream if not present already.
            redis.operations.opsForStream()
                    .add(targetStreamName,Map.of("ids","[]"))
                    .subscribe();

            var targetFlux = flux
                    .doOnNext(unifiedMessage -> {
                    })
                    .bufferTimeout(DEDUPLICATION_SIZE, DEDUPLICATION_TIME)
                    .map(recordList -> {
                        var nonDuplicateCollection = recordList.stream()
                                .collect(Collectors.toMap(UnifiedMessage::targetId, Function.identity(), (a, b) -> a))
                                ;
                        recordList.forEach(message -> {
                            if (nonDuplicateCollection.get(message.targetId()) != message) {
                                // send to acknowledgement flux;
                            }
                        });

                        return nonDuplicateCollection.values();
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
                        redis.operations.opsForStream()
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
     * @param config config to pass to next flux
     * @return list of source stream processors
     */
    protected List<Flux<UnifiedMessage>> generateSourceStreamConsumers(
            Map<String, Sinks.Many<UnifiedMessage>> targetSinks,
            ConfigParser.Config config) {
        var streamConsumers = new ArrayList<Flux<UnifiedMessage>>();
        for (var streamRecord: config.mapping().entrySet()) {

            var streamName = streamPrefix + streamRecord.getKey();
            var fieldToMap = streamRecord.getValue().entrySet().stream().toList().getFirst().getKey();

            var baseStream =
                redis.getStream(streamName,fieldToMap,
                    new Redis.Config(
                        group,consumer,
                        SOURCE_READ_TIME, SOURCE_READ_COUNT,
                        SOURCE_READ_TIME, SOURCE_READ_COUNT, SOURCE_READ_TIME*10
                    )
                );


            var unifiedFlux =  baseStream
                    .map( record -> {
                        // Send to Acknowledgement flux that there will be incoming
                        for (var columRecordMap : config.mapping().get(streamRecord.getKey()).entrySet()){
                            for (var processorName: columRecordMap.getValue()) {
                                var targetSink = targetSinks.get(processorName);
                                var res =  targetSink.tryEmitNext(record);
                                if (res.isSuccess()) {
                                    // sendto acknowledgement sync
                                } else {
                                    System.out.println("Message NOT processed by target Flux: " + record);
                                }
                            }
                        }
                        return record;
                    })
                    .publishOn(Schedulers.boundedElastic())
                    .map(record -> {
                        redis.operations.opsForStream().acknowledge(streamName, group, record.id()).subscribe();
                        return record;
                    });

            streamConsumers.add(unifiedFlux);
            var msg = "Watching stream " + streamName + " in group " + group + " with consumer " + consumer;
            System.out.println(msg);
        }
        return streamConsumers;
    }
}

