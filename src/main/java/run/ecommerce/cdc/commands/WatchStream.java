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
import run.ecommerce.cdc.model.EnvPhp;
import run.ecommerce.cdc.model.MviewXML;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

@ShellComponent
public class WatchStream extends BaseCommand {


    final static int SOURCE_READ_COUNT = 100;
    final static int SOURCE_READ_TIME = 1000;
    final static int BUFFER_SIZE = 1000;
    final static Duration BUFFER_TIME = Duration.ofSeconds(1);

    final static int DEDUPLICATION_SIZE = 10000;
    final static Duration DEDUPLICATION_TIME = Duration.ofSeconds(5);

    final static int TARGET_BUFFER_SIZE = 1000;
    final static Duration TARGET_BUFFER_TIME = Duration.ofSeconds(1);



    private ReactiveRedisOperations<String, String> redisOperations;
    private ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;
    WatchStream(
            EnvPhp env,
            MviewXML mviewConfig,
            ReactiveRedisOperations<String, String> redisOperations,
            ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {
        super(env, mviewConfig);
        this.redisOperations = redisOperations;
        this.reactiveRedisConnectionFactory = reactiveRedisConnectionFactory;
    }

    protected String streamPrefix = "";
    protected String group = "";
    protected String consumer = "";
    protected String targetPrefix = "target.";

    @SneakyThrows
    @ShellMethod(key = "watch", value = "Watch stream")
    public String generate(
            @Option(longNames = {"cwd"}, defaultValue = "./") String cwd,
            @Option(longNames = {"streamPrefix"}) String streamPrefix,
            @Option(longNames = {"group"}) String group,
            @Option(longNames = {"consumer"}) String consumer
    ) {
        var initError = init(cwd);
        if(initError != null) {
            return initError;
        }
        var dbName = (String) env.getValueByPath("db/connection/default/dbname");
        this.streamPrefix = streamPrefix;
        this.group = group;
        this.consumer = consumer;

        // Create a StreamReceiver instance
        var consumerInstance = Consumer.from(group, consumer);

        var targetSinks = generateSinks(mviewConfig);
        var targetFluxes = generateTargetStreams(targetSinks);
        System.out.println("Starting...");
        for(var fluxRec: targetFluxes.entrySet()) {
            fluxRec.getValue().subscribe();
            System.out.println("Started " + fluxRec.getKey());
        }
        System.out.println("Starting Redis Consumers");
        var sourceFluxes = generateSourceStreamConsumers(targetSinks);
        for (var fluxRec : sourceFluxes) {
            fluxRec.subscribe();
        }

        System.out.println("Started");

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
        return "";
    }


    protected Map<String, Sinks.Many<UnifiedMessage>> generateSinks(MviewXML configuration) {
        var sinks = new HashMap<String, Sinks.Many<UnifiedMessage>>();
        for (var processor: configuration.indexerMap.entrySet()) {
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
                    .map(unifiedMessage -> {
                        return unifiedMessage;
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
                    .flatMap(Flux::fromIterable)
                    .map(unifiedMessage -> {
                        return unifiedMessage;
                    });
            fluxes.put(record.getKey(), targetFlux);
        }
        return fluxes;
    }

    protected List<Flux<UnifiedMessage>> generateSourceStreamConsumers(
            Map<String, Sinks.Many<UnifiedMessage>> targetSinks
    ) {
        var streamConsumers = new ArrayList<Flux<UnifiedMessage>>();
        for (var streamRecord: mviewConfig.storage.entrySet()) {

            var streamName = streamPrefix + streamRecord.getKey();
            var fieldToMap = streamRecord.getValue().entrySet().stream().toList().get(0).getKey();

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

            var unifiedFlux =  messages
                    .map( record -> {
                        var value = new JSONObject(record.getValue().entrySet().stream().toList().get(0).getValue());
                        var idToPass = value.getJSONObject("after").get(fieldToMap);

                        return new UnifiedMessage(record.getId(), streamRecord.getKey(), (Integer) idToPass);
                    })
                    .map( record -> {
                        for (var columRecordMap : mviewConfig.storage.get(streamRecord.getKey()).entrySet()){
                            for (var processorName: columRecordMap.getValue()) {
                                var targetSink =targetSinks.get(processorName);
                                var res =  targetSink.tryEmitNext(record);
                                if (res.isSuccess()) {
                                } else {
                                    System.out.println("Message NOT processed by target Flux: " + record);
                                }
                            }
                        }
                        return record;
                    })
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

