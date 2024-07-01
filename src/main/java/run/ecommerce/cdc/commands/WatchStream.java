package run.ecommerce.cdc.commands;


import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@ShellComponent
public class WatchStream extends BaseCommand {


    final static int SOURCE_READ_COUNT = 100;
    final static int SOURCE_READ_TIME = 1000;
    final static int BUFFER_SIZE = 1000;
    final static Duration BUFFER_TIME = Duration.ofSeconds(1);

    final static int DEDUPLICATION_SIZE = 10000;
    final static Duration DEDUPLICATION_TIME = Duration.ofSeconds(5);


    // inject the actual template
    @Autowired
    private ReactiveRedisOperations<String, String> redisOperations;
    @Autowired
    private ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;
    WatchStream(EnvPhp env, MviewXML mviewConfig) {
        super(env, mviewConfig);
    }

    protected String streamPrefix = "";
    protected String group = "";
    protected String consumer = "";

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
        var targetFluxes = generateStreams(targetSinks);
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

    protected Map<String,Flux<UnifiedMessage>> generateStreams(
            Map<String, Sinks.Many<UnifiedMessage>> sinks
    ) {
        var fluxes = new HashMap<String,Flux<UnifiedMessage>>();
        for (var record: sinks.entrySet()) {
            var sink = sinks.get(record.getKey());
            var flux = sink.asFlux();
            var targetFlux = flux
                    .doOnNext(unifiedMessage -> {
                    })
                    .map(unifiedMessage -> {
                        System.out.println("For " + record.getKey() + " got " +unifiedMessage);

                        return unifiedMessage;
                    }).bufferTimeout(DEDUPLICATION_SIZE, DEDUPLICATION_TIME)
                    .map(recordList -> {
                        var targetMessage =  recordList.stream()
                                .map(UnifiedMessage::targetId)
                                .distinct()
                                .toList();

                        JSONArray jsonArray = new JSONArray(targetMessage);


                        return recordList;
                    })
                    .flatMap(list -> Flux.fromIterable(list));
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
                        System.out.println(record);
                        var value = new JSONObject(record.getValue().entrySet().stream().toList().get(0).getKey());
                        var idToPass = value.get(fieldToMap);

                        return new UnifiedMessage(record.getId(), streamRecord.getKey(), (Integer) idToPass);
                    })
                    .map( record -> {
                        for (var columRecordMap : mviewConfig.storage.get(streamRecord.getKey()).entrySet()){
                            for (var processorName: columRecordMap.getValue()) {
                                System.out.println("Passed to " + processorName + " " + record);
                                var targetSink =targetSinks.get(processorName);
                                var res =  targetSink.tryEmitNext(record);
                                if (res.isSuccess()) {
                                    System.out.println("Message processed by target Flux: " + record);
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

