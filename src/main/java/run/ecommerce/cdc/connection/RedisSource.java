package run.ecommerce.cdc.connection;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import run.ecommerce.cdc.commands.UnifiedMessage;
import run.ecommerce.cdc.commands.WatchStream;

import java.time.Duration;

@Component
public class RedisSource {

    private final Logger logger = LoggerFactory.getLogger(WatchStream.class);

    public record Config(
            String group, String consumer,
            Integer readTime, Integer readCount,
            Integer recoveryTime, Integer recoveryCount, Integer recoverySlaTime
    ) {
    }

    public ReactiveRedisOperations<String, String> operations;
    protected ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;
    RedisSource() {
    }


    public void configure(RedisConfiguration configuration) {
        var factory = new LettuceConnectionFactory(configuration);
        factory.start();
        this.operations = new ReactiveStringRedisTemplate(factory);
        this.reactiveRedisConnectionFactory = factory;
    }

    public Flux<UnifiedMessage> getStream(String streamName, String field, Config config) {

        var messages = getBaseStream(streamName, config);

        var res = messages
                .map( record -> {
                    var value = new JSONObject(record.getValue().entrySet().stream().toList().getFirst().getValue());
                    var idToPass = value.getJSONObject("after").get(field);

                    return new UnifiedMessage(record.getId(), streamName, (Integer) idToPass);
                });
        return res;
    }


    public Flux<MapRecord<String, String, String>> getBaseStream(String streamName, Config config) {
        operations.opsForStream().createGroup(streamName, ReadOffset.from("0-0"), config.group)
                .onErrorResume(e -> {
                    if (e.getCause().getMessage().equals("BUSYGROUP Consumer Group name already exists")) {
                        logger.info(e.getCause().getMessage());
                    }
                    return Mono.just("OK");
                }).block();

        var consumerInstance = Consumer.from(config.group, config.consumer);

        var options =
                StreamReceiver.StreamReceiverOptions.builder()
                        .pollTimeout(Duration.ofMillis(config.readTime))
                        .batchSize(config.readCount)
                        .build();


        var receiver = StreamReceiver.create(reactiveRedisConnectionFactory, options);

        return receiver.receive(consumerInstance, StreamOffset.create(streamName, ReadOffset.lastConsumed()));
    }

}
