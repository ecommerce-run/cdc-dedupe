package run.ecommerce.cdc.source;

import org.json.JSONObject;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import run.ecommerce.cdc.commands.UnifiedMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Component
public class Redis {

    public record Config(
            String group, String consumer,
            Integer readTime, Integer readCount,
            Integer recoveryTime, Integer recoveryCount, Integer recoverySlaTime
    ) {

    }

    public final ReactiveRedisOperations<String, String> operations;
    protected final ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;
    Redis(
            ReactiveRedisOperations<String, String> redisOperations,
            ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {
        this.operations = redisOperations;
        this.reactiveRedisConnectionFactory = reactiveRedisConnectionFactory;
    }

    public Flux<UnifiedMessage> getStream(String streamName, String field, Config config) {

        operations.opsForStream().createGroup(streamName, ReadOffset.from("0-0"), config.group).subscribe();

        var consumerInstance = Consumer.from(config.group, config.consumer);

        var options =
                StreamReceiver.StreamReceiverOptions.builder()
                        .pollTimeout(Duration.ofMillis(config.readTime))
                        .batchSize(config.readCount)
                        .build();


        var receiver = StreamReceiver.create(reactiveRedisConnectionFactory, options);

        Flux<MapRecord<String, String, String>> messages = receiver
                .receive(consumerInstance, StreamOffset.create(streamName, ReadOffset.lastConsumed()));

        var res = messages
                .map( record -> {
                    var value = new JSONObject(record.getValue().entrySet().stream().toList().getFirst().getValue());
                    var idToPass = value.getJSONObject("after").get(field);

                    return new UnifiedMessage(record.getId(), streamName, (Integer) idToPass);
                });
        return res;
    }
}
