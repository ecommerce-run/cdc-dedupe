package run.ecommerce.cdc.connection;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
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
public class RedisSource implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(WatchStream.class);

    public record Config(
            String group, String consumer,
            Integer readTime, Integer readCount,
            Integer recoveryTime, Integer recoveryCount, Integer recoverySlaTime
    ) {
    }

    public ReactiveRedisOperations<String, String> operations;
    protected LettuceConnectionFactory _factory;

    RedisSource() {
    }


    public void configure(RedisConfiguration configuration) {
        var factory = new LettuceConnectionFactory(configuration);
        this.operations = new ReactiveStringRedisTemplate(factory);
        this._factory = factory;
    }

    public Flux<UnifiedMessage> getStream(String streamName, String field, Config config) {

        var messages = getBaseStream(streamName, config);

        var res = messages
                .doOnNext(message -> {
                    logger.debug("Received " + streamName + ": " + message );
                })
                .map( record -> {
                    var compactFormat = record.getValue().size() == 1;
                    var valueStr =
                            compactFormat ?
                            record.getValue().entrySet().stream().findFirst().get().getValue() :
                            record.getValue().get("value");
                    var value = new JSONObject(valueStr);
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


        var receiver = StreamReceiver.create(_factory, options);

        return receiver.receive(consumerInstance, StreamOffset.create(streamName, ReadOffset.lastConsumed()));
    }

    @Override
    public void start() {
        _factory.start();
    }

    @Override
    public boolean isAutoStartup() {
        return false;
    }

    @Override
    public void stop() {
        _factory.stop();
    }

    @Override
    public boolean isRunning() {
        return _factory != null && _factory.isRunning();
    }

}
