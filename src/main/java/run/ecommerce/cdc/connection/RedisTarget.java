package run.ecommerce.cdc.connection;

import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;


@Component
public class RedisTarget implements SmartLifecycle {


    protected RedisConfiguration configuration;

    public ReactiveRedisOperations<String, String> operations;
    protected LettuceConnectionFactory _factory;
    RedisTarget() {
    }

    public void configure(RedisConfiguration configuration) {
        this.configuration = configuration;
        var factory = new LettuceConnectionFactory(configuration);
        this.operations = new ReactiveStringRedisTemplate(factory);
        this._factory = factory;
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
