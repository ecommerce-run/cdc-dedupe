package run.ecommerce.cdc.connection;


import jakarta.annotation.PreDestroy;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;


@Component
public class RedisTarget {


    public ReactiveRedisOperations<String, String> operations;
    protected ReactiveRedisConnectionFactory factory;
    protected LettuceConnectionFactory _factory;
    RedisTarget() {
    }

    public void configure(RedisConfiguration configuration) {
        var factory = new LettuceConnectionFactory(configuration);
        factory.start();
        this.operations = new ReactiveStringRedisTemplate(factory);
        this._factory = factory;
    }

    @PreDestroy
    public void destroy() {
        _factory.destroy();
    }
}
