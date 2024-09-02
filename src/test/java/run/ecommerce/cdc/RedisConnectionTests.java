package run.ecommerce.cdc;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;


@Testcontainers
public class RedisConnectionTests {

    @Container
    private static final RedisContainer redis = new RedisContainer(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);


    @Test
    public void testRedisConnection() throws IOException, InterruptedException {
        // Get the Redis host and port
        var redisHost = redis.getHost();
        int redisPort = redis.getMappedPort(6379);

        // Set the Redis connection properties
        System.setProperty("spring.redis.host", redisHost);
        System.setProperty("spring.redis.port", String.valueOf(redisPort));

        // Trigger a shell command to test the Redis connection
        var process = Runtime.getRuntime().exec("redis-cli -h " + redisHost + " -p " + redisPort + " ping");
        process.waitFor();
        assertEquals(0, process.exitValue());
        redis.stop();
        var reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        var output = reader.readLine();
        assertEquals("PONG", output);
    }
}