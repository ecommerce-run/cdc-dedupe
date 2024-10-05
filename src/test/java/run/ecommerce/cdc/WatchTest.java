
package run.ecommerce.cdc;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import run.ecommerce.cdc.commands.WatchStream;
import run.ecommerce.cdc.source.Redis;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Testcontainers
public class WatchTest {

    @Autowired
    private WatchStream watchStream;


    @Autowired
    protected Redis redis;

    @Container
    private static final RedisContainer redisContainer = new RedisContainer(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);


    @Test
    void testWatchCommandBasic() throws InterruptedException {
        String config = "./config.yaml";

        // Container for pressor
        var context = new Object() {
            String res;
        };
        var job = new Thread(() -> {
            context.res = watchStream.watch(config);
        });
        job.start();

        // Letting job run for 20 seconds, it has to do everything.
        Thread.sleep(50);

        // using external latch to finish process
        watchStream.latch.countDown();
        Thread.sleep(50);

        job.join();
        assertNotNull(context.res);
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redisContainer::getHost);
        registry.add("spring.data.redis.port", redisContainer::getFirstMappedPort);
    }

    protected void prepareConfig(String inputFile, String outputFile) throws IOException {

        String replaceFrom = "re";
        String replaceTo = "REPLACE_TO";

        String content = new String(Files.readAllBytes(Paths.get(inputFile)));
//        content = content.replace(replaceFrom, replaceTo);
        FileWriter writer = new FileWriter(outputFile);
        writer.write(content);
        writer.close();
    }

    @Test
    void testWatchCommandDeduplicateCompact() throws InterruptedException, IOException {
        String template = "./config.yaml";
        String configFile = "./config_watch_dedupe.yaml";
        prepareConfig(template, configFile);

        // Container for pressor
        var context = new Object() {
            String res;
        };
        var job = new Thread(() -> {
            context.res = watchStream.watch(configFile);
        });
        job.start();

        var jobAddChanges = new Thread(() -> {
            redis.operations.opsForStream().add("m2.m2.catalog_category_entity",
                    Map.of("key","{\"before\":{\"entity_id\":1},\"after\":{\"entity_id\":1}}")
            ).subscribe();
            redis.operations.opsForStream().add("m2.m2.catalog_category_entity",
                    Map.of("key","{\"before\":{\"entity_id\":1},\"after\":{\"entity_id\":1}}")
            ).subscribe();
            redis.operations.opsForStream().add("m2.m2.catalog_category_entity",
                    Map.of("key","{\"before\":{\"entity_id\":1},\"after\":{\"entity_id\":2}}")
            ).subscribe();
        });
        jobAddChanges.start();

        // Let it run for 15 seconds.
        Thread.sleep(15000);

        watchStream.latch.countDown();
        Thread.sleep(50);
        job.join();
        jobAddChanges.join();


        assertNotNull(context.res);
    }
}