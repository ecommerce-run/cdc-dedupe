
package run.ecommerce.cdc;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import run.ecommerce.cdc.commands.WatchStream;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
public class WatchTests {

    @Autowired
    private WatchStream watchStream;

    @Container
    private static final RedisContainer redisContainer = new RedisContainer(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);


    protected void prepareConfig(String inputFile, String outputFile) throws IOException {

        String replaceHostFrom = "host: 127.0.0.1";
        String replaceHostTo = "host: " + redisContainer.getHost();

        String replacePortFrom = "port: '6389'";
        String replacePortTo = "port: " + redisContainer.getFirstMappedPort();
        String content = new String(Files.readAllBytes(Paths.get(inputFile)));
        content = content.replace(replaceHostFrom, replaceHostTo);
        content = content.replace(replacePortFrom, replacePortTo);
        FileWriter writer = new FileWriter(outputFile);
        writer.write(content);
        writer.close();
    }


    @Test
    void testWatchCommandBasic() throws InterruptedException, IOException {
        var template = "./config.yaml";
        var configFile = "./config_watch_dedupe.yaml";
        prepareConfig(template, configFile);
        // Container for pressor
        var context = new Object() {
            String res;
        };
        var job = new Thread(() -> context.res = watchStream.watch(configFile));
        job.start();
        // wait till application is ready
        watchStream.ready.await();

        // using external latch to finish process
        watchStream.gracefulShutdown.countDown();

        var targetOps = watchStream.redisTarget.operations.opsForStream();
        var itemsList= targetOps.read(StreamOffset.fromStart("target.catalog_category_product"))
                .collectList().block();

        assertNotNull(itemsList);
        assertEquals(1, itemsList.size());
        assertNotNull(itemsList.getFirst());

        targetOps.delete("target.catalog_category_product", itemsList.getFirst().getId()).block();

        itemsList= targetOps.read(StreamOffset.fromStart("target.catalog_product_flat"))
                .collectList().block();

        assertNotNull(itemsList);
        assertNotNull(itemsList.getFirst());
        targetOps.delete("target.catalog_product_flat", itemsList.getFirst().getId()).block();

        job.join();
        assertNotNull(context.res);
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

        var job = new Thread(() -> context.res = watchStream.watch(configFile));
        job.start();

        // wait till application is ready
        watchStream.ready.await();

        Thread.sleep(2000);
        // push in data into redis
        var sourceOps = watchStream.redisSource.operations.opsForStream();
        sourceOps.add("m2.m2.catalog_category_entity",
                Map.of("key","{\"before\":{\"entity_id\":1,\"v\":4},\"after\":{\"entity_id\":1,\"v\":4}}")
        ).subscribe();
        sourceOps.add("m2.m2.catalog_category_entity",
                Map.of("key","{\"before\":{\"entity_id\":1,\"v\":3},\"after\":{\"entity_id\":2}}")
        ).subscribe();
        sourceOps.add("m2.m2.catalog_category_entity",
                Map.of("key","{\"before\":{\"entity_id\":1,\"v\":2},\"after\":{\"entity_id\":1}}")
        ).subscribe();
        sourceOps.add("m2.m2.catalog_category_entity",
                Map.of("key","{\"before\":{\"entity_id\":1,\"v\":1},\"after\":{\"entity_id\":2}}")
        ).subscribe();

        // Let it run for some time
        Thread.sleep(16000);

        // we can shut down safely
        watchStream.gracefulShutdown.countDown();
        job.join();

        // check that we got what we want to get
        var targetOps = watchStream.redisTarget.operations.opsForStream();
        var itemsList= targetOps.read(StreamOffset.fromStart("target.catalog_category_product"))
                .collectList().block();

        assertNotNull(itemsList);
        assertNotEquals(0, itemsList.size());
        assertNotEquals(1, itemsList.size());

        itemsList = targetOps.read(StreamOffset.fromStart("target.catalog_product_flat"))
                .collectList().block();

        assertNotNull(itemsList);
        assertNotEquals(0, itemsList.size());
        assertNotEquals(1, itemsList.size());

        var sourceList = sourceOps.read(StreamOffset.fromStart("m2.m2.catalog_category_entity"))
                .collectList().block();
        assertNotNull(sourceList);
        assertEquals(0, sourceList.size());

        assertNotNull(context.res);
    }
}