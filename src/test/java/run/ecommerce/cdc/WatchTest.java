
package run.ecommerce.cdc;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import run.ecommerce.cdc.commands.WatchStream;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
public class WatchTest {

    @Autowired
    private WatchStream watchStream;
    @Test
    void testGenerateCommand() throws InterruptedException {
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
        Thread.sleep(20000);
        // using external latch to finish process
        watchStream.latch.countDown();
        Thread.sleep(50);
        assertNotNull(context.res);

        // Assert processed data
    }
}