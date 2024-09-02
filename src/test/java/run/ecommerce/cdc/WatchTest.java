
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

        // using external latch to terminate.
        var latch = new CountDownLatch(1);
        watchStream.setLatch(latch);
        var context = new Object() {
            String res;
        };
        var job = new Thread(() -> {
            context.res = watchStream.watch(config);
        });
        job.start();

        Thread.sleep(20000);
        latch.countDown();
        Thread.sleep(500);
        assertNotNull(context.res);
    }
}