
package run.ecommerce.cdc;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import run.ecommerce.cdc.commands.WatchStream;
import run.ecommerce.cdc.source.Redis;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
public class WatchTest {

    @Autowired
    private WatchStream watchStream;


    @Autowired
    protected Redis redis;

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

    @Test
    void testWatchCommandDeduplicate() throws InterruptedException {
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
        Thread.sleep(100);
        // using external latch to finish process
        watchStream.latch.countDown();
        Thread.sleep(50);
        job.join();
        assertNotNull(context.res);
    }
}