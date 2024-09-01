
package run.ecommerce.cdc;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import run.ecommerce.cdc.commands.DebeziumConfiguration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
public class DebeziumConfigurationTest {

    @Autowired
    private DebeziumConfiguration debeziumConfiguration;
    @Test
    void testGenerateCommand() {
        String config = "./config.yaml";
        String result = debeziumConfiguration.generate(config);

        assertThat(result, containsString("Debezium"));
        assertThat(result, containsString("cdc"));
    }
}