package run.ecommerce.cdc;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import run.ecommerce.cdc.model.ConfigParser;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class ConfigParserTests {

    @Test
    void testConfigParserJson() throws Exception {
        var classLoader = getClass().getClassLoader();
        var filePath = classLoader.getResource("config.json").getFile();
        var config = ConfigParser.loadConfig(filePath);
        assertEquals("cdc", config.source().group());
    }

    @Test
    void testConfigParserYaml() throws Exception {
        var classLoader = getClass().getClassLoader();
        var filePath = classLoader.getResource("config.yaml").getFile();
        var config = ConfigParser.loadConfig(filePath);
        assertEquals("cdc", config.source().group());
    }
}
