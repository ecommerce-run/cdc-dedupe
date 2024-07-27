package run.ecommerce.cdc.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ConfigParser {

    public record Config(
            Source source,
            Buffers buffers,
            Source target,
            Map<String, Map<String,List<String>>> mapping
    ) {}

    public record Source(
            String prefix,
            String group,
            String consumer,
            Connection connection
    ) {}

    public record Connection(
            String host,
            int port,
            int db
    ) {}

    public record Buffers(
            Buffer source,
            Buffer dedupe,
            Buffer target
    ) {}

    public record Buffer(
            int size,
            int time
    ) {}

    public static Config loadConfig(String configPath) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        if (configPath.endsWith('.yaml')) {
            mapper = new ObjectMapper(new YamlFactory());
        }
        return mapper.readValue(new File(configPath), Config.class);
    }
}
