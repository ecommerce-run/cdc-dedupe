package run.ecommerce.cdc.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
public class ConfigParser {

    public record Config(
            Source source,
            Buffers buffers,
            Source target,
            Map<String, Map<String,List<String>>> mapping
    ) {}

    public record Source(
            String format,
            String prefix,
            String group,
            String consumer,
            String acknowledge,
            Connection connection
    ) {
        @Override
        public String acknowledge() {
            return acknowledge == null ? "simple" : acknowledge;
        }
    }

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

    @RegisterReflectionForBinding(Config.class)
    public static Config loadConfig(String configPath) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        if (configPath.endsWith(".yaml")) {
            mapper = new ObjectMapper(new YAMLFactory());
        }
        return mapper.readValue(new File(configPath), Config.class);
    }
}
