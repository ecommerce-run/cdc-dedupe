package run.ecommerce.cdc.commands;

import jakarta.annotation.Resource;
import lombok.SneakyThrows;
import org.springframework.core.io.ResourceLoader;
import org.springframework.shell.command.annotation.Option;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.StringUtils;
import run.ecommerce.cdc.model.ConfigParser;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

@ShellComponent
public class DebeziumConfiguration  {
    @SneakyThrows
    @ShellMethod(key = "generate", value = "generate debezium config")
    public String generate(
            @Option(longNames = {"config"}, shortNames = {'c'},
                    defaultValue = "./config.json") String config
    ) {

        var configObj = ConfigParser.loadConfig(config);

        var prefixSplit = configObj.source().prefix().split("\\.",3);
        var topicPrefix = prefixSplit[0];
        var dbName = prefixSplit[1];
        var dbUser = "DB_USER";
        var dbPassword = "DB_PASSWORD";

        List<String> watchTables = new ArrayList<>();
        List<String> watchColumns = new ArrayList<>();
        for (var tableName : configObj.mapping().keySet()) {
            watchTables.add(dbName + "." + tableName);
            for (var columnName : configObj.mapping().get(tableName).keySet()) {
                watchColumns.add(dbName + "." + tableName + "." + columnName);
            }
        }

        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("TOPIC_PREFIX", topicPrefix);
        placeholders.put("DB_USER", dbUser);
        placeholders.put("DB_PASSWORD", dbPassword);
        placeholders.put("DB_WATCH_TABLES",  String.join(",", watchTables));
        placeholders.put("DB_WATCH_COLUMNS", String.join(",", watchColumns));

        var fileContent = readFileFromResources();

        return replacePlaceholders(fileContent, placeholders);
    }

    @Resource
    private ResourceLoader resourceLoader;

    public String readFileFromResources() throws IOException {
        var resource = resourceLoader.getResource("classpath:templates/debezium.properties");
        return Files.readString(resource.getFile().toPath());
    }

    public String replacePlaceholders(String template, Map<String, String> placeholders) {
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            template = StringUtils.replace(template, "{{" + entry.getKey() + "}}", entry.getValue());
        }
        return template;
    }
}
