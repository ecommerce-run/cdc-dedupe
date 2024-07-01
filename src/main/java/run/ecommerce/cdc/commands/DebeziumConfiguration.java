package run.ecommerce.cdc.commands;

import jakarta.annotation.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.shell.command.annotation.Option;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.StringUtils;
import run.ecommerce.cdc.model.EnvPhp;
import run.ecommerce.cdc.model.MviewXML;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

@ShellComponent
public class DebeziumConfiguration extends BaseCommand {

    DebeziumConfiguration(EnvPhp env, MviewXML mviewConfig) {
        super(env, mviewConfig);
    }

    @ShellMethod(key = "generate", value = "generate debezium config")
    public String generate(
            @Option(longNames = {"cwd"}, defaultValue = "./") String cwd
    ) {

        var initError = init(cwd);
        if(initError != null) {
            return initError;
        }
        try {
            env.init(cwd);
            mviewConfig.init(cwd);
        } catch (IOException | InterruptedException e) {
            return e.getMessage();
        }

        var dbName = (String) env.getValueByPath("db/connection/default/dbname");
        var dbUser = (String) env.getValueByPath("db/connection/default/username");
        var dbPassword = (String) env.getValueByPath("db/connection/default/password");

        List<String> watchTables = new ArrayList<>();
        List<String> watchColumns = new ArrayList<>();
        for (var tableName : mviewConfig.storage.keySet()) {
            watchTables.add(dbName + "." + tableName);
            for (var columnName : mviewConfig.storage.get(tableName).keySet()) {
                watchColumns.add(dbName + "." + tableName + "." + columnName);
            }
        }

        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("DB_NAME", dbName);
        placeholders.put("DB_USER", dbUser);
        placeholders.put("DB_PASSWORD", dbPassword);
        placeholders.put("DB_WATCH_TABLES",  String.join(",", watchTables));
        placeholders.put("DB_WATCH_COLUMNS", String.join(",", watchColumns));

        var fileContent = readFileFromResources();

        return replacePlaceholders(fileContent, placeholders);
    }


    @Resource
    private ResourceLoader resourceLoader;

    public String readFileFromResources() {
        var resource = resourceLoader.getResource("classpath:templates/debezium.properties");
        try {
            return Files.readString(resource.getFile().toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public String replacePlaceholders(String template, Map<String, String> placeholders) {
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            template = StringUtils.replace(template, "{{" + entry.getKey() + "}}", entry.getValue());
        }
        return template;
    }
}
