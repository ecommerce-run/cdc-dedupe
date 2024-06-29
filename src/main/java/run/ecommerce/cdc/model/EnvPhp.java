package run.ecommerce.cdc.model;

import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.json.JSONObject;

@Component
public class EnvPhp {

    protected JSONObject phpConfig;
    public void init(String rootDirectoryPath) throws IOException, InterruptedException {
        var root = new File(rootDirectoryPath);
        var phpCommand = List.of("php", "-r", "echo json_encode(include \"app/etc/env.php\");");
        phpConfig = runCommandAndGetJson(phpCommand, root);


    }

    protected static JSONObject runCommandAndGetJson(List<String> command, File cwd) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(cwd);
        Process process = processBuilder.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String output = String.join("\n", reader.lines().toArray(String[]::new));

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("Command failed with exit code " + exitCode);
            }

            return new JSONObject(output);
        }
    }

    public Object getValueByPath( String path) {
        String[] pathComponents = path.split("/");
        JSONObject current = phpConfig;
        for (int i = 0; i < pathComponents.length; i++) {
            String component = pathComponents[i];
            if (current.isNull(component)) {
                return null; // or return a custom "not found" value
            }
            if (i == pathComponents.length - 1) {
                return current.get(component);
            } else {
                current = current.getJSONObject(component);
            }
        }
        return null; // or return a custom "not found" value
    }

}
