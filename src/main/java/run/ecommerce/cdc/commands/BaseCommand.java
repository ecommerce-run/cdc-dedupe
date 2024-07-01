package run.ecommerce.cdc.commands;

import run.ecommerce.cdc.model.EnvPhp;
import run.ecommerce.cdc.model.MviewXML;

import java.io.IOException;

public class BaseCommand {
    protected EnvPhp env;
    protected MviewXML mviewConfig;
    BaseCommand(EnvPhp env, MviewXML mviewConfig) {
        this.env = env;
        this.mviewConfig = mviewConfig;
    }

    protected String init(String cwd) {
        try {
            env.init(cwd);
            mviewConfig.init(cwd);
        } catch (IOException | InterruptedException e) {
            return e.getMessage();
        }
        return null;
    }
}
