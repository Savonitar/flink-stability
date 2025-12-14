package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.ScenarioRunner;
import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Run chaos scenario")
public class RunScenarioCommand implements Runnable {

    @CommandLine.Option(names = {"-s", "--scenario"}, required = true, description = "Path to scenario YAML file.")
    private String scenarioPath;

    @Override
    public void run() {
        ScenarioRunner.runScenario(scenarioPath);
    }
}
