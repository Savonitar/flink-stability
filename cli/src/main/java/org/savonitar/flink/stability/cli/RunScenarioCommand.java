package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.ScenarioRunner;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "run",
        mixinStandardHelpOptions = true,
        description = "Run chaos scenario")
public class RunScenarioCommand implements Callable<Integer> {

    @CommandLine.Option(names = {"-s", "--scenario"}, required = true, description = "Path to scenario YAML file.")
    private String scenarioPath;

    @Override
    public Integer call() {
        ScenarioRunner.runScenario(scenarioPath);
        return CommandLine.ExitCode.OK;
    }
}
