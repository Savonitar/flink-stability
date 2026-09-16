package org.savonitar.flink.stability.cli;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "flink-stability", mixinStandardHelpOptions = true, version = "0.1",
        subcommands = {
                RunScenarioCommand.class,
                ValidateSpecificationsCommand.class
        })
public class FlinkStabilityCommand implements Callable<Integer> {
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec specification;

    @Override
    public Integer call() {
        specification.commandLine().usage(specification.commandLine().getOut());
        return CommandLine.ExitCode.USAGE;
    }
}
