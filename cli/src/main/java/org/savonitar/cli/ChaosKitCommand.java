package org.savonitar.cli;

import picocli.CommandLine;

@CommandLine.Command(name = "chaos-kit", mixinStandardHelpOptions = true, version = "0.1",
        subcommands = {RunScenarioCommand.class})
public class ChaosKitCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("Use subcommand: run");
    }
}