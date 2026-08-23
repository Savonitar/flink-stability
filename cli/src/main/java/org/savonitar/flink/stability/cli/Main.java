package org.savonitar.flink.stability.cli;

import picocli.CommandLine;

public class Main {
    public static void main(String[] args) {
        System.exit(execute(args));
    }

    static int execute(String... args) {
        return new CommandLine(new ChaosKitCommand()).execute(args);
    }
}
