package org.savonitar.cli;

import picocli.CommandLine;

public class Main {
    public static void main(String[] args) {
        new CommandLine(new ChaosKitCommand()).execute(args);
    }
}