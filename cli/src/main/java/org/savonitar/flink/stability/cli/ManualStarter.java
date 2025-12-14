package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.ScenarioRunner;

// Running via maven sometimes can face the issue in testcontainers:
// https://github.com/testcontainers/testcontainers-java/issues/1454
// That's why locally it might be preferred to run via this class
public class ManualStarter {
    public static void main(String[] args) {
        ScenarioRunner.runScenario("scenarios/example.yaml");
    }
}
