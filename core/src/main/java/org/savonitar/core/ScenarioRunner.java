package org.savonitar.core;

import org.savonitar.testcontainers.ClusterManager;

public class ScenarioRunner {

    public static void runScenario(String scenarioPath) {
        System.out.println("Loading scenario: " + scenarioPath);
        ScenarioFile scenarioFile = ScenarioLoader.load(scenarioPath);

        ClusterManager clusterManager = new ClusterManager();
        clusterManager.startKafka();

        scenarioFile.getScenario().getPhases().forEach(phase -> {
            System.out.println("Planned phase: Flink version " + phase.getFlinkVersion() +
                    " with parallelism " + phase.getParallelism() +
                    " to write " + phase.getTotalRecords() + " records.");

            clusterManager.startFlink(phase.getFlinkVersion());

            System.out.println("Phase completed for version: " + phase.getFlinkVersion());

            clusterManager.stopFlink();
        });

        if (scenarioFile.getChaos() != null && !scenarioFile.getChaos().isEmpty()) {
            scenarioFile.getChaos().forEach(action -> {
                System.out.println("Chaos action: " + action.getType() + " after " + action.getAfterSeconds() + " seconds.");
            });
        }

        if (scenarioFile.getValidate() != null && !scenarioFile.getValidate().isEmpty()) {
            scenarioFile.getValidate().forEach(rule -> {
                System.out.println("Validation: " + rule.getType() + " on topic " + rule.getTopic() +
                        " expecting " + rule.getExpectedRecords() + " records.");
            });
        }

        clusterManager.stopAll();
        System.out.println("Scenario execution completed.");
    }

}
