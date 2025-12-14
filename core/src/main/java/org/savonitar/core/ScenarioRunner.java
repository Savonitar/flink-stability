package org.savonitar.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.testcontainers.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class ScenarioRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ScenarioRunner.class);

    private ScenarioRunner() {
        // Noop
    }

    public static void runScenario(String scenarioPath) {
        LOG.info("Loading scenario: {}", scenarioPath);
        final long beginTime = System.currentTimeMillis();
        ScenarioFile scenarioFile = ScenarioLoader.load(scenarioPath);

        try (ClusterManager clusterManager = new ClusterManager()) {
            for (ScenarioPhase phase : scenarioFile.getScenario().getPhases()) {
                final int repeats = phase.effectiveRepeat();
                for (int i = 0; i < repeats; i++) {
                    LOG.info("Starting phase: {} (iteration {}/{})", phase.getName(), i, repeats);
                    final long phaseBeginTime = System.currentTimeMillis();
                    executePhase(phase, clusterManager);
                    LOG.info("Completed phase: {} (iteration {}/{}) in {} ms", phase.getName(), i, repeats,
                            System.currentTimeMillis() - phaseBeginTime);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to execute scenario", e);
            throw new RuntimeException("Scenario execution failed", e);
        }

        LOG.info("Scenario execution completed in {} ms", System.currentTimeMillis() - beginTime);
    }

    private static void executePhase(ScenarioPhase phase, ClusterManager clusterManager) throws Exception {
        AtomicReference<String> savepointPath = new AtomicReference<>();

        for (ScenarioStep step : phase.getSteps()) {
            LOG.info("Executing step: {}", step);
            switch (step.getType()) {
                case "start":
                    if ("kafka".equals(step.getComponent())) {
                        clusterManager.startKafka();
                    } else if ("flink".equals(step.getComponent())) {
                        clusterManager.startFlink(step.getImage());
                        String jobId = startFlinkJob(step, clusterManager, step.isRestoreFromSavepoint() ? savepointPath.get() : null);
                        clusterManager.setRunningJobId(jobId);
                    } else if ("taskmanager".equals(step.getComponent())) {
                        clusterManager.startNewTaskManager();
                    }
                    break;

                case "stop":
                    if ("flink".equals(step.getComponent())) {
                        clusterManager.stopFlink();
                    }
                    break;

                case "kill":
                    if ("taskmanager".equals(step.getComponent())) {
                        clusterManager.simulateTaskManagerFailureAndRecovery();
                    }
                    break;

                case "savepoint":
                    try(FlinkRestClient restClient = new FlinkRestClient(clusterManager.getJobManagerRestUrl())) {
                        String jobId = clusterManager.getRunningJobId();
                        String savepoint = restClient.stopJobWithSavepoint(jobId);
                        savepointPath.set(savepoint);
                        LOG.info("Savepoint stored at: {}", savepoint);
                    }
                    break;

                case "wait":
                    sleep(step.getWaitMs());
                    break;

                case "validate":
                    if (step.getValidations() != null) {
                        for (ValidationRule validation : step.getValidations()) {
                            validateKafka(clusterManager, validation);
                        }
                    }
                    break;

                default:
                    LOG.warn("Unknown step type: {}", step.getType());
            }
        }
    }

    public static void validateKafka(ClusterManager clusterManager, ValidationRule rule) throws Exception {
        if ("kafka-count".equals(rule.getType())) {
            clusterManager.waitForAndValidateKafkaOutput(rule.getTopic(), rule.getExpectedRecords());
        } else if ("kafka-unique-ids".equals(rule.getType())) {
            clusterManager.checkKafkaUniqueIds(rule.getTopic(), rule.getExpectedRecords());
        } else {
            throw new UnsupportedOperationException("Unknown validation type: " + rule.getType());
        }
    }

    private static String startFlinkJob(ScenarioStep step, ClusterManager clusterManager, String savepointPath) throws IOException {
        try (FlinkRestClient restClient = new FlinkRestClient(clusterManager.getJobManagerRestUrl())) {
            String jarId = restClient.uploadJar(step.getJar());

            String programArgs = step.getArgs() != null ? String.join(" ", step.getArgs()) : "";

            String jobResponse = restClient.runJob(jarId, programArgs, savepointPath);
            return extractJobId(jobResponse);
        }
    }

    private static String extractJobId(String jobResponse) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jobResponse);
        return root.get("jobid").asText();
    }

    private static void sleep(Long ms) {
        if (ms == null) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Sleep interrupted", e);
        }
    }
}
