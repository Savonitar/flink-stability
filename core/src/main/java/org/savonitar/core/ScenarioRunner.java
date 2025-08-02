package org.savonitar.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.testcontainers.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ScenarioRunner {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkRestClient.class);
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9095";
    private static final String SAVEPOINT_PATH = "file:/flink/checkpoints";
    private static final String KAFKA_OUTPUT_TOPIC = "flink-output";
    private static final int FIRST_PHASE = 1;
    private static final int EXPECTED_MESSAGES = 1000;
    private static final int INITIAL_PHASE_WAIT_MS = 30000;

    public static void runScenario(String scenarioPath) {
        LOG.info("Loading scenario: {}", scenarioPath);
        ScenarioFile scenarioFile = ScenarioLoader.load(scenarioPath);

        try (ClusterManager clusterManager = new ClusterManager()) {
            initializeCluster(clusterManager);
            executeScenarioPhases(scenarioFile, clusterManager);
            performFinalValidation(clusterManager);
        } catch (Exception e) {
            LOG.error("Failed to execute scenario", e);
            throw new RuntimeException("Scenario execution failed", e);
        }
        LOG.info("Scenario execution completed.");
    }

    private static void performFinalValidation(ClusterManager clusterManager) throws Exception {
        LOG.info("Performing final validation...");
        clusterManager.waitForAndValidateKafkaOutput(KAFKA_OUTPUT_TOPIC, EXPECTED_MESSAGES);
        clusterManager.performFinalValidation(KAFKA_OUTPUT_TOPIC, EXPECTED_MESSAGES);
        clusterManager.printKafka();
    }

    private static void initializeCluster(ClusterManager clusterManager) throws IOException, InterruptedException {
        LOG.info("Initializing cluster...");
        clusterManager.startKafka();
    }

    private static void executeScenarioPhases(ScenarioFile scenarioFile, ClusterManager clusterManager) {
        AtomicInteger phaseNum = new AtomicInteger(0);
        AtomicReference<String> savepointPath = new AtomicReference<>();
        long start = System.currentTimeMillis();

        scenarioFile.getScenario().getPhases().forEach(phase ->
                executePhase(phase, clusterManager, phaseNum, savepointPath));

        LOG.info("Execution of all phases took: {}ms", System.currentTimeMillis() - start);
    }

    private static void executePhase(ScenarioPhase phase, ClusterManager clusterManager,
                                     AtomicInteger phaseNum, AtomicReference<String> savepointPath) {
        int phaseId = phaseNum.incrementAndGet();
        LOG.info("Starting phase: {}", phase);

        try {
            switch (phaseId) {
                case 1:
                    handleFirstPhase(phase, clusterManager, savepointPath);
                    break;

                case 2:
                    handlePreFailurePhase(phase, clusterManager, savepointPath.get());
                    break;

                case 3:
                    handleRecoveryPhase();
                    break;

                default:
                    LOG.warn("Unknown phase: {}", phaseId);
                    break;
            }
            LOG.info("Phase={} completed for flinkImage: {}", phase, phase.getFlinkImage());
        } catch (Exception e) {
            LOG.error("Failed to execute phase", e);
            throw new RuntimeException("Phase execution failed", e);
        }
    }

    private static void handleRecoveryPhase() {
        LOG.info("Phase 3: Recovering job");
        sleep(INITIAL_PHASE_WAIT_MS);
        LOG.info("Phase 3: Recovering finished");
    }

    private static void handlePreFailurePhase(ScenarioPhase phase, ClusterManager clusterManager, String savepoint) throws IOException, InterruptedException {
        clusterManager.startFlink(phase.getFlinkImage());
        startFlinkJob(phase, clusterManager, savepoint);

        LOG.info("Phase 2: Job restored.");
        sleep(INITIAL_PHASE_WAIT_MS);

        clusterManager.simulateTaskManagerFailureAndRecovery();
    }

    private static String startFlinkJob(ScenarioPhase phase, ClusterManager clusterManager, String savepointPath)
            throws IOException {
        FlinkRestClient restClient = new FlinkRestClient(clusterManager.getJobManagerRestUrl());
        String jarId = uploadAndVerifyJar(phase, restClient);
        String programArgs = buildProgramArgs(phase);

        String jobResponse = restClient.runJob(jarId, programArgs, savepointPath);
        String jobId = extractJobId(jobResponse);
        LOG.info("Job started with ID: {}", jobId);

        return jobId;
    }

    private static String uploadAndVerifyJar(ScenarioPhase phase, FlinkRestClient restClient) throws IOException {
        String jarId = restClient.uploadJar(phase.getJar());
        restClient.availableJars();
        LOG.info("Uploaded jar with ID: {}", jarId);
        return jarId;
    }

    private static String buildProgramArgs(ScenarioPhase phase) {
        return String.format("--bootstrapServers %s --processingDelayMs %d",
                KAFKA_BOOTSTRAP_SERVERS, phase.getProcessingDelayMs());
    }

    private static void handleFirstPhase(ScenarioPhase phase, ClusterManager clusterManager,
                                         AtomicReference<String> savepointPath) throws IOException, InterruptedException {
        LOG.info("Executing first phase tasks...");
        clusterManager.startFlink(phase.getFlinkImage());
        String jobId = startFlinkJob(phase, clusterManager, null);

        LOG.info("Phase 1: Running job to create initial state");
        sleep(INITIAL_PHASE_WAIT_MS);

        FlinkRestClient restClient = new FlinkRestClient(clusterManager.getJobManagerRestUrl());
        createSavepoint(jobId, restClient, savepointPath);
        LOG.info("Created savepoint at: {}", savepointPath.get());

        clusterManager.stopFlink();
    }

    private static void handleSubsequentPhase(ClusterManager clusterManager) {
        try {
            clusterManager.waitForAndValidateKafkaOutput(KAFKA_OUTPUT_TOPIC, EXPECTED_MESSAGES);
            clusterManager.stopFlink();
            clusterManager.performFinalValidation(KAFKA_OUTPUT_TOPIC, EXPECTED_MESSAGES);
            clusterManager.printKafka();
        } catch (Exception ex) {
            LOG.error("Failed to complete Flink job", ex);
        }
        clusterManager.stopFlink();
    }

    private static void createSavepoint(String jobId, FlinkRestClient restClient,
                                        AtomicReference<String> savepointPath) throws IOException {
        savepointPath.set(restClient.stopJobWithSavepoint(jobId, SAVEPOINT_PATH));
        LOG.info("Savepoint created at: {}", savepointPath.get());
    }

    private static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Sleep interrupted", e);
        }
    }

    private static String extractJobId(String jobResponse) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jobResponse);
        return root.get("jobid").asText();
    }

    public static boolean waitForJobCompletion(FlinkRestClient restClient, String jobId, long timeoutMs, long pollIntervalMs) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            String status = restClient.getJobStatus(jobId);
            LOG.info("Job {} status: {}", jobId, status);

            switch (status.toLowerCase()) {
                case "finished":
                    LOG.info("Job {} completed successfully", jobId);
                    return true;
                case "failed":
                case "canceled":
                    LOG.error("Job {} failed with status: {}", jobId, status);
                    throw new IOException("Job failed with status: " + status);
                case "running":
                case "created":
                case "restarting":
                    Thread.sleep(pollIntervalMs);
                    break;
                default:
                    LOG.warn("Unknown job status: {}", status);
                    Thread.sleep(pollIntervalMs);
            }
        }

        throw new IOException("Job did not complete within timeout: " + timeoutMs + "ms");
    }
}