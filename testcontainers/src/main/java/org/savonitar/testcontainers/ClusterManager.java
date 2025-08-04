package org.savonitar.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;

public class ClusterManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
    private static final int CONSUMER_TIMEOUT_MS = 10_000;
    private final Network network = Network.newNetwork();
    private KafkaManager kafkaManager;
    private GenericContainer<?> jobManager;
    private GenericContainer<?> taskManager;
    private String jobManagerRestUrl;
    private FlinkContainer flinkContainer;
    private String runningJobId;

    public void checkKafkaUniqueIds(String topic, int expectedMessages) throws Exception {
        kafkaManager.checkKafkaUniqueIds(topic, expectedMessages);
    }

    public void startKafka() throws IOException, InterruptedException {
        LOG.info("Start Kafka");
        kafkaManager = new KafkaManager(network, 1000);
        LOG.info("Kafka started at: {}", kafkaManager.getBootstrapServers());
        kafkaManager.createAndFillInInputTopic();
    }

    public void simulateTaskManagerFailureAndRecovery() {
        if (this.taskManager == null || !this.taskManager.isRunning()) {
            throw new IllegalStateException("TaskManager is not running");
        }
        LOG.info("Simulating TaskManager failure by stopping the container");
        this.taskManager.stop();
        LOG.info("TaskManager container stopped");

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOG.info("Starting a new TaskManager to trigger recovery");
        this.taskManager = this.flinkContainer.createTaskManager();
        this.taskManager.start();
        LOG.info("TaskManager started");
    }

    public void startNewTaskManager() {
        LOG.info("Starting an additional TaskManager");
        this.taskManager = this.flinkContainer.createTaskManager();
        this.taskManager.start();
        LOG.info("New TaskManager started");
    }

    public void startFlink(String version) throws InterruptedException, IOException {
        registerShutdownHook();

        this.flinkContainer = new FlinkContainer(version, network);
        jobManager = flinkContainer.createJobManager();
        taskManager = flinkContainer.createTaskManager();

        startContainers();
        configureJobManagerUrl();
        logFlinkStartup();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (taskManager != null) taskManager.stop();
                if (jobManager != null) jobManager.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    private void startContainers() {
        jobManager.start();
        taskManager.start();
    }

    private void configureJobManagerUrl() {
        int restPort = jobManager.getMappedPort(FlinkContainer.JOB_MANAGER_PORT);
        jobManagerRestUrl = "http://localhost:" + restPort;
    }

    private void logFlinkStartup() {
        LOG.info("Flink JobManager started at: {}", jobManagerRestUrl);
        LOG.info("Flink Web UI: {}", jobManagerRestUrl);
    }

    public boolean waitForAndValidateKafkaOutput(String topic, int expectedMessages) throws Exception {
        return kafkaManager.waitForAndValidateKafkaOutput(topic, expectedMessages);
    }

    public boolean performFinalValidation(String topic, int expectedMessages) throws Exception {
        return kafkaManager.performFinalValidation(topic, expectedMessages);
    }

    public void printKafka() throws InterruptedException, IOException {
        kafkaManager.printMessages("flink-output", CONSUMER_TIMEOUT_MS, 1000);
    }

    public void stopFlink() {
        if (taskManager != null) {
            taskManager.stop();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
    }

    public void stopAll() {
        LOG.info("ClusterManager stopping everything.");
        stopFlink();
        kafkaManager.stop();
    }

    @Override
    public void close() {
        stopAll();
    }

    public String getJobManagerRestUrl() {
        return jobManagerRestUrl;
    }

    public void setRunningJobId(String jobId) {
        this.runningJobId = jobId;
    }

    public String getRunningJobId() {
        return this.runningJobId;
    }
}
