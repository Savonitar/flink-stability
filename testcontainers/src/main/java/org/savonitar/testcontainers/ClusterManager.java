package org.savonitar.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;

public class ClusterManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
    private final Network network = Network.newNetwork();
    private KafkaValidator kafkaValidator;
    private ConfluentKafkaContainer kafka;
    private GenericContainer<?> jobManager;
    private GenericContainer<?> taskManager;
    private String jobManagerRestUrl;
    private static final int MESSAGE_COUNT = 1_000;
    private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9093";
    private static final String RECORDS_FILE_PATH = "/tmp/records.txt";
    private static final int CONSUMER_TIMEOUT_MS = 10000;

    private static final class KafkaTopicConfig {
        private final String name;
        private final int partitions;
        private final int replicationFactor;
        private final String bootstrapServer;

        public KafkaTopicConfig(String name, int partitions, int replicationFactor, String bootstrapServer) {
            this.name = name;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.bootstrapServer = bootstrapServer;
        }
    }

    public void startKafka() throws IOException, InterruptedException {
        LOG.info("Start Kafka");

        kafka = new ConfluentKafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
                        .asCompatibleSubstituteFor("apache/kafka"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener("kafka:9095")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
                .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", String.valueOf(Duration.ofHours(2).toMillis()))
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withStartupTimeout(Duration.ofSeconds(120))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("KAFKA_CONTAINER_LOGS")));

        kafka.start();
        kafkaValidator = new KafkaValidator(kafka);
        LOG.info("Kafka started at: {}", kafka.getBootstrapServers());
        createAndFillInInputTopic();
    }

    private void createAndFillInInputTopic() throws IOException, InterruptedException {
        KafkaTopicConfig topicConfig = new KafkaTopicConfig(
                "input-topic",
                1,
                1,
                KAFKA_BOOTSTRAP_SERVER
        );

        createTopic(topicConfig);
        String messages = generateMessages(MESSAGE_COUNT);
        produceMessages(topicConfig, messages);
        consumeAndVerifyMessages(topicConfig);
        listTopics();
    }

    private void createTopic(KafkaTopicConfig config) throws IOException, InterruptedException {
        LOG.info("Creating topic '{}'", config.name);
        kafka.execInContainer(
                "/bin/sh", "-c",
                "kafka-topics",
                "--create",
                "--if-not-exists",
                "--topic", config.name,
                "--bootstrap-server", config.bootstrapServer,
                "--partitions", String.valueOf(config.partitions),
                "--replication-factor", String.valueOf(config.replicationFactor)
        );
    }

    private String generateMessages(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            sb.append(i).append("\n");
        }
        return sb.toString();
    }

    private void produceMessages(KafkaTopicConfig config, String messages) throws IOException, InterruptedException {
        LOG.info("Producing {} messages into '{}'", MESSAGE_COUNT, config.name);
        kafka.copyFileToContainer(Transferable.of(messages.getBytes()), RECORDS_FILE_PATH);

        Container.ExecResult result = kafka.execInContainer(
                "/bin/sh", "-c",
                String.format("cat %s | kafka-console-producer --broker-list %s --topic %s",
                        RECORDS_FILE_PATH, config.bootstrapServer, config.name)
        );

        if (result.getExitCode() != 0) {
            LOG.error("Failed to produce records. ExitCode={}, Stderr={}",
                    result.getExitCode(), result.getStderr());
        } else {
            LOG.info("Successfully produced messages to '{}'", config.name);
        }
    }

    private void consumeAndVerifyMessages(KafkaTopicConfig config) throws IOException, InterruptedException {
        Container.ExecResult result = kafka.execInContainer(
                "/bin/sh", "-c",
                String.format("kafka-console-consumer --bootstrap-server %s --topic %s " +
                                "--from-beginning --timeout-ms %d --max-messages %d " +
                                "--consumer-property group.id=test-input-group " +
                                "--consumer-property isolation.level=read_uncommitted",
                        config.bootstrapServer, config.name, CONSUMER_TIMEOUT_MS, MESSAGE_COUNT)
        );
        LOG.info("InputConsumer output: {}", result.getStdout());
    }

    private void listTopics() throws IOException, InterruptedException {
        Thread.sleep(5000);
        Container.ExecResult execResult = kafka.execInContainer(
                "/bin/sh", "-c",
                "kafka-topics", "--list",
                "--bootstrap-server localhost:9093");
        LOG.info("Topics output: {}", execResult.getStdout());
    }

    public void startFlink(String version) throws InterruptedException, IOException {
        registerShutdownHook();

        FlinkContainer containerConfig = new FlinkContainer(version, network);
        jobManager = containerConfig.createJobManager();
        taskManager = containerConfig.createTaskManager();

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
        String flinkWebUi = jobManagerRestUrl;
        LOG.info("Flink JobManager started at: {}", jobManagerRestUrl);
        LOG.info("Flink Web UI: {}", flinkWebUi);
    }

    public boolean waitForAndValidateKafkaOutput(String topic, int expectedMessages) throws Exception {
        return kafkaValidator.waitForAndValidateKafkaOutput(topic, expectedMessages);
    }

    public boolean performFinalValidation(String topic, int expectedMessages) throws Exception {
        return kafkaValidator.performFinalValidation(topic, expectedMessages);
    }

    public void printKafka() throws InterruptedException, IOException {
        kafkaValidator.printMessages("flink-output", CONSUMER_TIMEOUT_MS, 1000);
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
        if (kafka != null) kafka.stop();
    }

    @Override
    public void close() {
        stopAll();
    }

    public String getJobManagerRestUrl() {
        return jobManagerRestUrl;
    }
}