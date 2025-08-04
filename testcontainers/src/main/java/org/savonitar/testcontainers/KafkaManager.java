package org.savonitar.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class KafkaManager {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaManager.class);
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    private static final int STABILITY_WAIT_MS = 10_000;
    private static final int POLL_INTERVAL_MS = 2000;
    private static final String KAFKA_CONSUMER_CMD = "kafka-console-consumer";
    private static final String BOOTSTRAP_SERVER = "localhost:9093";

    private final int messages;
    private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9093";
    private static final String RECORDS_FILE_PATH = "/tmp/records.txt";
    private static final int CONSUMER_TIMEOUT_MS = 10_000;

    private final ConfluentKafkaContainer kafka;

    public KafkaManager(Network network, int messages) {
        this.kafka = new ConfluentKafkaContainer(
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
        this.kafka.start();
        this.messages = messages;
    }

    public String getBootstrapServers() {
        return this.kafka.getBootstrapServers();
    }

    public void stop() {
        this.kafka.stop();
    }

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

    public record KafkaValidationConfig(
            String topic,
            int expectedMessages,
            int timeoutMs,
            int maxMessages,
            String groupId,
            String isolationLevel
    ) {
        public static KafkaValidationConfig forValidation(String topic, int expectedMessages) {
            return new KafkaValidationConfig(topic, expectedMessages, 5000, expectedMessages * 2,
                    "test-validation-group", "read_committed");
        }

        public static KafkaValidationConfig forFinalCheck(String topic, int expectedMessages) {
            return new KafkaValidationConfig(topic, expectedMessages, 10000, expectedMessages * 2,
                    "test-final-validation-group", "read_committed");
        }
    }

    public boolean waitForAndValidateKafkaOutput(String topic, int expectedMessages) throws Exception {
        KafkaValidationConfig config = KafkaValidationConfig.forValidation(topic, expectedMessages);
        return validateMessages(config, DEFAULT_TIMEOUT_SECONDS, true);
    }

    public boolean performFinalValidation(String topic, int expectedMessages) throws Exception {
        LOG.info("✅ Proceeding with Flink job shutdown and final Kafka validation...");
        KafkaValidationConfig config = KafkaValidationConfig.forFinalCheck(topic, expectedMessages);
        boolean valid = validateMessages(config, 0, false);
        if (!valid) {
            return false;
        }
        return validateMessageSequence(getMessages(config), expectedMessages);
    }

    public void createAndFillInInputTopic() throws IOException, InterruptedException {
        KafkaTopicConfig topicConfig = new KafkaTopicConfig(
                "input-topic",
                1,
                1,
                KAFKA_BOOTSTRAP_SERVER
        );

        createTopic(topicConfig);
        String messages = generateMessages(this.messages);
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
        for (int i = 0; i < count; i++) {
            sb.append(i).append("\n");
        }
        return sb.toString();
    }

    private void produceMessages(KafkaTopicConfig config, String messages) throws IOException, InterruptedException {
        LOG.info("Producing {} messages into '{}'", this.messages, config.name);
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
                        config.bootstrapServer, config.name, CONSUMER_TIMEOUT_MS, messages)
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

    private boolean validateMessages(KafkaValidationConfig config, int timeoutSeconds, boolean waitForStability) throws Exception {
        LOG.info("Waiting for {} messages to appear in Kafka topic: {}", config.expectedMessages(), config.topic());
        Set<String> messages;
        long start = System.currentTimeMillis();

        do {
            messages = getMessages(config);
            LOG.info("Kafka topic {} currently contains {} unique messages", config.topic(), messages.size());

            if (messages.size() == config.expectedMessages()) {
                if (waitForStability) {
                    LOG.info("✅ Detected {} unique messages. Waiting {} ms to ensure stability...",
                            config.expectedMessages(), STABILITY_WAIT_MS);
                    Thread.sleep(STABILITY_WAIT_MS);
                }
                break;
            }

            if (timeoutSeconds > 0) {
                Thread.sleep(POLL_INTERVAL_MS);
            }
        } while (timeoutSeconds > 0 && (System.currentTimeMillis() - start) < timeoutSeconds * 1000L);

        if (messages.size() != config.expectedMessages()) {
            throw new IllegalStateException("❌ Expected " + config.expectedMessages() +
                    " unique messages in topic " + config.topic() + ". Found " + messages.size());
        }
        return true;
    }

    public void checkKafkaUniqueIds(String topic, int expectedMessages) throws Exception {
        KafkaValidationConfig config = KafkaValidationConfig.forFinalCheck(topic, expectedMessages);
        Set<String> messages = getMessages(config);
        validateMessageSequence(messages, expectedMessages);
    }

    private Set<String> getMessages(KafkaValidationConfig config) throws IOException, InterruptedException {
        String command = String.format("%s --bootstrap-server %s --topic %s --from-beginning " +
                        "--timeout-ms %d --max-messages %d --consumer-property group.id=%s " +
                        "--consumer-property isolation.level=%s",
                KAFKA_CONSUMER_CMD, BOOTSTRAP_SERVER, config.topic(), config.timeoutMs(),
                config.maxMessages(), UUID.randomUUID(), config.isolationLevel());

        Container.ExecResult result = kafka.execInContainer("/bin/sh", "-c", command);
        return Arrays.stream(result.getStdout().split("\n"))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toSet());
    }

    private boolean validateMessageSequence(Set<String> messages, int expectedMessages) {
        List<Integer> ids = messages.stream()
                .map(s -> {
                    try {
                        return Integer.parseInt(s);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Non-integer message found: " + s);
                    }
                })
                .sorted()
                .toList();
        LOG.info("Found {} messages in Kafka topic: {}", ids.size(), ids);

        for (int i = 0; i < expectedMessages; i++) {
            if (!ids.contains(i)) {
                LOG.error("❌ Missing expected message ID: {}", i);
                return false;
            }
        }
        LOG.info("✅ All {} messages validated successfully and are unique from 0 to {}",
                expectedMessages, expectedMessages);
        return true;
    }

    public void printMessages(String topic, int timeoutMs, int maxMessages)
            throws InterruptedException, IOException {
        String command = String.format(
                "kafka-console-consumer --bootstrap-server localhost:9093 " +
                        "--topic %s --from-beginning --timeout-ms %d --max-messages %d " +
                        "--consumer-property group.id=test-group-%s " +
                        "--consumer-property isolation.level=read_uncommitted",
                topic, timeoutMs, maxMessages, UUID.randomUUID());

        Container.ExecResult result = kafka.execInContainer("/bin/sh", "-c", command);
        LOG.info("Consumer output: {}", result.getStdout());
    }
}
