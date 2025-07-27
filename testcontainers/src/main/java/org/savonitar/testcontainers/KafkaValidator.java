package org.savonitar.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaValidator {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaValidator.class);
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    private static final int STABILITY_WAIT_MS = 10_000;
    private static final int POLL_INTERVAL_MS = 2000;
    private static final String KAFKA_CONSUMER_CMD = "kafka-console-consumer";
    private static final String BOOTSTRAP_SERVER = "localhost:9093";

    private final ConfluentKafkaContainer kafka;

    public KafkaValidator(ConfluentKafkaContainer kafka) {
        this.kafka = kafka;
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
            return new KafkaValidationConfig(topic, expectedMessages, 5000, 10000,
                    "test-validation-group", "read_committed");
        }

        public static KafkaValidationConfig forFinalCheck(String topic, int expectedMessages) {
            return new KafkaValidationConfig(topic, expectedMessages, 10000, 10000,
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
        if (valid) {
            validateMessageSequence(getMessages(config), expectedMessages);
        }
        return true;
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

    private void validateMessageSequence(Set<String> messages, int expectedMessages) {
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

        for (int i = 1; i <= expectedMessages; i++) {
            if (!ids.contains(i)) {
                throw new AssertionError("❌ Missing expected message ID: " + i);
            }
        }
        LOG.info("✅ All {} messages validated successfully and are unique from 1 to {}",
                expectedMessages, expectedMessages);
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
