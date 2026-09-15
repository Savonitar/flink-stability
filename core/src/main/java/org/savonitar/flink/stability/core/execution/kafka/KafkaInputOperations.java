package org.savonitar.flink.stability.core.execution.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Injectable Kafka boundary; production uses kafka-clients and unit tests remain Docker-free. */
interface KafkaInputOperations {

    void createTopics(
            List<TopicDefinition> topics,
            KafkaInputPreparationDeadline deadline) throws Exception;

    List<ProducedRecord> produceAndAwait(
            String topic,
            int partitions,
            long totalRecords,
            KafkaInputPreparationDeadline deadline) throws Exception;

    OffsetSnapshot readOffsets(
            String topic,
            int partitions,
            KafkaInputPreparationDeadline deadline) throws Exception;

    ReconciliationSnapshot reconcileFromZeroThrough(
            String topic,
            Map<Integer, Long> exclusiveEndOffsets,
            KafkaInputPreparationDeadline deadline) throws Exception;

    void close(Duration timeout) throws Exception;

    record TopicDefinition(
            String name,
            int partitions,
            short replicationFactor,
            TopicCleanupPolicy cleanupPolicy,
            Duration retention) {
        public TopicDefinition {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(cleanupPolicy, "cleanupPolicy");
            Objects.requireNonNull(retention, "retention");
            if (name.isBlank() || partitions < 1 || replicationFactor < 1
                    || retention.isZero() || retention.isNegative()) {
                throw new IllegalArgumentException("Invalid Kafka topic definition");
            }
        }
    }

    enum TopicCleanupPolicy {
        DELETE("delete");

        private final String configurationValue;

        TopicCleanupPolicy(String configurationValue) {
            this.configurationValue = configurationValue;
        }

        String configurationValue() {
            return configurationValue;
        }
    }

    record ProducedRecord(
            long id,
            int partition,
            long offset,
            List<HarnessProducerAttempt> producerAttempts) {
        public ProducedRecord {
            producerAttempts = List.copyOf(producerAttempts);
        }
    }

    /** One harness-issued send invocation; Kafka-internal retry attempts stay opaque. */
    record HarnessProducerAttempt(
            int ordinal,
            int targetPartition,
            int acknowledgedPartition,
            long acknowledgedOffset) {}

    record OffsetSnapshot(
            Map<Integer, Long> beginningOffsets,
            Map<Integer, Long> exclusiveEndOffsets) {}

    record ObservedRecord(byte[] value, int partition, long offset) {
        public ObservedRecord {
            value = value == null ? null : value.clone();
        }

        @Override
        public byte[] value() {
            return value == null ? null : value.clone();
        }
    }

    record ReconciliationSnapshot(
            List<ObservedRecord> records,
            boolean reachedEveryExclusiveEnd,
            Map<String, String> consumerConfiguration) {
        public ReconciliationSnapshot {
            records = List.copyOf(records);
            Objects.requireNonNull(consumerConfiguration, "consumerConfiguration");
            TreeMap<String, String> sorted = new TreeMap<>(consumerConfiguration);
            consumerConfiguration = Collections.unmodifiableMap(
                    new LinkedHashMap<>(sorted));
        }
    }

    /** Retains a completed reconciliation snapshot when only consumer cleanup failed. */
    final class ReconciliationCloseException extends RuntimeException {
        private final ReconciliationSnapshot snapshot;

        ReconciliationCloseException(
                String message,
                Throwable cause,
                ReconciliationSnapshot snapshot) {
            super(message, cause);
            this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        }

        ReconciliationSnapshot snapshot() {
            return snapshot;
        }
    }

    @FunctionalInterface
    interface Factory {
        KafkaInputOperations open(String hostBootstrapServers, Duration timeout) throws Exception;
    }
}
