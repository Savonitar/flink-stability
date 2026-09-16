package org.savonitar.flink.stability.core.validation.kafka;

import java.time.Duration;

/**
 * Captures one fixed raw Kafka boundary and traverses it with committed isolation under one
 * non-resetting absolute deadline.
 */
@FunctionalInterface
public interface KafkaSnapshotReader {
    KafkaTopicSnapshot read(
            String bootstrapServers,
            String topic,
            Duration timeout,
            long maximumRecords) throws KafkaSnapshotException;
}
