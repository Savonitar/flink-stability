package org.savonitar.flink.stability.core.validation.kafka;

import java.time.Duration;

/** Raw-byte fixed-boundary read for the state oracle; legacy integer records use their old reader. */
@FunctionalInterface
public interface KafkaStateSnapshotReader {
    KafkaStateTopicSnapshot read(String bootstrapServers, String topic, Duration timeout,
            long maximumRecords, long maximumRetainedBytes) throws KafkaSnapshotException;
}
