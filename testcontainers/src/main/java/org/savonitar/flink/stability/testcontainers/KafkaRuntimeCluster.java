package org.savonitar.flink.stability.testcontainers;

import java.util.List;
import java.nio.file.Path;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;

interface KafkaRuntimeCluster extends org.savonitar.flink.stability.runtime.api.KafkaBrokerControl {
    void start();

    void stop();

    KafkaRuntimeEndpoints endpoints();

    default java.util.Optional<org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence> runtimeEvidence() {
        return java.util.Optional.empty();
    }
    default KafkaLogCapture captureKafkaLogs(
            List<KafkaLogCapture.Partition> partitions,
            Path directory, MonotonicDeadline deadline) {
        throw new UnsupportedOperationException("Owned Kafka log collection is unavailable");
    }
}
