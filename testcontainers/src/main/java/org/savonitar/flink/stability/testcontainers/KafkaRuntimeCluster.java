package org.savonitar.flink.stability.testcontainers;

import java.util.List;
import java.nio.file.Path;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;

interface KafkaRuntimeCluster {
    void start();

    void stop();

    KafkaRuntimeEndpoints endpoints();
    default KafkaLogCapture captureKafkaLogs(
            List<KafkaLogCapture.Partition> partitions,
            Path directory, MonotonicDeadline deadline) {
        throw new UnsupportedOperationException("Owned Kafka log collection is unavailable");
    }
}
