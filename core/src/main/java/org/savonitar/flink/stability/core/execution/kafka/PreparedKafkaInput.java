package org.savonitar.flink.stability.core.execution.kafka;

import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;

import java.util.Map;
import java.util.Objects;

/** Runtime endpoints paired with the exact immutable input evidence prepared through them. */
public record PreparedKafkaInput(
        KafkaRuntimeEndpoints endpoints,
        KafkaInputManifest inputManifest) {

    public PreparedKafkaInput {
        Objects.requireNonNull(endpoints, "endpoints");
        Objects.requireNonNull(inputManifest, "inputManifest");
        if (!endpoints.clusterAlias().equals(inputManifest.clusterAlias())) {
            throw new IllegalArgumentException(
                    "Kafka endpoints and input manifest must identify the same cluster");
        }
        if (inputManifest.evidenceStatus() != KafkaInputManifest.EvidenceStatus.COMPLETE) {
            throw new IllegalArgumentException("Prepared Kafka input requires complete evidence");
        }
    }

    public Map<Integer, Long> stoppingOffsets() {
        return inputManifest.exclusiveEndOffsets();
    }
}
