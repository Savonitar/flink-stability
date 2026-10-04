package org.savonitar.flink.stability.runtime.api;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Published Kafka endpoints and their resolved runtime identity. */
public record KafkaRuntimeEndpoints(
        String clusterAlias,
        String imageReference,
        String internalBootstrapServers,
        String hostBootstrapServers, java.util.Optional<KafkaRuntimeEvidence> runtimeEvidence) {

    public KafkaRuntimeEndpoints(String clusterAlias, String imageReference, String internalBootstrapServers, String hostBootstrapServers) {
        this(clusterAlias, imageReference, internalBootstrapServers, hostBootstrapServers, java.util.Optional.empty());
    }

    public KafkaRuntimeEndpoints {
        java.util.Objects.requireNonNull(runtimeEvidence, "runtimeEvidence");
        clusterAlias = requireNonBlank(clusterAlias, "clusterAlias");
        imageReference = requireNonBlank(imageReference, "imageReference");
        internalBootstrapServers = requireNonBlank(
                internalBootstrapServers, "internalBootstrapServers");
        hostBootstrapServers = requireNonBlank(hostBootstrapServers, "hostBootstrapServers");
    }
}
