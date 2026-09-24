package org.savonitar.flink.stability.runtime.api;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Published Kafka endpoints and their resolved runtime identity. */
public record KafkaRuntimeEndpoints(
        String clusterAlias,
        String imageReference,
        String internalBootstrapServers,
        String hostBootstrapServers) {

    public KafkaRuntimeEndpoints {
        clusterAlias = requireNonBlank(clusterAlias, "clusterAlias");
        imageReference = requireNonBlank(imageReference, "imageReference");
        internalBootstrapServers = requireNonBlank(
                internalBootstrapServers, "internalBootstrapServers");
        hostBootstrapServers = requireNonBlank(hostBootstrapServers, "hostBootstrapServers");
    }
}
