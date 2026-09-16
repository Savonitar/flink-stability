package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;

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

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
