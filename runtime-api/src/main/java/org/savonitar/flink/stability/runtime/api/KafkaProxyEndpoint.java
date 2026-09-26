package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;
import java.util.Objects;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/**
 * A started Kafka proxy: the address routed clients use, the pinned proxy image, and the host
 * directory its fault filter reads rules from and writes evidence to (SPEC-004 K6.11).
 */
public record KafkaProxyEndpoint(
        String proxyAlias,
        String bootstrapServers,
        String imageReference,
        Path controlDirectory) {
    public KafkaProxyEndpoint {
        requireNonBlank(proxyAlias, "proxyAlias");
        requireNonBlank(bootstrapServers, "bootstrapServers");
        requireNonBlank(imageReference, "imageReference");
        Objects.requireNonNull(controlDirectory, "controlDirectory");
    }
}
