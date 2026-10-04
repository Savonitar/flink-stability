package org.savonitar.flink.stability.runtime.api;

import java.util.List;
import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;
import static org.savonitar.flink.stability.runtime.api.Checks.requireSha256;

/** A connector already present in the image; its bytes are observed before process start. */
public record ImageConnectorArtifact(String alias, String containerPath, String sha256) {
    public static final List<String> ENTRY_CLASSES = List.of(
            "org.apache.flink.connector.kafka.source.KafkaSource",
            "org.apache.flink.connector.kafka.sink.KafkaSink");

    /** Covers current and legacy connector implementation classes, not just entry points. */
    public static boolean connectorClassEntry(String name) {
        return name.endsWith(".class") && (name.startsWith("org/apache/flink/connector/kafka/")
                || name.startsWith("org/apache/flink/streaming/connectors/kafka/"));
    }

    public ImageConnectorArtifact {
        alias = requireNonBlank(alias, "alias");
        sha256 = requireSha256(sha256, "Image connector sha256");
        containerPath = requireNonBlank(containerPath, "containerPath");
        if (!containerPath.matches("/(?:[A-Za-z0-9_.+-]+/)*[A-Za-z0-9_.+-]+\\.jar")
                || java.util.Arrays.stream(containerPath.split("/")).anyMatch(
                        part -> part.equals(".") || part.equals(".."))) {
            throw new IllegalArgumentException("Image connector path must be a normalized absolute JAR path"
                    + " using only letters, digits, dots, underscores, pluses and hyphens");
        }
    }
}
