package org.savonitar.flink.stability.runtime.api;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/** One connector JAR checksum observed inside a created, not-yet-started container. */
public record ProvisionedConnectorArtifact(int index, String containerPath, String sha256) {
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    public ProvisionedConnectorArtifact {
        if (index < 0 || index > 99_999_999) {
            throw new IllegalArgumentException(
                    "index must fit exactly eight decimal digits");
        }
        containerPath = Objects.requireNonNull(containerPath, "containerPath");
        sha256 = Objects.requireNonNull(sha256, "sha256");
        if (!SHA_256.matcher(sha256).matches()) {
            throw new IllegalArgumentException(
                    "sha256 must contain 64 lowercase hexadecimal characters");
        }
        String expectedPath = String.format(
                Locale.ROOT,
                "/opt/flink/lib/flink-stability-connector-%08d-%s.jar",
                index,
                sha256);
        if (!containerPath.equals(expectedPath)) {
            throw new IllegalArgumentException(
                    "containerPath does not match its classpath index and SHA-256");
        }
    }
}
