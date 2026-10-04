package org.savonitar.flink.stability.core.execution;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Identity and retained checkpoint location for one isolated scenario attempt. */
public record V1AttemptContext(
        int attemptOrdinal,
        String attemptNonce8,
        Path checkpointStorageRoot, Optional<Path> kafkaLogOutput, Optional<Path> retainCheckpoints) {
    public V1AttemptContext(int ordinal, String nonce, Path root, Optional<Path> kafkaLogOutput) {
        this(ordinal, nonce, root, kafkaLogOutput, Optional.empty());
    }
    public V1AttemptContext(int attemptOrdinal, String attemptNonce8, Path checkpointStorageRoot) {
        this(attemptOrdinal, attemptNonce8, checkpointStorageRoot, Optional.empty());
    }
    public V1AttemptContext withKafkaLogOutput(Path output) {
        return new V1AttemptContext(attemptOrdinal, attemptNonce8, checkpointStorageRoot,
                Optional.of(output.toAbsolutePath().normalize()), retainCheckpoints);
    }
    public V1AttemptContext withRetainCheckpoints(Path output) {
        return new V1AttemptContext(attemptOrdinal, attemptNonce8, checkpointStorageRoot, kafkaLogOutput,
                Optional.of(output.toAbsolutePath().normalize()));
    }
    private static final Pattern NONCE = Pattern.compile("^[a-z0-9]{8}$");

    public V1AttemptContext {
        Objects.requireNonNull(kafkaLogOutput, "kafkaLogOutput");
        Objects.requireNonNull(retainCheckpoints, "retainCheckpoints");
        if (attemptOrdinal < 1) {
            throw new IllegalArgumentException("attemptOrdinal must be positive");
        }
        Objects.requireNonNull(attemptNonce8, "attemptNonce8");
        if (!NONCE.matcher(attemptNonce8).matches()) {
            throw new IllegalArgumentException(
                    "attemptNonce8 must contain exactly eight lowercase letters or digits");
        }
        checkpointStorageRoot = Objects.requireNonNull(
                        checkpointStorageRoot, "checkpointStorageRoot")
                .toAbsolutePath()
                .normalize();
    }
}
