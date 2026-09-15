package org.savonitar.flink.stability.core.execution;

import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

/** Identity and retained checkpoint location for one isolated scenario attempt. */
public record V1AttemptContext(
        int attemptOrdinal,
        String attemptNonce8,
        Path checkpointStorageRoot) {
    private static final Pattern NONCE = Pattern.compile("^[a-z0-9]{8}$");

    public V1AttemptContext {
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
