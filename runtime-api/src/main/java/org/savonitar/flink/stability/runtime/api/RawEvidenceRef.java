package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;
import java.util.Objects;

/**
 * An exact retained response body (or explicitly named source-byte projection), before typed
 * parsing. Hashes attest retained-byte consistency, not authenticity of the responding process.
 * Monotonic timestamps belong to the harness JVM and are not container or JobManager clocks.
 */
public record RawEvidenceRef(String path, String sha256, long bytes, String kind, long sequence,
                             long startedNanos, long completedNanos, String group,
                             String method, String endpoint, int status) {
    public RawEvidenceRef {
        path = Checks.requireNonBlank(path, "path");
        if (!Path.of(path).isAbsolute() || !Path.of(path).normalize().toString().equals(path))
            throw new IllegalArgumentException("Raw evidence path must be absolute and normalized");
        if (!Objects.requireNonNull(sha256, "sha256").matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Invalid raw evidence SHA-256");
        if (bytes < 0 || sequence < 1 || completedNanos - startedNanos < 0)
            throw new IllegalArgumentException("Invalid raw evidence size, sequence or interval");
        kind = Checks.requireNonBlank(kind, "kind");
        group = Checks.requireNonBlank(group, "group");
        method = Checks.requireNonBlank(method, "method");
        endpoint = Checks.requireNonBlank(endpoint, "endpoint");
        if (status < 0 || status > 599) throw new IllegalArgumentException("Invalid response status");
    }
}
