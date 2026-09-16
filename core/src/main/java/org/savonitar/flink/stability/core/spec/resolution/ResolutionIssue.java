package org.savonitar.flink.stability.core.spec.resolution;

import java.nio.file.Path;
import java.util.Objects;

/** A source- and side-aware parameter-resolution problem. */
public record ResolutionIssue(
        Path source,
        ResolutionScope scope,
        String code,
        String path,
        String message) {
    public ResolutionIssue {
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }
}
