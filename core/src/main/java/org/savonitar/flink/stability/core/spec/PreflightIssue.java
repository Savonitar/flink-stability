package org.savonitar.flink.stability.core.spec;

import java.nio.file.Path;
import java.util.Objects;

/** A source- and side-aware semantic problem found before provisioning. */
public record PreflightIssue(
        Path source,
        ResolutionScope scope,
        String code,
        String path,
        String message) {
    public PreflightIssue {
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }
}
