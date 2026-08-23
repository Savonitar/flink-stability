package org.savonitar.flink.stability.core.spec;

import java.nio.file.Path;
import java.util.Objects;

/** A source-, side-, and field-aware artifact preparation problem. */
public record ArtifactIssue(
        Path source,
        ResolutionScope scope,
        String code,
        String path,
        String message) {
    public ArtifactIssue {
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }
}
