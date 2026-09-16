package org.savonitar.flink.stability.core.spec.resolution;

import java.nio.file.Path;
import java.util.Objects;

/** A source-aware expected-result validation or selection problem. */
public record ExpectationIssue(Path source, String code, String path, String message) {
    public ExpectationIssue {
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }
}
