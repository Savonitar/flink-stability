package org.savonitar.flink.stability.core.execution.plan;

import org.savonitar.flink.stability.core.spec.resolution.ResolutionScope;

import java.nio.file.Path;
import java.util.Objects;

/** One source- and side-aware reason that a valid scenario cannot run on the v1 runner. */
public record RunnerCapabilityIssue(
        Path source,
        ResolutionScope scope,
        String code,
        String path,
        String message) {
    public RunnerCapabilityIssue {
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(scope, "scope");
        code = requireNonBlank(code, "code");
        path = requireNonBlank(path, "path");
        message = requireNonBlank(message, "message");
        if (!path.startsWith("$")) {
            throw new IllegalArgumentException("path must be a root-relative JSON pointer");
        }
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
