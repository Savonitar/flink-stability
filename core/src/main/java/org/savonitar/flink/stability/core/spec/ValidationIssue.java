package org.savonitar.flink.stability.core.spec;

import java.util.Objects;

/** A project-owned, machine-readable preflight validation problem. */
public record ValidationIssue(String code, String path, String message) {
    public ValidationIssue {
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }
}
