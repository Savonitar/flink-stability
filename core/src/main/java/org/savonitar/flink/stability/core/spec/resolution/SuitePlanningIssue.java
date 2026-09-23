package org.savonitar.flink.stability.core.spec.resolution;

import java.nio.file.Path;
import java.util.Objects;

/** A source- and suite-entry-aware problem found before any suite provisioning. */
public record SuitePlanningIssue(
        SuiteEntryIdentity entry,
        Path source,
        ResolutionScope scope,
        String code,
        String path,
        String message) {
    public SuitePlanningIssue {
        Objects.requireNonNull(entry, "entry");
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }
}
