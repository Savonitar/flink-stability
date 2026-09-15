package org.savonitar.flink.stability.core.spec.document;

import java.nio.file.Path;
import java.util.Objects;

/** A cross-document validation problem found during recursive discovery. */
public record CatalogIssue(Path source, String code, String path, String message) {
    public CatalogIssue {
        source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(message, "message");
    }
}
