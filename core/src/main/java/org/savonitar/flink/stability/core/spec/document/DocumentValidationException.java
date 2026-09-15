package org.savonitar.flink.stability.core.spec.document;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Raised when a specification cannot be parsed, dispatched, or schema-validated. */
public final class DocumentValidationException extends IllegalArgumentException {
    private final Path source;
    private final List<ValidationIssue> issues;

    public DocumentValidationException(Path source, ValidationIssue issue) {
        this(source, List.of(issue), null);
    }

    public DocumentValidationException(Path source, ValidationIssue issue, Throwable cause) {
        this(source, List.of(issue), cause);
    }

    public DocumentValidationException(Path source, List<ValidationIssue> issues) {
        this(source, issues, null);
    }

    private DocumentValidationException(Path source, List<ValidationIssue> issues, Throwable cause) {
        super(formatMessage(source, issues), cause);
        this.source = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = List.copyOf(issues);
    }

    public Path source() {
        return source;
    }

    public List<ValidationIssue> issues() {
        return issues;
    }

    private static String formatMessage(Path source, List<ValidationIssue> issues) {
        Objects.requireNonNull(source, "source");
        if (issues == null || issues.isEmpty()) {
            return "Invalid specification " + source;
        }
        String details = issues.stream()
                .map(issue -> issue.code() + " at " + issue.path() + ": " + issue.message())
                .collect(Collectors.joining("; "));
        return "Invalid specification " + source.toAbsolutePath().normalize() + ": " + details;
    }
}
