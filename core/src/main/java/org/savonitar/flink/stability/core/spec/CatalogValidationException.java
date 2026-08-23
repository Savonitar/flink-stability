package org.savonitar.flink.stability.core.spec;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Raised when discovered documents do not form a coherent v1 catalog. */
public final class CatalogValidationException extends IllegalArgumentException {
    private static final Comparator<CatalogIssue> ISSUE_ORDER = Comparator
            .comparing((CatalogIssue issue) -> issue.source().toString())
            .thenComparing(CatalogIssue::path)
            .thenComparing(CatalogIssue::code)
            .thenComparing(CatalogIssue::message);

    private final List<CatalogIssue> issues;

    public CatalogValidationException(List<CatalogIssue> issues) {
        super(formatMessage(issues));
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = normalized(issues);
    }

    public List<CatalogIssue> issues() {
        return issues;
    }

    private static String formatMessage(List<CatalogIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            return "Invalid specification catalog";
        }
        return normalized(issues).stream()
                .map(issue -> issue.code() + " in " + issue.source() + " at " + issue.path()
                        + ": " + issue.message())
                .collect(Collectors.joining("; ", "Invalid specification catalog: ", ""));
    }

    private static List<CatalogIssue> normalized(List<CatalogIssue> issues) {
        return issues.stream().distinct().sorted(ISSUE_ORDER).toList();
    }
}
