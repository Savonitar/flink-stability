package org.savonitar.flink.stability.core.spec.resolution;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Raised after eager validation finds one or more invalid suite entries. */
public final class SuitePlanningException extends IllegalArgumentException {
    private static final Comparator<SuitePlanningIssue> ISSUE_ORDER = Comparator
            .comparingInt((SuitePlanningIssue issue) -> issue.entry().entryIndex())
            .thenComparing(issue -> issue.source().toString())
            .thenComparing(SuitePlanningIssue::scope)
            .thenComparing(SuitePlanningIssue::path)
            .thenComparing(SuitePlanningIssue::code)
            .thenComparing(SuitePlanningIssue::message);

    private final List<SuitePlanningIssue> issues;

    public SuitePlanningException(List<SuitePlanningIssue> issues) {
        super(formatMessage(issues));
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = normalized(issues);
    }

    public List<SuitePlanningIssue> issues() {
        return issues;
    }

    private static String formatMessage(List<SuitePlanningIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            return "Suite planning failed";
        }
        return normalized(issues).stream()
                .map(issue -> issue.code() + " for " + issue.entry().suiteName()
                        + "[" + issue.entry().entryIndex() + ":"
                        + issue.entry().entryId() + "] in " + issue.source()
                        + " [" + issue.scope() + "] at " + issue.path()
                        + ": " + issue.message())
                .collect(Collectors.joining("; ", "Suite planning failed: ", ""));
    }

    private static List<SuitePlanningIssue> normalized(List<SuitePlanningIssue> issues) {
        Map<IssueIdentity, SuitePlanningIssue> unique = new LinkedHashMap<>();
        issues.stream().sorted(ISSUE_ORDER).forEach(issue -> unique.putIfAbsent(
                new IssueIdentity(
                        issue.entry().entryIndex(),
                        issue.source().toString(),
                        issue.scope(),
                        issue.code(),
                        issue.path()),
                issue));
        return List.copyOf(unique.values());
    }

    private record IssueIdentity(
            int entryIndex,
            String source,
            ResolutionScope scope,
            String code,
            String path) {}
}
