package org.savonitar.flink.stability.core.spec.resolution;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Raised when a resolved scenario is not safe to provision or execute. */
public final class ScenarioPreflightException extends IllegalArgumentException {
    private static final Comparator<PreflightIssue> ISSUE_ORDER = Comparator
            .comparing((PreflightIssue issue) -> issue.source().toString())
            .thenComparing(PreflightIssue::scope)
            .thenComparing(PreflightIssue::path)
            .thenComparing(PreflightIssue::code)
            .thenComparing(PreflightIssue::message);

    private final List<PreflightIssue> issues;

    public ScenarioPreflightException(List<PreflightIssue> issues) {
        super(formatMessage(issues));
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = normalized(issues);
    }

    public List<PreflightIssue> issues() {
        return issues;
    }

    private static String formatMessage(List<PreflightIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            return "Scenario preflight failed";
        }
        return normalized(issues).stream()
                .map(issue -> issue.code() + " in " + issue.source() + " [" + issue.scope() + "] at "
                        + issue.path() + ": " + issue.message())
                .collect(Collectors.joining("; ", "Scenario preflight failed: ", ""));
    }

    private static List<PreflightIssue> normalized(List<PreflightIssue> issues) {
        Map<IssueIdentity, PreflightIssue> unique = new LinkedHashMap<>();
        issues.stream().sorted(ISSUE_ORDER).forEach(issue -> unique.putIfAbsent(
                new IssueIdentity(
                        issue.source().toString(), issue.scope(), issue.code(), issue.path()),
                issue));
        return List.copyOf(unique.values());
    }

    private record IssueIdentity(
            String source, ResolutionScope scope, String code, String path) {}
}
