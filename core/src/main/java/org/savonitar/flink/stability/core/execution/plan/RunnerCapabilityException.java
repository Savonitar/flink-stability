package org.savonitar.flink.stability.core.execution.plan;

import org.savonitar.flink.stability.core.spec.resolution.ResolutionScope;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Raised before provisioning when a resolved v1 document exceeds runner capabilities. */
public final class RunnerCapabilityException extends IllegalArgumentException {
    private static final Comparator<RunnerCapabilityIssue> ISSUE_ORDER = Comparator
            .comparing((RunnerCapabilityIssue issue) -> issue.source().toString())
            .thenComparing(RunnerCapabilityIssue::scope)
            .thenComparing(RunnerCapabilityIssue::path)
            .thenComparing(RunnerCapabilityIssue::code)
            .thenComparing(RunnerCapabilityIssue::message);

    private final List<RunnerCapabilityIssue> issues;

    public RunnerCapabilityException(List<RunnerCapabilityIssue> issues) {
        super(formatMessage(issues));
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = normalized(issues);
    }

    public List<RunnerCapabilityIssue> issues() {
        return issues;
    }

    private static String formatMessage(List<RunnerCapabilityIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            return "Runner capability validation failed";
        }
        return normalized(issues).stream()
                .map(issue -> issue.code() + " in " + issue.source() + " ["
                        + issue.scope() + "] at " + issue.path() + ": " + issue.message())
                .collect(Collectors.joining(
                        "; ", "Runner capability validation failed: ", ""));
    }

    private static List<RunnerCapabilityIssue> normalized(List<RunnerCapabilityIssue> issues) {
        Map<IssueIdentity, RunnerCapabilityIssue> unique = new LinkedHashMap<>();
        issues.stream().sorted(ISSUE_ORDER).forEach(issue -> unique.putIfAbsent(
                new IssueIdentity(
                        issue.source().toString(), issue.scope(), issue.code(), issue.path()),
                issue));
        return List.copyOf(unique.values());
    }

    private record IssueIdentity(
            String source,
            org.savonitar.flink.stability.core.spec.resolution.ResolutionScope scope,
            String code,
            String path) {}
}
