package org.savonitar.flink.stability.core.spec;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Raised when parameters/defaults cannot produce a valid executable scenario. */
public final class ScenarioResolutionException extends IllegalArgumentException {
    private static final Comparator<ResolutionIssue> ISSUE_ORDER = Comparator
            .comparing((ResolutionIssue issue) -> issue.source().toString())
            .thenComparing(ResolutionIssue::scope)
            .thenComparing(ResolutionIssue::path)
            .thenComparing(ResolutionIssue::code)
            .thenComparing(ResolutionIssue::message);

    private final List<ResolutionIssue> issues;

    public ScenarioResolutionException(List<ResolutionIssue> issues) {
        super(formatMessage(issues));
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = normalized(issues);
    }

    public List<ResolutionIssue> issues() {
        return issues;
    }

    private static String formatMessage(List<ResolutionIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            return "Scenario resolution failed";
        }
        return normalized(issues).stream()
                .map(issue -> issue.code() + " in " + issue.source() + " [" + issue.scope() + "] at "
                        + issue.path() + ": " + issue.message())
                .collect(Collectors.joining("; ", "Scenario resolution failed: ", ""));
    }

    private static List<ResolutionIssue> normalized(List<ResolutionIssue> issues) {
        Map<IssueIdentity, ResolutionIssue> unique = new LinkedHashMap<>();
        issues.stream().sorted(ISSUE_ORDER).forEach(issue -> unique.putIfAbsent(
                new IssueIdentity(issue.source().toString(), issue.scope(), issue.code(), issue.path()), issue));
        return List.copyOf(unique.values());
    }

    private record IssueIdentity(String source, ResolutionScope scope, String code, String path) {}
}
