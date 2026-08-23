package org.savonitar.flink.stability.core.spec;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Raised when an expected-result contract cannot select one valid expectation. */
public final class ExpectedResultSelectionException extends IllegalArgumentException {
    private static final Comparator<ExpectationIssue> ISSUE_ORDER = Comparator
            .comparing((ExpectationIssue issue) -> issue.source().toString())
            .thenComparing(ExpectationIssue::path)
            .thenComparing(ExpectationIssue::code)
            .thenComparing(ExpectationIssue::message);

    private final List<ExpectationIssue> issues;

    public ExpectedResultSelectionException(List<ExpectationIssue> issues) {
        super(formatMessage(issues));
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = normalized(issues);
    }

    public List<ExpectationIssue> issues() {
        return issues;
    }

    private static String formatMessage(List<ExpectationIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            return "Expected-result selection failed";
        }
        return normalized(issues).stream()
                .map(issue -> issue.code() + " in " + issue.source() + " at "
                        + issue.path() + ": " + issue.message())
                .collect(Collectors.joining("; ", "Expected-result selection failed: ", ""));
    }

    private static List<ExpectationIssue> normalized(List<ExpectationIssue> issues) {
        Map<IssueIdentity, ExpectationIssue> unique = new LinkedHashMap<>();
        issues.stream().sorted(ISSUE_ORDER).forEach(issue -> unique.putIfAbsent(
                new IssueIdentity(issue.source().toString(), issue.code(), issue.path()), issue));
        return List.copyOf(unique.values());
    }

    private record IssueIdentity(String source, String code, String path) {}
}
