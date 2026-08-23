package org.savonitar.flink.stability.core.spec;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Raised when declared artifacts cannot be made reproducibly executable. */
public final class ArtifactResolutionException extends IllegalArgumentException {
    private static final Comparator<ArtifactIssue> ISSUE_ORDER = Comparator
            .comparing((ArtifactIssue issue) -> issue.source().toString())
            .thenComparing(ArtifactIssue::scope)
            .thenComparing(ArtifactIssue::path)
            .thenComparing(ArtifactIssue::code)
            .thenComparing(ArtifactIssue::message);

    private final List<ArtifactIssue> issues;

    public ArtifactResolutionException(List<ArtifactIssue> issues) {
        super(formatMessage(issues));
        if (issues == null || issues.isEmpty()) {
            throw new IllegalArgumentException("issues must not be empty");
        }
        this.issues = normalized(issues);
    }

    public List<ArtifactIssue> issues() {
        return issues;
    }

    private static String formatMessage(List<ArtifactIssue> issues) {
        if (issues == null || issues.isEmpty()) {
            return "Artifact resolution failed";
        }
        return normalized(issues).stream()
                .map(issue -> issue.code() + " in " + issue.source() + " ["
                        + issue.scope() + "] at " + issue.path() + ": " + issue.message())
                .collect(Collectors.joining("; ", "Artifact resolution failed: ", ""));
    }

    private static List<ArtifactIssue> normalized(List<ArtifactIssue> issues) {
        Map<IssueIdentity, ArtifactIssue> unique = new LinkedHashMap<>();
        issues.stream().sorted(ISSUE_ORDER).forEach(issue -> unique.putIfAbsent(
                new IssueIdentity(
                        issue.source().toString(), issue.scope(), issue.code(), issue.path()),
                issue));
        return List.copyOf(unique.values());
    }

    private record IssueIdentity(
            String source,
            ResolutionScope scope,
            String code,
            String path) {}
}
