package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.spec.ArtifactIssue;
import org.savonitar.flink.stability.core.spec.ArtifactResolutionException;
import org.savonitar.flink.stability.core.spec.CatalogIssue;
import org.savonitar.flink.stability.core.spec.CatalogValidationException;
import org.savonitar.flink.stability.core.spec.DocumentValidationException;
import org.savonitar.flink.stability.core.spec.ExpectationIssue;
import org.savonitar.flink.stability.core.spec.ExpectedResultSelectionException;
import org.savonitar.flink.stability.core.spec.PreflightIssue;
import org.savonitar.flink.stability.core.spec.ResolutionIssue;
import org.savonitar.flink.stability.core.spec.ScenarioPreflightException;
import org.savonitar.flink.stability.core.spec.ScenarioResolutionException;
import org.savonitar.flink.stability.core.spec.SuitePlanningException;
import org.savonitar.flink.stability.core.spec.SuitePlanningIssue;
import org.savonitar.flink.stability.core.spec.ValidationIssue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Converts typed core diagnostics into stable, source-aware CLI lines. */
final class ValidationDiagnosticRenderer {
    List<String> render(IllegalArgumentException failure, Path catalogRoot) {
        Objects.requireNonNull(failure, "failure");
        Path root = Objects.requireNonNull(catalogRoot, "catalogRoot")
                .toAbsolutePath().normalize();
        List<String> lines = new ArrayList<>();
        lines.add("validation failed:");

        if (failure instanceof DocumentValidationException exception) {
            for (ValidationIssue issue : exception.issues()) {
                lines.add(line(exception.source(), root, "common", issue.code(),
                        issue.path(), issue.message()));
            }
        } else if (failure instanceof CatalogValidationException exception) {
            for (CatalogIssue issue : exception.issues()) {
                lines.add(line(issue.source(), root, "common", issue.code(),
                        issue.path(), issue.message()));
            }
        } else if (failure instanceof ScenarioResolutionException exception) {
            for (ResolutionIssue issue : exception.issues()) {
                lines.add(line(issue.source(), root, scope(issue.scope()), issue.code(),
                        issue.path(), issue.message()));
            }
        } else if (failure instanceof ExpectedResultSelectionException exception) {
            for (ExpectationIssue issue : exception.issues()) {
                lines.add(line(issue.source(), root, "common", issue.code(),
                        issue.path(), issue.message()));
            }
        } else if (failure instanceof ScenarioPreflightException exception) {
            for (PreflightIssue issue : exception.issues()) {
                lines.add(line(issue.source(), root, scope(issue.scope()), issue.code(),
                        issue.path(), issue.message()));
            }
        } else if (failure instanceof ArtifactResolutionException exception) {
            for (ArtifactIssue issue : exception.issues()) {
                lines.add(line(issue.source(), root, scope(issue.scope()), issue.code(),
                        issue.path(), issue.message()));
            }
        } else if (failure instanceof SuitePlanningException exception) {
            for (SuitePlanningIssue issue : exception.issues()) {
                String entry = "entry " + issue.entry().entryIndex() + " '"
                        + issue.entry().entryId() + "'";
                lines.add(line(issue.source(), root, entry + ", " + scope(issue.scope()),
                        issue.code(), issue.path(), issue.message()));
            }
        } else {
            throw new IllegalArgumentException("Unsupported validation failure", failure);
        }
        return List.copyOf(lines);
    }

    private static String line(
            Path source,
            Path root,
            String context,
            String code,
            String pointer,
            String message) {
        return "  " + displayPath(source, root) + " [" + context + "] " + code
                + " at " + pointer + ": " + message;
    }

    private static String scope(Enum<?> scope) {
        return scope.name().toLowerCase(java.util.Locale.ROOT);
    }

    private static String displayPath(Path source, Path root) {
        Path normalized = source.toAbsolutePath().normalize();
        if (normalized.startsWith(root)) {
            Path relative = root.relativize(normalized);
            return relative.getNameCount() == 0 ? "." : relative.toString();
        }
        return normalized.toString();
    }
}
