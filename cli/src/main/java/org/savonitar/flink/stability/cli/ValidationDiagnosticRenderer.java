package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Converts typed core diagnostics into stable, source-aware CLI lines. */
final class ValidationDiagnosticRenderer {
    List<String> render(SpecificationException failure, Path catalogRoot) {
        Objects.requireNonNull(failure, "failure");
        Path root = Objects.requireNonNull(catalogRoot, "catalogRoot")
                .toAbsolutePath().normalize();
        List<String> lines = new ArrayList<>();
        lines.add("validation failed:");
        for (Diagnostic diagnostic : failure.diagnostics()) {
            String scope = diagnostic.scope().name().toLowerCase(Locale.ROOT);
            String context = diagnostic.entry()
                    .map(entry -> "entry " + entry.entryIndex() + " '" + entry.entryId()
                            + "', " + scope)
                    .orElse(scope);
            lines.add("  " + displayPath(diagnostic.source(), root) + " [" + context + "] "
                    + diagnostic.code() + " at " + diagnostic.path() + ": "
                    + diagnostic.message());
        }
        return List.copyOf(lines);
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
