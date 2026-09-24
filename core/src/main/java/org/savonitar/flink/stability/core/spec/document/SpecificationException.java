package org.savonitar.flink.stability.core.spec.document;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Raised when a specification cannot be prepared for execution. The stage names the preparation
 * step that found the diagnostics; every stage is reported to the user as a validation failure.
 */
public final class SpecificationException extends IllegalArgumentException {
    private static final Comparator<Diagnostic> ORDER = Comparator
            .comparingInt(SpecificationException::entryIndex)
            .thenComparing(diagnostic -> diagnostic.source().toString())
            .thenComparing(Diagnostic::scope)
            .thenComparing(Diagnostic::path)
            .thenComparing(Diagnostic::code)
            .thenComparing(Diagnostic::message);

    private final Stage stage;
    private final List<Diagnostic> diagnostics;

    public SpecificationException(Stage stage, List<Diagnostic> diagnostics) {
        this(stage, diagnostics, null);
    }

    public SpecificationException(Stage stage, List<Diagnostic> diagnostics, Throwable cause) {
        super(message(stage, diagnostics), cause);
        this.stage = stage;
        this.diagnostics = normalized(stage, diagnostics);
    }

    public Stage stage() {
        return stage;
    }

    public List<Diagnostic> diagnostics() {
        return diagnostics;
    }

    /** The preparation steps, in the order a scenario or suite passes through them. */
    public enum Stage {
        /** Parsing, dispatch, and schema validation of one document. */
        DOCUMENT("Invalid specification", false),
        /** Cross-document checks of the discovered catalog. */
        CATALOG("Invalid specification catalog", true),
        /** Parameter materialization and the checks of each materialized side. */
        RESOLUTION("Scenario resolution failed", true),
        /** Selecting one expectation from the expected-result contract. */
        EXPECTATION("Expected-result selection failed", true),
        /** Semantic checks of a resolved scenario before provisioning. */
        PREFLIGHT("Scenario preflight failed", true),
        /** Making the declared artifacts reproducibly executable. */
        ARTIFACT("Artifact resolution failed", true),
        /** Eager validation of every entry of a suite. */
        SUITE_PLANNING("Suite planning failed", true),
        /** Features of a valid scenario that the v1 runner cannot execute. */
        RUNNER_CAPABILITY("Runner capability validation failed", true);

        private final String summary;
        private final boolean sortsAndDeduplicates;

        Stage(String summary, boolean sortsAndDeduplicates) {
            this.summary = summary;
            this.sortsAndDeduplicates = sortsAndDeduplicates;
        }
    }

    /**
     * Sorts the diagnostics and keeps one per entry, source, side, code, and pointer. Document
     * diagnostics keep the order and multiplicity the loader reported them in: one schema
     * pointer can fail several ways, and each failure is worth showing.
     */
    private static List<Diagnostic> normalized(Stage stage, List<Diagnostic> diagnostics) {
        Objects.requireNonNull(stage, "stage");
        if (diagnostics == null || diagnostics.isEmpty()) {
            throw new IllegalArgumentException("diagnostics must not be empty");
        }
        if (!stage.sortsAndDeduplicates) {
            return List.copyOf(diagnostics);
        }
        Map<List<Object>, Diagnostic> unique = new LinkedHashMap<>();
        diagnostics.stream().sorted(ORDER).forEach(diagnostic -> unique.putIfAbsent(
                List.of(entryIndex(diagnostic), diagnostic.source().toString(),
                        diagnostic.scope(), diagnostic.code(), diagnostic.path()),
                diagnostic));
        return List.copyOf(unique.values());
    }

    private static String message(Stage stage, List<Diagnostic> diagnostics) {
        return normalized(stage, diagnostics).stream()
                .map(diagnostic -> diagnostic.code()
                        + diagnostic.entry()
                                .map(entry -> " for " + entry.suiteName() + "["
                                        + entry.entryIndex() + ":" + entry.entryId() + "]")
                                .orElse("")
                        + " in " + diagnostic.source() + " [" + diagnostic.scope() + "] at "
                        + diagnostic.path() + ": " + diagnostic.message())
                .collect(Collectors.joining("; ", stage.summary + ": ", ""));
    }

    private static int entryIndex(Diagnostic diagnostic) {
        return diagnostic.entry().map(SuiteEntryIdentity::entryIndex).orElse(-1);
    }
}
