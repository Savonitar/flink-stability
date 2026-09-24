package org.savonitar.flink.stability.core.spec.document;

import java.nio.file.Path;
import java.util.Objects;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Stable report and artifact identity for one source-ordered suite membership. */
public record SuiteEntryIdentity(
        Path suiteSource,
        String suiteName,
        int entryIndex,
        String entryPointer,
        String entryId,
        String scenarioName) {
    public SuiteEntryIdentity {
        suiteSource = Objects.requireNonNull(suiteSource, "suiteSource")
                .toAbsolutePath().normalize();
        requireNonBlank(suiteName, "suiteName");
        if (entryIndex < 0) {
            throw new IllegalArgumentException("entryIndex must be non-negative");
        }
        String requiredPointer = "$/scenarios/" + entryIndex;
        if (!requiredPointer.equals(entryPointer)) {
            throw new IllegalArgumentException(
                    "entryPointer must be '" + requiredPointer + "'");
        }
        requireNonBlank(entryId, "entryId");
        requireNonBlank(scenarioName, "scenarioName");
    }
}
