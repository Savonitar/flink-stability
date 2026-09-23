package org.savonitar.flink.stability.core.spec.resolution;

import java.nio.file.Path;
import java.util.Objects;

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
        requireText(suiteName, "suiteName");
        if (entryIndex < 0) {
            throw new IllegalArgumentException("entryIndex must be non-negative");
        }
        String requiredPointer = "$/scenarios/" + entryIndex;
        if (!requiredPointer.equals(entryPointer)) {
            throw new IllegalArgumentException(
                    "entryPointer must be '" + requiredPointer + "'");
        }
        requireText(entryId, "entryId");
        requireText(scenarioName, "scenarioName");
    }

    private static void requireText(String value, String name) {
        if (Objects.requireNonNull(value, name).isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
