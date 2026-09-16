package org.savonitar.flink.stability.cli;

import java.util.Objects;

/** Small user-facing summary of a successfully prepared validation target. */
record ValidationSummary(TargetKind kind, String name, int entries, int artifacts) {
    ValidationSummary {
        Objects.requireNonNull(kind, "kind");
        if (Objects.requireNonNull(name, "name").isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (entries <= 0) {
            throw new IllegalArgumentException("entries must be positive");
        }
        if (artifacts < 0) {
            throw new IllegalArgumentException("artifacts must be non-negative");
        }
    }

    enum TargetKind {
        SCENARIO("scenario"),
        SUITE("suite");

        private final String displayName;

        TargetKind(String displayName) {
            this.displayName = displayName;
        }

        String displayName() {
            return displayName;
        }
    }
}
