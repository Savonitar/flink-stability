package org.savonitar.flink.stability.runtime.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable proof that every known Flink process was observed stopped. */
public record FlinkProcessWriteFenceEvidence(
        List<Component> components,
        Instant completedAt) {
    public FlinkProcessWriteFenceEvidence {
        components = List.copyOf(Objects.requireNonNull(components, "components"));
        Objects.requireNonNull(completedAt, "completedAt");
    }

    public record Component(
            String logicalName,
            FlinkComponentRole role,
            Optional<String> runtimeId,
            Outcome outcome) {
        public Component {
            logicalName = requireNonBlank(logicalName, "logicalName");
            Objects.requireNonNull(role, "role");
            runtimeId = Objects.requireNonNull(runtimeId, "runtimeId")
                    .map(value -> requireNonBlank(value, "runtimeId"));
            Objects.requireNonNull(outcome, "outcome");
        }
    }

    public enum Outcome {
        SIGKILLED,
        ALREADY_STOPPED
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
