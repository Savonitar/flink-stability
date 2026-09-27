package org.savonitar.flink.stability.runtime.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

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

    public enum Moment {
        BEFORE_FENCE,
        AFTER_DECLARED_KILL,
        AFTER_FENCE_KILL
    }

    /** Missing/failed inspection is distinct from observing an exited process. */
    public record Observation(
            String logicalName, FlinkComponentRole role, Optional<String> runtimeId,
            Moment moment, Instant observedAt, Optional<FlinkHaControl.ProcessState> state,
            boolean missing, Optional<String> diagnostic) {
        public Observation {
            logicalName = requireNonBlank(logicalName, "logicalName");
            Objects.requireNonNull(role, "role");
            runtimeId = Objects.requireNonNull(runtimeId, "runtimeId")
                    .map(value -> requireNonBlank(value, "runtimeId"));
            Objects.requireNonNull(moment, "moment");
            Objects.requireNonNull(observedAt, "observedAt");
            Objects.requireNonNull(state, "state");
            diagnostic = Objects.requireNonNull(diagnostic, "diagnostic")
                    .map(value -> requireNonBlank(value, "diagnostic"));
            if (state.isPresent() && (missing || diagnostic.isPresent()
                    || !runtimeId.equals(Optional.of(state.orElseThrow().runtimeId())))) {
                throw new IllegalArgumentException("Observed process state must match its physical identity");
            }
            if (state.isEmpty() && !missing && diagnostic.isEmpty()) {
                throw new IllegalArgumentException("Unavailable process state requires a diagnostic");
            }
        }
    }

    /** Survives a failed fence; entries do not imply that every process was stopped. */
    public record Observations(List<Observation> observations, List<Component> fenced,
                               boolean overflow) {
        public static final int LIMIT = 1024;

        public Observations {
            observations = List.copyOf(Objects.requireNonNull(observations, "observations"));
            fenced = List.copyOf(Objects.requireNonNull(fenced, "fenced"));
            if (observations.size() > LIMIT || fenced.size() > LIMIT) {
                throw new IllegalArgumentException("Process observation limit exceeded");
            }
        }
    }
}
