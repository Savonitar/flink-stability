package org.savonitar.flink.stability.core.flink;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One read-only REST view of the job state that fault-effect evidence needs.
 *
 * <p>Every timestamp is JobManager-clock epoch milliseconds. Compare timestamps only with each
 * other, never with harness time: containers need not share the harness clock. This observation
 * spans several REST calls; {@code jobManagerTimeMillis} belongs to its initial job-details
 * response and is not a timestamp for a subsequent kill.</p>
 */
public record FlinkJobObservation(
        long jobManagerTimeMillis,
        FlinkJobState state,
        long completedCheckpoints,
        long restoredCheckpoints,
        Optional<Restore> latestRestore,
        List<Failure> failures,
        List<Subtask> subtasks) {

    private static final Set<String> ACTIVE_SUBTASK_STATES =
            Set.of("DEPLOYING", "INITIALIZING", "RUNNING");

    public FlinkJobObservation {
        Objects.requireNonNull(state, "state");
        if (completedCheckpoints < 0 || restoredCheckpoints < 0) {
            throw new IllegalArgumentException("Checkpoint counts must not be negative");
        }
        latestRestore = Objects.requireNonNull(latestRestore, "latestRestore");
        failures = List.copyOf(Objects.requireNonNull(failures, "failures"));
        subtasks = List.copyOf(Objects.requireNonNull(subtasks, "subtasks"));
    }

    /** Failures that Flink recorded strictly after the given JobManager time, newest first. */
    public List<Failure> failuresAfter(long jobManagerTimeMillis) {
        return failures.stream()
                .filter(failure -> failure.timestampMillis() > jobManagerTimeMillis)
                .toList();
    }

    /** Subtasks that occupy a TaskManager: deployed, initializing, or running. */
    public List<Subtask> activeSubtasks() {
        return subtasks.stream()
                .filter(subtask -> ACTIVE_SUBTASK_STATES.contains(subtask.status()))
                .toList();
    }

    /** The latest checkpoint restore that Flink reports, with its JobManager restore time. */
    public record Restore(long checkpointId, long restoredAtMillis) {}

    /**
     * One exception-history entry. {@code rootCause} is the innermost cause line of Flink's
     * stack trace, bounded in length.
     */
    public record Failure(
            long timestampMillis,
            String exceptionName,
            String rootCause,
            Optional<String> taskManagerId) {
        public Failure {
            Objects.requireNonNull(exceptionName, "exceptionName");
            Objects.requireNonNull(rootCause, "rootCause");
            Objects.requireNonNull(taskManagerId, "taskManagerId");
        }
    }

    /** One subtask's current execution attempt and the TaskManager that hosts it, if any. */
    public record Subtask(
            String vertexName,
            int index,
            int attempt,
            String status,
            Optional<String> taskManagerId) {
        public Subtask {
            Objects.requireNonNull(vertexName, "vertexName");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(taskManagerId, "taskManagerId");
        }
    }

    /** One best-effort observation: exactly one of the observation or its failure is present. */
    public record Attempt(Optional<FlinkJobObservation> observation, Optional<String> failure) {
        public Attempt {
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(failure, "failure");
            if (observation.isPresent() == failure.isPresent()) {
                throw new IllegalArgumentException(
                        "An observation attempt has either an observation or a failure");
            }
        }

        /** Observes the job; a REST or parsing failure becomes evidence, never an exception. */
        public static Attempt of(FlinkJobControl jobs, FlinkJobHandle job) {
            try {
                return new Attempt(Optional.of(jobs.observe(job)), Optional.empty());
            } catch (IOException | RuntimeException failure) {
                return new Attempt(
                        Optional.empty(),
                        Optional.of(failure.getClass().getSimpleName() + ": "
                                + failure.getMessage()));
            }
        }
    }
}
