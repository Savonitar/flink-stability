package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkJobObservation;

import java.util.List;
import java.util.Objects;

/**
 * Immutable ordered evidence for every atomic typed phase step that was attempted, plus the job
 * as observed just before each confirmed TaskManager kill.
 */
public record PhaseExecutionEvidence(
        List<StepEvidence> steps,
        List<TaskManagerKill> taskManagerKills) {
    public PhaseExecutionEvidence {
        steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
        taskManagerKills = List.copyOf(Objects.requireNonNull(
                taskManagerKills, "taskManagerKills"));
    }

    public PhaseExecutionEvidence(List<StepEvidence> steps) {
        this(steps, List.of());
    }

    /** A kill whose process exit was confirmed, and the job observed right before it. */
    public record TaskManagerKill(
            String path,
            List<LoopIteration> loopIterations,
            String target,
            FlinkJobObservation.Attempt jobBeforeKill) {
        public TaskManagerKill {
            path = requireNonBlank(path, "path");
            loopIterations = List.copyOf(Objects.requireNonNull(
                    loopIterations, "loopIterations"));
            target = requireNonBlank(target, "target");
            Objects.requireNonNull(jobBeforeKill, "jobBeforeKill");
        }
    }

    public record StepEvidence(
            int phaseIndex,
            String phaseName,
            String path,
            List<LoopIteration> loopIterations,
            StepKind kind,
            StepStatus status,
            String detail) {
        public StepEvidence {
            if (phaseIndex < 0) {
                throw new IllegalArgumentException("phaseIndex must not be negative");
            }
            phaseName = requireNonBlank(phaseName, "phaseName");
            path = requireNonBlank(path, "path");
            if (!path.startsWith("$/phases/")) {
                throw new IllegalArgumentException("path must identify an exact phase step");
            }
            loopIterations = List.copyOf(Objects.requireNonNull(
                    loopIterations, "loopIterations"));
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(status, "status");
            detail = requireNonBlank(detail, "detail");
        }
    }

    /** One active structural loop and its one-based iteration at the recorded step. */
    public record LoopIteration(String loopPath, int iteration, int totalIterations) {
        public LoopIteration {
            loopPath = requireNonBlank(loopPath, "loopPath");
            if (!loopPath.startsWith("$/phases/")) {
                throw new IllegalArgumentException("loopPath must identify an exact phase loop");
            }
            if (iteration < 1 || totalIterations < 1 || iteration > totalIterations) {
                throw new IllegalArgumentException(
                        "iteration must be within the loop's one-based iteration range");
            }
        }
    }

    public enum StepKind {
        AWAIT_JOB_STATE,
        AWAIT_CHECKPOINTS,
        WAIT,
        KILL_TASKMANAGER,
        RESTART_TASKMANAGER
    }

    public enum StepStatus {
        SUCCEEDED,
        FAILED
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
