package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkJobObservation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Whether one confirmed TaskManager kill observably disrupted the job and Flink recovered it
 * (SPEC-001 R6.12a). A process that died proves only the injection; this adds the effect.
 *
 * <p>The kill counts only if a TaskManager that hosted active subtasks just before it later
 * shows a failure, and, when a checkpoint had completed, Flink restored one afterwards. Each kill
 * is judged against the next observation of the same job: the one taken before the next kill,
 * or the one taken before the process fence. Counters and JobManager-clock timestamps are
 * compared only with each other.</p>
 */
public record TaskManagerKillEffect(
        PhaseExecutionEvidence.TaskManagerKill kill,
        Outcome outcome,
        Optional<FlinkJobObservation.Restore> restore,
        List<FlinkJobObservation.Failure> failuresAfterKill,
        String detail) {

    public TaskManagerKillEffect {
        Objects.requireNonNull(kill, "kill");
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(restore, "restore");
        failuresAfterKill = List.copyOf(Objects.requireNonNull(
                failuresAfterKill, "failuresAfterKill"));
        Objects.requireNonNull(detail, "detail");
    }

    public enum Outcome {
        /** Flink restored a checkpoint after the kill. */
        CHECKPOINT_RESTORED(true),
        /** No checkpoint existed yet; Flink recorded a failure after the kill and restarted. */
        RESTARTED_WITHOUT_CHECKPOINT(true),
        /** The job had already reached a terminal state, so the kill could not affect it. */
        JOB_TERMINAL_BEFORE_KILL(false),
        /** No subtask was deployed or running on any TaskManager when the kill happened. */
        NO_ACTIVE_SUBTASK_BEFORE_KILL(false),
        /** No failure on a TaskManager that hosted subtasks, or no restore of a checkpoint. */
        NO_RECOVERY_OBSERVED(false),
        /** The job could not be observed before or after the kill. */
        EVIDENCE_UNAVAILABLE(false);

        private final boolean confirmed;

        Outcome(boolean confirmed) {
            this.confirmed = confirmed;
        }

        public boolean confirmed() {
            return confirmed;
        }
    }

    /**
     * Evaluates kills in execution order. {@code jobAfterLastKill} is the observation taken when
     * the job completed, or empty when execution stopped before that point.
     */
    public static List<TaskManagerKillEffect> evaluate(
            List<PhaseExecutionEvidence.TaskManagerKill> kills,
            Optional<FlinkJobObservation.Attempt> jobAfterLastKill) {
        Objects.requireNonNull(kills, "kills");
        Objects.requireNonNull(jobAfterLastKill, "jobAfterLastKill");
        List<TaskManagerKillEffect> effects = new ArrayList<>();
        for (int index = 0; index < kills.size(); index++) {
            Optional<FlinkJobObservation.Attempt> next = index + 1 < kills.size()
                    ? Optional.of(kills.get(index + 1).jobBeforeKill())
                    : jobAfterLastKill;
            effects.add(evaluate(kills.get(index), next));
        }
        return List.copyOf(effects);
    }

    private static TaskManagerKillEffect evaluate(
            PhaseExecutionEvidence.TaskManagerKill kill,
            Optional<FlinkJobObservation.Attempt> next) {
        Optional<FlinkJobObservation> observedBefore = kill.jobBeforeKill().observation();
        if (observedBefore.isEmpty()) {
            return unconfirmed(kill, Outcome.EVIDENCE_UNAVAILABLE,
                    "the job was not observed before the kill: "
                            + kill.jobBeforeKill().failure().orElseThrow());
        }
        FlinkJobObservation before = observedBefore.orElseThrow();
        if (before.state().terminal()) {
            return unconfirmed(kill, Outcome.JOB_TERMINAL_BEFORE_KILL,
                    "the job was already " + before.state() + " before the kill");
        }
        Set<String> hosts = before.activeSubtasks().stream()
                .map(FlinkJobObservation.Subtask::taskManagerId)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        if (hosts.isEmpty()) {
            return unconfirmed(kill, Outcome.NO_ACTIVE_SUBTASK_BEFORE_KILL,
                    "no subtask was deployed or running on any TaskManager before the kill"
                            + " (job state " + before.state() + ")");
        }
        Optional<FlinkJobObservation> observedAfter =
                next.flatMap(FlinkJobObservation.Attempt::observation);
        if (observedAfter.isEmpty()) {
            return unconfirmed(kill, Outcome.EVIDENCE_UNAVAILABLE,
                    "the job was not observed after the kill"
                            + next.flatMap(FlinkJobObservation.Attempt::failure)
                                    .map(failure -> ": " + failure)
                                    .orElse(""));
        }
        FlinkJobObservation after = observedAfter.orElseThrow();
        long killedAt = before.jobManagerTimeMillis();
        List<FlinkJobObservation.Failure> failures = after.failuresAfter(killedAt);
        boolean hostFailed = failures.stream().anyMatch(failure ->
                failure.taskManagerId().filter(hosts::contains).isPresent());
        Optional<FlinkJobObservation.Restore> restore = after.restoredCheckpoints()
                > before.restoredCheckpoints()
                ? after.latestRestore().filter(latest -> latest.restoredAtMillis() > killedAt)
                : Optional.empty();

        if (!hostFailed) {
            return new TaskManagerKillEffect(kill, Outcome.NO_RECOVERY_OBSERVED, restore, failures,
                    "Flink recorded " + failures.size() + " failure(s) after the kill, none on a"
                            + " TaskManager that hosted active subtasks before it");
        }
        if (restore.isPresent()) {
            FlinkJobObservation.Restore restored = restore.orElseThrow();
            return new TaskManagerKillEffect(kill, Outcome.CHECKPOINT_RESTORED, restore, failures,
                    "Flink restored checkpoint " + restored.checkpointId() + " "
                            + (restored.restoredAtMillis() - killedAt)
                            + " ms after the pre-kill observation");
        }
        if (before.completedCheckpoints() > 0) {
            return new TaskManagerKillEffect(kill, Outcome.NO_RECOVERY_OBSERVED, restore, failures,
                    before.completedCheckpoints() + " checkpoint(s) had completed before the kill,"
                            + " but Flink restored none afterwards");
        }
        return new TaskManagerKillEffect(
                kill, Outcome.RESTARTED_WITHOUT_CHECKPOINT, restore, failures,
                "no checkpoint had completed before the kill; a TaskManager that hosted"
                        + " subtasks failed afterwards and Flink restarted the job");
    }

    private static TaskManagerKillEffect unconfirmed(
            PhaseExecutionEvidence.TaskManagerKill kill,
            Outcome outcome,
            String detail) {
        return new TaskManagerKillEffect(kill, outcome, Optional.empty(), List.of(), detail);
    }
}
