package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;

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
 * or the one taken before the process fence. Failure and restore timestamps must follow a
 * JobManager clock sample requested after the process exit was confirmed, and a restore must
 * not precede the matching host failure. Events in the ambiguous interval before that sample
 * cannot confirm the kill. Without a checkpoint, a later active execution attempt or a finished
 * job must show that execution resumed. Counters and JobManager-clock timestamps are compared
 * only with each other.</p>
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
        if (kill.jobManagerTimeAfterKill().isEmpty()) {
            return unconfirmed(kill, Outcome.EVIDENCE_UNAVAILABLE,
                    "the JobManager clock could not be sampled after the confirmed process exit");
        }
        long afterKill = kill.jobManagerTimeAfterKill().orElseThrow();
        if (afterKill < before.jobManagerTimeMillis()
                || after.jobManagerTimeMillis() < afterKill) {
            return unconfirmed(kill, Outcome.EVIDENCE_UNAVAILABLE,
                    "JobManager clock samples do not establish the order of the kill and recovery");
        }
        List<FlinkJobObservation.Failure> failures = after.failuresAfter(afterKill).stream()
                .filter(failure -> !before.failures().contains(failure))
                .toList();
        List<FlinkJobObservation.Failure> hostFailures = failures.stream()
                .filter(failure -> failure.taskManagerId().filter(hosts::contains).isPresent())
                .toList();
        Optional<FlinkJobObservation.Restore> restore = after.restoredCheckpoints()
                > before.restoredCheckpoints()
                ? after.latestRestore().filter(latest -> latest.restoredAtMillis() > afterKill)
                        // Flink timestamps have millisecond precision: equal times are ordered
                        // only to that precision, but an earlier restore cannot prove recovery.
                        .filter(latest -> hostFailures.stream().anyMatch(failure ->
                                latest.restoredAtMillis() >= failure.timestampMillis()))
                : Optional.empty();

        if (hostFailures.isEmpty()) {
            return new TaskManagerKillEffect(kill, Outcome.NO_RECOVERY_OBSERVED, restore, failures,
                    "Flink recorded " + failures.size() + " new failure(s) after the post-exit"
                            + " clock sample, none on a"
                            + " TaskManager that hosted active subtasks before it");
        }
        if (restore.isPresent()) {
            FlinkJobObservation.Restore restored = restore.orElseThrow();
            return new TaskManagerKillEffect(kill, Outcome.CHECKPOINT_RESTORED, restore, failures,
                    "Flink restored checkpoint " + restored.checkpointId() + " "
                            + (restored.restoredAtMillis() - afterKill)
                            + " ms after the post-exit clock sample");
        }
        if (before.completedCheckpoints() > 0) {
            return new TaskManagerKillEffect(kill, Outcome.NO_RECOVERY_OBSERVED, restore, failures,
                    before.completedCheckpoints() + " checkpoint(s) had completed before the kill,"
                            + " but no restore followed a matching post-kill host failure");
        }
        boolean restarted = after.state() == FlinkJobState.FINISHED
                || after.activeSubtasks().stream().anyMatch(current ->
                        before.subtasks().stream().anyMatch(previous ->
                                current.vertexName().equals(previous.vertexName())
                                        && current.index() == previous.index()
                                        && current.attempt() > previous.attempt()));
        if (!restarted) {
            return new TaskManagerKillEffect(kill, Outcome.NO_RECOVERY_OBSERVED, restore, failures,
                    "no checkpoint had completed before the kill and a host failure followed it,"
                            + " but no later active execution attempt or finished job was observed");
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
