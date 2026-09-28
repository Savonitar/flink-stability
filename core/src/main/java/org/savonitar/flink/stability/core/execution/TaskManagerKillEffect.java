package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Whether one confirmed TaskManager kill observably disrupted the job and Flink recovered it
 * (SPEC-001 R6.12a). A process that died proves only the injection; this adds the effect.
 *
 * <p>The kill counts only if the exact targeted incarnation hosted RUNNING subtasks and a new
 * failure identifies that incarnation, and, when a checkpoint had completed, Flink restored one
 * afterwards. A peer's narrowly recognized remote-transport failure may identify the target. Each kill
 * is judged against the next observation of the same job: the one taken before the next kill,
 * or the one taken before the process fence. Failure and restore timestamps must follow a
 * fresh JobManager clock sample requested after baseline/identity collection, just before injection.
 * A second sample after confirmed process exit must establish the other boundary, but failures
 * detected during the kill must not be discarded. A restore must not precede the matching failure.
 * Without a checkpoint, a later active execution attempt or a finished
 * job must show that execution resumed. Counters and JobManager-clock timestamps are compared
 * only with each other.</p>
 */
public record TaskManagerKillEffect(
        PhaseExecutionEvidence.TaskManagerKill kill,
        Outcome outcome,
        Optional<FlinkJobObservation.Restore> restore,
        List<FlinkJobObservation.Failure> failuresAfterKill,
        String detail) {

    // Deliberately recognize one concrete Flink transport diagnostic, not arbitrary mentions of
    // a ResourceID in application exceptions. The bracketed value must equal the full incarnation.
    private static final Pattern CLOSED_REMOTE_TASK_MANAGER = Pattern.compile(
            "^org\\.apache\\.flink\\.runtime\\.io\\.network\\.netty\\.exception\\.RemoteTransportException: "
                    + "Connection unexpectedly closed by remote task manager '[^'\\s]+ \\[ ([^\\]\\s]+) \\] '\\. "
                    + "This might indicate that the remote task manager was lost\\.$");

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
        /** No RUNNING subtask was observed on the exact targeted TaskManager. */
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

    /** Exact-incarnation matches among the new failures retained for this injection. */
    public List<FlinkJobObservation.Failure> qualifyingTargetFailures() {
        return targetFailures(kill, failuresAfterKill);
    }

    private static List<FlinkJobObservation.Failure> targetFailures(
            PhaseExecutionEvidence.TaskManagerKill kill,
            List<FlinkJobObservation.Failure> failures) {
        if (kill.identity().isEmpty()
                || !kill.target().equals(kill.identity().orElseThrow().logicalName())) {
            return List.of();
        }
        String resourceId = kill.identity().orElseThrow().resourceId();
        return failures.stream().filter(failure -> {
            if (failure.taskManagerId().filter(resourceId::equals).isPresent()) {
                return true;
            }
            boolean runningPeer = kill.jobBeforeKill().observation().stream()
                    .flatMap(before -> before.subtasks().stream())
                    .anyMatch(subtask -> "RUNNING".equals(subtask.status())
                            && subtask.taskManagerId().isPresent()
                            && subtask.taskManagerId().equals(failure.taskManagerId()));
            Matcher remote = CLOSED_REMOTE_TASK_MANAGER.matcher(failure.rootCause());
            return runningPeer && remote.matches() && resourceId.equals(remote.group(1));
        }).toList();
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
        if (kill.identity().isEmpty()
                || !kill.target().equals(kill.identity().orElseThrow().logicalName())) {
            return unconfirmed(kill, Outcome.EVIDENCE_UNAVAILABLE,
                    "the killed target has no matching physical and Flink resource identity");
        }
        String resourceId = kill.identity().orElseThrow().resourceId();
        List<FlinkJobObservation.Subtask> targetedSubtasks = before.subtasks().stream()
                .filter(subtask -> "RUNNING".equals(subtask.status()))
                .filter(subtask -> subtask.taskManagerId().filter(resourceId::equals).isPresent())
                .toList();
        if (targetedSubtasks.isEmpty()) {
            return unconfirmed(kill, Outcome.NO_ACTIVE_SUBTASK_BEFORE_KILL,
                    "no RUNNING subtask was observed on targeted TaskManager " + resourceId
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
        if (kill.jobManagerTimeBeforeKill().isEmpty() || kill.jobManagerTimeAfterKill().isEmpty()) {
            return unconfirmed(kill, Outcome.EVIDENCE_UNAVAILABLE,
                    "the fresh pre-injection or confirmed post-exit JobManager clock sample is missing");
        }
        long beforeKill = kill.jobManagerTimeBeforeKill().orElseThrow();
        long afterKill = kill.jobManagerTimeAfterKill().orElseThrow();
        if (beforeKill < before.jobManagerTimeMillis()
                || afterKill < beforeKill
                || after.jobManagerTimeMillis() < afterKill) {
            return unconfirmed(kill, Outcome.EVIDENCE_UNAVAILABLE,
                    "JobManager clock samples do not establish the order of the kill and recovery");
        }
        List<FlinkJobObservation.Failure> failures = after.failuresAfter(beforeKill).stream()
                .filter(failure -> !before.failures().contains(failure))
                // The observation spans REST calls. An event after its initial clock sample
                // needs a subsequent observation before it can establish completed recovery.
                .filter(failure -> failure.timestampMillis() <= after.jobManagerTimeMillis())
                .toList();
        List<FlinkJobObservation.Failure> hostFailures = targetFailures(kill, failures);
        Optional<FlinkJobObservation.Restore> restore = after.restoredCheckpoints()
                > before.restoredCheckpoints()
                ? after.latestRestore().filter(latest -> latest.restoredAtMillis() > beforeKill)
                        .filter(latest -> latest.restoredAtMillis() <= after.jobManagerTimeMillis())
                        .filter(latest -> !before.latestRestore().filter(latest::equals).isPresent())
                        // Flink timestamps have millisecond precision: equal times are ordered
                        // only to that precision, but an earlier restore cannot prove recovery.
                        .filter(latest -> hostFailures.stream().anyMatch(failure ->
                                latest.restoredAtMillis() >= failure.timestampMillis()))
                : Optional.empty();

        if (hostFailures.isEmpty()) {
            return new TaskManagerKillEffect(kill, Outcome.NO_RECOVERY_OBSERVED, restore, failures,
                    "Flink recorded " + failures.size() + " new failure(s) after the pre-injection"
                            + " clock sample, none attributable to targeted TaskManager " + resourceId);
        }
        if (restore.isPresent()) {
            FlinkJobObservation.Restore restored = restore.orElseThrow();
            return new TaskManagerKillEffect(kill, Outcome.CHECKPOINT_RESTORED, restore, failures,
                    "Flink restored checkpoint " + restored.checkpointId() + " "
                            + (restored.restoredAtMillis() - beforeKill)
                            + " ms after the fresh pre-injection clock sample");
        }
        if (before.completedCheckpoints() > 0) {
            return new TaskManagerKillEffect(kill, Outcome.NO_RECOVERY_OBSERVED, restore, failures,
                    before.completedCheckpoints() + " checkpoint(s) had completed before the kill,"
                            + " but no restore followed a matching target failure after injection began");
        }
        boolean restarted = after.state() == FlinkJobState.FINISHED
                || after.activeSubtasks().stream().anyMatch(current ->
                        targetedSubtasks.stream().anyMatch(previous ->
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
                "no checkpoint had completed before the kill; targeted TaskManager "
                        + resourceId + " failed afterwards and Flink restarted the job");
    }

    private static TaskManagerKillEffect unconfirmed(
            PhaseExecutionEvidence.TaskManagerKill kill,
            Outcome outcome,
            String detail) {
        return new TaskManagerKillEffect(kill, outcome, Optional.empty(), List.of(), detail);
    }
}
