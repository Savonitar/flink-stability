package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskManagerKillEffectTest {
    private static final FlinkJobObservation.Failure LOST_TASK_MANAGER =
            new FlinkJobObservation.Failure(
                    4_000,
                    "org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException",
                    "TaskManager with id tm-1 is no longer reachable.",
                    Optional.of("tm-1"));

    @Test
    void aHostFailureAndACheckpointRestoreAfterTheKillConfirmIt() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 2, 0))),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.CHECKPOINT_RESTORED, effect.outcome());
        assertTrue(effect.outcome().confirmed());
        assertEquals(Optional.of(new FlinkJobObservation.Restore(2, 5_000)), effect.restore());
        assertEquals(List.of(LOST_TASK_MANAGER), effect.failuresAfterKill());
        assertTrue(effect.detail().contains("checkpoint 2 3500 ms after"), effect.detail());
    }

    @Test
    void aHostFailureBeforeAnyCheckpointConfirmsTheKillWithoutARestore() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 0, 0))),
                observed(finished(9_000, 0, Optional.empty(), List.of(LOST_TASK_MANAGER))));

        assertEquals(
                TaskManagerKillEffect.Outcome.RESTARTED_WITHOUT_CHECKPOINT, effect.outcome());
        assertTrue(effect.outcome().confirmed());
        assertTrue(effect.restore().isEmpty());
    }

    @Test
    void aHostFailureWithoutRestoringTheCompletedCheckpointIsNotConfirmed() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 2, 0))),
                observed(finished(9_000, 0, Optional.empty(), List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertFalse(effect.outcome().confirmed());
        assertEquals(1, effect.failuresAfterKill().size());
    }

    @Test
    void aRestoreCausedByAFailureOnAnotherTaskManagerIsNotConfirmed() {
        FlinkJobObservation.Failure elsewhere = new FlinkJobObservation.Failure(
                4_000, "java.lang.RuntimeException", "boom", Optional.of("tm-2"));
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 2, 0))),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        List.of(elsewhere))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.detail().contains("none on a TaskManager that hosted"),
                effect.detail());
    }

    @Test
    void aKillOfAnAlreadyFinishedJobIsNotConfirmed() {
        TaskManagerKillEffect effect = only(
                kill(observed(finished(1_000, 0, Optional.empty(), List.of()))),
                observed(finished(9_000, 0, Optional.empty(), List.of())));

        assertEquals(TaskManagerKillEffect.Outcome.JOB_TERMINAL_BEFORE_KILL, effect.outcome());
        assertTrue(effect.detail().contains("already FINISHED"), effect.detail());
    }

    @Test
    void aKillWhileNoSubtaskIsActiveIsNotConfirmed() {
        // A job already in a restart loop: its subtasks were cancelled before the kill.
        FlinkJobObservation restarting = new FlinkJobObservation(
                1_000, FlinkJobState.RESTARTING, 2, 3, Optional.empty(), List.of(),
                List.of(new FlinkJobObservation.Subtask(
                        "Kafka Source", 0, 3, "CANCELED", Optional.of("tm-1"))));
        TaskManagerKillEffect effect = only(
                kill(observed(restarting)),
                observed(finished(9_000, 4,
                        Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(
                TaskManagerKillEffect.Outcome.NO_ACTIVE_SUBTASK_BEFORE_KILL, effect.outcome());
        assertTrue(effect.detail().contains("RESTARTING"), effect.detail());
    }

    @Test
    void neitherEarlierFailuresNorEarlierRestoresCountForALaterKill() {
        FlinkJobObservation.Restore earlierRestore = new FlinkJobObservation.Restore(1, 800);
        FlinkJobObservation.Failure earlierFailure = new FlinkJobObservation.Failure(
                700, "java.lang.RuntimeException", "unrelated", Optional.of("tm-1"));
        FlinkJobObservation before = new FlinkJobObservation(
                1_000, FlinkJobState.RUNNING, 2, 1, Optional.of(earlierRestore),
                List.of(earlierFailure), activeOn("tm-1"));
        TaskManagerKillEffect effect = only(
                kill(observed(before)),
                observed(finished(9_000, 1, Optional.of(earlierRestore),
                        List.of(earlierFailure))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.failuresAfterKill().isEmpty());
    }

    @Test
    void aNaturalFailoverDuringThePreKillObservationCannotConfirmTheKill() {
        // The details and checkpoint endpoints were read at 1000. A natural failure then
        // restored the job before the remaining REST calls and the actual kill completed.
        FlinkJobObservation before = new FlinkJobObservation(
                1_000, FlinkJobState.RUNNING, 2, 0, Optional.empty(),
                List.of(LOST_TASK_MANAGER), activeOn("tm-1"));
        TaskManagerKillEffect effect = only(
                kill(observed(before), OptionalLong.of(6_000)),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.failuresAfterKill().isEmpty());
        assertTrue(effect.restore().isEmpty());
    }

    @Test
    void aPreKillFailureStillDoesNotCountWhenTheRestoreFollowsTheKill() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 2, 0)), OptionalLong.of(4_500)),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.restore().isEmpty());
        assertTrue(effect.failuresAfterKill().isEmpty());
    }

    @Test
    void aPostKillRestoreBeforeTheMatchingHostFailureDoesNotProveRecovery() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 2, 0)), OptionalLong.of(1_500)),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 2_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.restore().isEmpty());
        assertEquals(List.of(LOST_TASK_MANAGER), effect.failuresAfterKill());
    }

    @Test
    void failureAndRestoreMayShareTheSameMillisecond() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 2, 0)), OptionalLong.of(1_500)),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 4_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.CHECKPOINT_RESTORED, effect.outcome());
        assertTrue(effect.outcome().confirmed());
    }

    @Test
    void aFailureWithoutACheckpointNeedsEvidenceThatExecutionRestarted() {
        for (FlinkJobState state : List.of(FlinkJobState.FAILED, FlinkJobState.RUNNING)) {
            FlinkJobObservation after = new FlinkJobObservation(
                    9_000, state, 0, 0, Optional.empty(), List.of(LOST_TASK_MANAGER),
                    state == FlinkJobState.RUNNING ? activeOn("tm-1") : List.of());
            TaskManagerKillEffect effect = only(
                    kill(observed(runningOn("tm-1", 1_000, 0, 0))), observed(after));

            assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
            assertFalse(effect.outcome().confirmed());
        }
    }

    @Test
    void aLaterActiveAttemptProvesRestartWithoutACheckpoint() {
        FlinkJobObservation after = new FlinkJobObservation(
                9_000, FlinkJobState.RUNNING, 0, 0, Optional.empty(),
                List.of(LOST_TASK_MANAGER), List.of(new FlinkJobObservation.Subtask(
                        "Kafka Source", 0, 1, "RUNNING", Optional.of("tm-2"))));
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 0, 0))), observed(after));

        assertEquals(TaskManagerKillEffect.Outcome.RESTARTED_WITHOUT_CHECKPOINT, effect.outcome());
        assertTrue(effect.outcome().confirmed());
    }

    @Test
    void aPreKillRestoreCannotCombineWithALaterFailureToConfirmTheKill() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 2, 0)), OptionalLong.of(3_000)),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 2_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.restore().isEmpty());
        assertEquals(List.of(LOST_TASK_MANAGER), effect.failuresAfterKill());
    }

    @Test
    void anEventAtThePostExitClockSampleIsTemporallyAmbiguous() {
        TaskManagerKillEffect effect = only(
                kill(observed(runningOn("tm-1", 1_000, 0, 0)), OptionalLong.of(4_000)),
                observed(finished(9_000, 0, Optional.empty(), List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.failuresAfterKill().isEmpty());
    }

    @Test
    void absentOrOutOfOrderPostExitClockEvidenceFailsClosed() {
        for (OptionalLong sample : List.of(
                OptionalLong.empty(), OptionalLong.of(500), OptionalLong.of(10_000))) {
            TaskManagerKillEffect effect = only(
                    kill(observed(runningOn("tm-1", 1_000, 2, 0)), sample),
                    observed(finished(9_000, 1,
                            Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                            List.of(LOST_TASK_MANAGER))));

            assertEquals(TaskManagerKillEffect.Outcome.EVIDENCE_UNAVAILABLE, effect.outcome());
            assertFalse(effect.outcome().confirmed());
        }
    }

    @Test
    void anAlreadyObservedFailureNeverCountsEvenIfItsTimestampIsInconsistent() {
        FlinkJobObservation before = new FlinkJobObservation(
                1_000, FlinkJobState.RUNNING, 2, 0, Optional.empty(),
                List.of(LOST_TASK_MANAGER), activeOn("tm-1"));
        TaskManagerKillEffect effect = only(
                kill(observed(before), OptionalLong.of(2_000)),
                observed(finished(9_000, 1,
                        Optional.of(new FlinkJobObservation.Restore(2, 5_000)),
                        List.of(LOST_TASK_MANAGER))));

        assertEquals(TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED, effect.outcome());
        assertTrue(effect.failuresAfterKill().isEmpty());
    }

    @Test
    void missingObservationsLeaveTheEffectUnconfirmed() {
        TaskManagerKillEffect unobservedBefore = only(
                kill(new FlinkJobObservation.Attempt(
                        Optional.empty(), Optional.of("IOException: REST unavailable"))),
                observed(finished(9_000, 1, Optional.empty(), List.of())));
        assertEquals(
                TaskManagerKillEffect.Outcome.EVIDENCE_UNAVAILABLE, unobservedBefore.outcome());
        assertTrue(unobservedBefore.detail().contains("REST unavailable"));

        List<TaskManagerKillEffect> unobservedAfter = TaskManagerKillEffect.evaluate(
                List.of(kill(observed(runningOn("tm-1", 1_000, 2, 0)))),
                Optional.empty());
        assertEquals(
                TaskManagerKillEffect.Outcome.EVIDENCE_UNAVAILABLE,
                unobservedAfter.getFirst().outcome());
    }

    @Test
    void eachKillIsJudgedAgainstTheObservationBeforeTheNextKill() {
        // The first kill recovers before the second one; the second never recovers.
        FlinkJobObservation.Restore firstRestore = new FlinkJobObservation.Restore(2, 5_000);
        FlinkJobObservation beforeSecond = new FlinkJobObservation(
                6_000, FlinkJobState.RUNNING, 4, 1, Optional.of(firstRestore),
                List.of(LOST_TASK_MANAGER), activeOn("tm-2"));

        List<TaskManagerKillEffect> effects = TaskManagerKillEffect.evaluate(
                List.of(
                        kill(observed(runningOn("tm-1", 1_000, 2, 0))),
                        kill(observed(beforeSecond))),
                Optional.of(observed(finished(9_000, 1, Optional.of(firstRestore),
                        List.of(LOST_TASK_MANAGER)))));

        assertEquals(
                List.of(
                        TaskManagerKillEffect.Outcome.CHECKPOINT_RESTORED,
                        TaskManagerKillEffect.Outcome.NO_RECOVERY_OBSERVED),
                effects.stream().map(TaskManagerKillEffect::outcome).toList());
    }

    private static TaskManagerKillEffect only(
            PhaseExecutionEvidence.TaskManagerKill kill,
            FlinkJobObservation.Attempt after) {
        List<TaskManagerKillEffect> effects =
                TaskManagerKillEffect.evaluate(List.of(kill), Optional.of(after));
        assertEquals(1, effects.size());
        return effects.getFirst();
    }

    private static PhaseExecutionEvidence.TaskManagerKill kill(
            FlinkJobObservation.Attempt before) {
        return kill(before, OptionalLong.of(before.observation()
                .map(FlinkJobObservation::jobManagerTimeMillis).orElse(1_000L) + 500));
    }

    private static PhaseExecutionEvidence.TaskManagerKill kill(
            FlinkJobObservation.Attempt before, OptionalLong afterKill) {
        return new PhaseExecutionEvidence.TaskManagerKill(
                "$/phases/1/steps/0", List.of(), "taskmanager-1", before, afterKill);
    }

    private static FlinkJobObservation.Attempt observed(FlinkJobObservation observation) {
        return new FlinkJobObservation.Attempt(Optional.of(observation), Optional.empty());
    }

    private static FlinkJobObservation runningOn(
            String taskManagerId,
            long jobManagerTimeMillis,
            long completedCheckpoints,
            long restoredCheckpoints) {
        return new FlinkJobObservation(
                jobManagerTimeMillis, FlinkJobState.RUNNING, completedCheckpoints,
                restoredCheckpoints, Optional.empty(), List.of(), activeOn(taskManagerId));
    }

    private static FlinkJobObservation finished(
            long jobManagerTimeMillis,
            long restoredCheckpoints,
            Optional<FlinkJobObservation.Restore> latestRestore,
            List<FlinkJobObservation.Failure> failures) {
        return new FlinkJobObservation(
                jobManagerTimeMillis, FlinkJobState.FINISHED, 6, restoredCheckpoints,
                latestRestore, failures, List.of());
    }

    private static List<FlinkJobObservation.Subtask> activeOn(String taskManagerId) {
        return List.of(new FlinkJobObservation.Subtask(
                "Kafka Source", 0, 0, "RUNNING", Optional.of(taskManagerId)));
    }
}
