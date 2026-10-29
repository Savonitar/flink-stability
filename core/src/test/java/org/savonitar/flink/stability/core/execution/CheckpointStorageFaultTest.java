package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.*;
import org.savonitar.flink.stability.runtime.api.*;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointStorageFaultTest {
    private static final String JOB = "a".repeat(32), NAMESPACE = "job_test", HASH = "b".repeat(64);
    private static final String PREFIX = NAMESPACE + "/" + JOB + "/chk-";
    private static final TaskManagerControl.Identity OLD = new TaskManagerControl.Identity("taskmanager-1", "old-runtime", "old-resource");
    private static final TaskManagerControl.Identity NEW = new TaskManagerControl.Identity("taskmanager-1", "new-runtime", "new-resource");
    private static final StorageFaultControl.Instance JM = instance("jobmanager-1", "jm-runtime", FlinkComponentRole.JOB_MANAGER, 11);
    private static final StorageFaultControl.Instance TM = instance("taskmanager-1", "old-runtime", FlinkComponentRole.TASK_MANAGER, 12);
    private static final StorageFaultControl.Instance REPLACEMENT = instance("taskmanager-1", "new-runtime", FlinkComponentRole.TASK_MANAGER, 13);
    @TempDir Path temporary;

    @Test void generatedCheckpointUriAcceptsEquivalentLocalFileSyntax() throws Exception {
        var original = new Fixture(Mode.HEALTHY).snapshot(false);
        for (String uri : List.of("file:/flink/checkpoints/" + NAMESPACE,
                "file:///flink/checkpoints/" + NAMESPACE)) {
            assertEquals(uri, CheckpointStorageFault.verifiedCheckpointUri(withCheckpointUri(original, uri)));
        }
    }

    @Test void generatedCheckpointUriRejectsForeignMalformedAndEncodedLocations() throws Exception {
        var original = new Fixture(Mode.HEALTHY).snapshot(false);
        for (String uri : List.of("file://foreign/flink/checkpoints/" + NAMESPACE,
                "file://localhost/flink/checkpoints/" + NAMESPACE,
                "file:/other/checkpoints/" + NAMESPACE,
                "file:/flink/checkpoints/job_foreign",
                "file:/flink/checkpoints/" + NAMESPACE + "?query=value",
                "file:/flink/checkpoints/" + NAMESPACE + "#fragment",
                "file:/flink/checkpoints/job_%74est",
                "file:/flink/checkpoints/%2e%2e/" + NAMESPACE,
                "file:/flink/checkpoints%2f" + NAMESPACE,
                "file:/flink/checkpoints/../checkpoints/" + NAMESPACE,
                "file:/flink/checkpoints/" + NAMESPACE + "/",
                "file:/flink/checkpoints/%", "not a URI", "")) {
            assertThrows(IOException.class,
                    () -> CheckpointStorageFault.verifiedCheckpointUri(withCheckpointUri(original, uri)), uri);
        }
        assertThrows(IOException.class, () -> CheckpointStorageFault.verifiedCheckpointUri(
                copy(original, false, false, original.instances())));
        var invalidNamespace = new StorageFaultControl.Snapshot(original.attemptId(), original.storageId(),
                "other_namespace", "file:/flink/checkpoints/other_namespace", original.helperManifestSha256(),
                original.instances(), original.materializedFiles(), true, false, List.of());
        assertThrows(IOException.class, () -> CheckpointStorageFault.verifiedCheckpointUri(invalidNamespace));
    }

    @Test void completeSequenceBindsFailedCheckpointNativeEioPhysicalStopAndRestoredState() throws Exception {
        var fake = new Fixture(Mode.HEALTHY);
        var evidence = fake.execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors() + " recovery=" + evidence.recovery());
        assertEquals(List.of("gate", "materialize", "arm", "trigger", "trigger-status", "kill", "heal", "restart"), fake.actions);
        assertEquals(3, evidence.checkpointId());
        assertEquals(4, evidence.failedCheckpointId());
        assertTrue(evidence.triggerResult().checkpointId().isEmpty());
        assertEquals(6, evidence.samples().size());
        assertEquals(2, evidence.transitions().size());
        assertTrue(evidence.recovery().confirmed());
        assertEquals(3, evidence.recovery().restoredCheckpointId());
        assertEquals(2, evidence.recovery().restoredKeys().size());
        assertEquals(NEW, evidence.restart().replacementIdentity().orElseThrow());
        assertTrue(CheckpointStorageFault.finalConfirmed(evidence, fake.snapshot(true)));
    }

    @Test void acceptsExactRestoreStartedByJobManagerBeforeReplacementSlotsExist() throws Exception {
        var fake = new Fixture(Mode.EARLY_RESTORE);
        var evidence = fake.execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors() + " recovery=" + evidence.recovery());
        assertEquals(1, evidence.samples().get(3).checkpoints().counts().restored());
        assertEquals(1250, evidence.samples().get(3).checkpoints().latestRestored().orElseThrow().restoreTimestamp());
        assertEquals(1, fake.count("heal"));
        assertEquals(1, fake.count("restart"));
    }

    @Test void exactDeclaredToleranceIsRequiredBeforeArmingAndInRetainedProof() throws Exception {
        var declared = new ExecutableScenarioPlan.CheckpointControls(0, 1, 3);
        var matching = new Fixture(Mode.HEALTHY);
        matching.observedTolerance = 3;
        var evidence = matching.execute(declared);
        assertTrue(evidence.confirmed(), () -> evidence.errors().toString());
        assertEquals(declared, evidence.declaredControls());
        assertTrue(CheckpointStorageFault.finalConfirmed(evidence, matching.snapshot(true)));

        var changedDeclaration = new CheckpointStorageFault.Evidence(evidence.path(), evidence.jobId(), evidence.triggerId(),
                evidence.timeoutMillis(), evidence.checkpointId(), evidence.failedCheckpointId(), evidence.configuration(),
                evidence.readiness(), evidence.materialization(), evidence.triggerResult(), evidence.failedCheckpoint(),
                evidence.failureTrace(), evidence.recoveryTrace(), evidence.samples(), evidence.transitions(), evidence.kill(),
                evidence.restart(), evidence.recovery(), evidence.errors(), new ExecutableScenarioPlan.CheckpointControls(0, 1, 10));
        assertFalse(changedDeclaration.confirmed());
        assertFalse(CheckpointStorageFault.finalConfirmed(changedDeclaration, matching.snapshot(true)));

        for (int observed : List.of(1, 2, 4, 10)) {
            var mismatched = new Fixture(Mode.HEALTHY);
            mismatched.observedTolerance = observed;
            var rejected = mismatched.execute(declared);
            assertFalse(rejected.confirmed(), "Observed tolerance " + observed);
            assertEquals(List.of("gate"), mismatched.actions, "No storage arming or physical fault before exact config proof");
            assertTrue(rejected.errors().stream().anyMatch(value -> value.contains("checkpoint configuration")));
            assertEquals(declared, rejected.declaredControls());
        }
    }

    @Test void postKillRpcFailuresAreAccountedBeforeHealingAndReplacement() throws Exception {
        var fake = new Fixture(Mode.POST_KILL_FAILURES);
        var evidence = fake.execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors().toString());
        assertEquals(2, evidence.samples().get(3).checkpoints().counts().failed());
        assertEquals(3, evidence.samples().get(4).checkpoints().counts().failed());
        assertEquals(3, evidence.samples().get(4).checkpoints().latestCompleted().orElseThrow().id());
        assertEquals(7, evidence.samples().get(5).checkpoints().latestCompleted().orElseThrow().id());
        assertEquals(1, fake.count("kill"));
        assertEquals(1, fake.count("heal"));
        assertEquals(1, fake.count("restart"));
        assertTrue(CheckpointStorageFault.finalConfirmed(evidence, fake.snapshot(true)));
    }

    @Test void postKillPendingHistoryMustSettleWithinOriginalDeadlineBeforeHealing() throws Exception {
        var delayed = new Fixture(Mode.DELAYED_POST_KILL_HISTORY);
        var complete = delayed.execute();
        assertTrue(complete.confirmed(), () -> complete.errors().toString());
        assertTrue(delayed.postKillHistoryReads >= 4);
        assertEquals(0, complete.samples().get(3).checkpoints().counts().inProgress());
        assertTrue(complete.samples().get(3).elapsedNanos() >= Duration.ofMillis(200).toNanos());
        assertEquals(1, delayed.count("kill"));
        assertEquals(1, delayed.count("heal"));
        assertEquals(1, delayed.count("restart"));

        var pending = new Fixture(Mode.STALE_POST_KILL_HISTORY);
        var incomplete = pending.execute();
        assertFalse(incomplete.confirmed());
        assertTrue(pending.postKillHistoryReads > 2);
        assertEquals(1, pending.count("kill"));
        assertEquals(0, pending.count("heal"));
        assertEquals(0, pending.count("restart"));
        assertEquals(1, pending.count("abort"));
        assertTrue(incomplete.errors().stream().anyMatch(error -> error.contains("deadline")));
    }

    @Test void postKillBoundaryRejectsUnaccountedPredatingOrNonfailedCheckpoints() {
        var before = history(2);
        var failed = postKillCheckpoint(5, 1150);
        var valid = postKillHistory(List.of(failed));
        assertTrue(CheckpointStorageFault.afterKillBoundary(before, valid, 3, 1100));
        var pending = new FlinkCheckpointEvidence.Checkpoint(5, FlinkCheckpointEvidence.Status.IN_PROGRESS,
                false, "CHECKPOINT", 1150, -1, 0, 0, 1, 0, OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        var savepoint = new FlinkCheckpointEvidence.Checkpoint(5, FlinkCheckpointEvidence.Status.FAILED,
                true, "SAVEPOINT", 1150, -1, 0, 0, 1, 0, OptionalLong.of(1175), Optional.of("RPC failure"), Optional.empty(), Optional.empty());
        for (var invalid : List.of(
                postKillHistory(List.of(postKillCheckpoint(5, 1099))),
                postKillHistory(List.of(checkpoint(5, false))),
                postKillHistory(List.of(pending)),
                postKillHistory(List.of(savepoint)),
                new FlinkCheckpointEvidence.History(JOB, valid.counts(), before.retained(), before.latestCompleted(),
                        valid.latestFailed(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, valid.counts(), List.of(checkpoint(3, false), failed),
                        before.latestCompleted(), valid.latestFailed(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(4, 0, 1, 3, 0),
                        valid.retained(), before.latestCompleted(), valid.latestFailed(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 1, 1, 1, 0),
                        List.of(checkpoint(3, false), checkpoint(4, true), pending), before.latestCompleted(),
                        before.latestFailed(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, valid.counts(), valid.retained(), before.latestCompleted(),
                        before.latestFailed(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 0, 1, 2, 1),
                        valid.retained(), before.latestCompleted(), valid.latestFailed(), Optional.empty(),
                        Optional.of(new FlinkCheckpointEvidence.Restored(5, 1300, false, Optional.empty()))))) {
            assertFalse(CheckpointStorageFault.afterKillBoundary(before, invalid, 3, 1100));
        }
    }

    @Test void postKillHistoryCannotRegressOrSubstituteFailuresAcrossHealing() {
        var first = postKillHistory(List.of(postKillCheckpoint(5, 1150)));
        var second = postKillHistory(List.of(postKillCheckpoint(5, 1150), postKillCheckpoint(6, 1200)));
        assertTrue(CheckpointStorageFault.monotonicRestore(first, second));
        assertFalse(CheckpointStorageFault.monotonicRestore(second, first));
        assertFalse(CheckpointStorageFault.monotonicRestore(first,
                postKillHistory(List.of(postKillCheckpoint(6, 1200)))));
        var restored = new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 0, 1, 2, 1),
                first.retained(), first.latestCompleted(), first.latestFailed(), Optional.empty(),
                Optional.of(new FlinkCheckpointEvidence.Restored(3, 1250, false, Optional.empty())));
        assertTrue(CheckpointStorageFault.afterKillBoundary(history(2), restored, 3, 1100));
        assertFalse(CheckpointStorageFault.monotonicRestore(restored, second));
    }

    @Test void unknownPostOutcomeIsNeverRetriedAndNeverKills() throws Exception {
        var fake = new Fixture(Mode.UNKNOWN_POST);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertEquals(1, fake.count("trigger"));
        assertEquals(0, fake.count("trigger-status"));
        assertEquals(0, fake.count("kill"));
        assertEquals(0, fake.count("heal"));
        assertEquals(0, fake.count("restart"));
        assertEquals(1, fake.count("abort"));
        assertTrue(evidence.errors().stream().anyMatch(error -> error.contains("lost POST response")));
    }

    @Test void checkpointCompletingDuringArmInvalidatesTheBoundaryBeforeTrigger() throws Exception {
        var fake = new Fixture(Mode.ARM_RACE);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertEquals(1, fake.count("arm"));
        assertEquals(0, fake.count("trigger"));
        assertEquals(0, fake.count("kill"));
        assertEquals(1, fake.count("abort"));
    }

    @Test void delayedFailedSnapshotReceiptRefreshesTheWholeWindowBeforeKill() throws Exception {
        var fake = new Fixture(Mode.DELAYED_FAILED_MARKER);
        var evidence = fake.execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors().toString());
        assertEquals(3, fake.failureLogPolls);
        assertEquals(3, fake.failureHistoryReads);
        assertEquals(1, fake.count("trigger"));
        assertEquals(1, fake.count("kill"));
        assertTrue(evidence.samples().get(2).elapsedNanos() >= Duration.ofMillis(200).toNanos());
        assertTrue(evidence.failureTrace().markers().stream().anyMatch(marker ->
                marker.event() == StateRecoveryEvidence.Event.SNAPSHOT && marker.checkpointId() == 4));
    }

    @Test void completedTriggerWaitsForCachedHistoryToExposeTheFailedCheckpoint() throws Exception {
        var fake = new Fixture(Mode.DELAYED_FAILED_HISTORY);
        var evidence = fake.execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors().toString());
        assertTrue(fake.failureHistoryReads >= 4);
        assertEquals(1, fake.count("trigger"));
        assertEquals(1, fake.count("kill"));
        assertEquals(4, evidence.failedCheckpointId());
        assertEquals(6, evidence.samples().size());
        assertTrue(evidence.samples().get(2).elapsedNanos() >= Duration.ofMillis(200).toNanos());
    }

    @Test void permanentlyCachedHistoryExhaustsSharedDeadlineWithoutAnotherTriggerOrKill() throws Exception {
        var fake = new Fixture(Mode.STALE_FAILED_HISTORY);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertTrue(fake.failureHistoryReads > 2);
        assertEquals(1, fake.count("trigger"));
        assertEquals(0, fake.count("kill"));
        assertEquals(0, fake.count("heal"));
        assertEquals(0, fake.count("restart"));
        assertEquals(1, fake.count("abort"));
        assertTrue(evidence.errors().stream().anyMatch(error -> error.contains("deadline")));
    }

    @Test void cachedHistoryRevealingMultipleFailedCandidatesStillRejectsAmbiguousWindow() throws Exception {
        var fake = new Fixture(Mode.DELAYED_COMPETING_HISTORY);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertTrue(fake.failureHistoryReads >= 3);
        assertEquals(1, fake.count("trigger"));
        assertEquals(0, fake.count("kill"));
        assertEquals(0, fake.count("heal"));
        assertEquals(1, fake.count("abort"));
        assertTrue(evidence.errors().stream().anyMatch(error -> error.contains("Ambiguous failed checkpoint window")));
    }

    @Test void permanentlyMissingFailedSnapshotExhaustsSharedDeadlineAndAbortsWithoutKill() throws Exception {
        var fake = new Fixture(Mode.MISSING_FAILED_MARKER);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertTrue(fake.failureLogPolls > 1);
        assertEquals(2, fake.failureHistoryReads);
        assertEquals(1, fake.count("trigger"));
        assertEquals(0, fake.count("kill"));
        assertEquals(0, fake.count("heal"));
        assertEquals(1, fake.count("abort"));
        assertTrue(evidence.errors().stream().anyMatch(error -> error.contains("deadline")));
    }

    @Test void checkpointChangeWhileWaitingForReceiptInvalidatesTheRefreshedWindow() throws Exception {
        var fake = new Fixture(Mode.CHECKPOINT_CHANGE_DURING_MARKER);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertEquals(3, fake.failureLogPolls);
        assertEquals(3, fake.failureHistoryReads);
        assertEquals(1, fake.count("trigger"));
        assertEquals(0, fake.count("kill"));
        assertEquals(0, fake.count("heal"));
        assertEquals(1, fake.count("abort"));
        assertTrue(evidence.errors().stream().anyMatch(error -> error.contains("Checkpoint changed while awaiting")));
    }

    @Test void competingCheckpointMissingHistoryMissingEioAndBrokenCaptureNeverReachKill() throws Exception {
        for (Mode mode : List.of(Mode.COMPETING_CHECKPOINT, Mode.MISSING_HISTORY, Mode.MISSING_EIO,
                Mode.FOREIGN_EIO_PATH, Mode.INCOMPLETE_JOURNAL, Mode.LEASE_EXPIRED, Mode.DAEMON_DEAD)) {
            var fake = new Fixture(mode);
            var evidence = fake.execute();
            assertFalse(evidence.confirmed(), mode.name());
            assertEquals(0, fake.count("kill"), mode.name());
            assertEquals(0, fake.count("heal"), mode.name());
            assertEquals(1, fake.count("abort"), mode.name());
            assertFalse(evidence.errors().isEmpty(), mode.name());
        }
    }

    @Test void failedMaterializationNeverArmsAndDoesNotUseSyntheticProbeAsProof() throws Exception {
        var fake = new Fixture(Mode.NO_MATERIALIZATION);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertEquals(List.of("gate", "materialize"), fake.actions);
        assertTrue(evidence.errors().stream().anyMatch(error -> error.contains("state")));
    }

    @Test void healingRequiresBothPhysicalDeathAndNoActiveTasksAndNoLaterCheckpoint() throws Exception {
        for (Mode mode : List.of(Mode.PROCESS_STILL_ALIVE, Mode.ACTIVE_TASKS, Mode.LATE_COMPLETION)) {
            var fake = new Fixture(mode);
            var evidence = fake.execute();
            assertFalse(evidence.confirmed(), mode.name());
            assertEquals(1, fake.count("kill"), mode.name());
            assertEquals(0, fake.count("heal"), mode.name());
            assertEquals(0, fake.count("restart"), mode.name());
            assertEquals(1, fake.count("abort"), mode.name());
        }
    }

    @Test void sharedDeadlineIncludesReadinessAndFailedTriggerPolling() throws Exception {
        for (Mode mode : List.of(Mode.GATE_DEADLINE, Mode.TRIGGER_DEADLINE)) {
            var fake = new Fixture(mode);
            var evidence = fake.execute();
            assertFalse(evidence.confirmed());
            assertEquals(0, fake.count("kill"));
            assertTrue(evidence.errors().stream().anyMatch(error -> error.contains("deadline")));
            assertEquals(mode == Mode.GATE_DEADLINE ? 0 : 1, fake.count("abort"));
        }
    }

    @Test void lostArmAcknowledgementStillAttemptsAbortWithoutTriggerOrKill() throws Exception {
        var fake = new Fixture(Mode.LOST_ARM_ACK);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertEquals(1, fake.count("arm"));
        assertEquals(1, fake.count("abort"));
        assertEquals(0, fake.count("trigger"));
        assertEquals(0, fake.count("kill"));
    }

    @Test void armAcknowledgementMustPointToItsExactJournalReceiptBeforeTriggering() throws Exception {
        for (Mode mode : List.of(Mode.WRONG_ARM_ACK_SEQUENCE, Mode.MISSING_ARM_RECEIPT)) {
            var fake = new Fixture(mode);
            var evidence = fake.execute();
            assertFalse(evidence.confirmed(), mode.name());
            assertEquals(1, fake.count("arm"));
            assertEquals(0, fake.count("trigger"), mode.name());
            assertEquals(0, fake.count("kill"), mode.name());
            assertEquals(1, fake.count("abort"));
        }
    }

    @Test void nativeEioMustFollowTheAcknowledgedArmSequence() throws Exception {
        var fake = new Fixture(Mode.EIO_BEFORE_ARM);
        var evidence = fake.execute();
        assertFalse(evidence.confirmed());
        assertEquals(1, fake.count("trigger"));
        assertEquals(0, fake.count("kill"));
        assertEquals(1, fake.count("abort"));
    }

    @Test void healAcknowledgementMustPointToItsExactJournalReceiptBeforeReplacement() throws Exception {
        for (Mode mode : List.of(Mode.WRONG_HEAL_ACK_SEQUENCE, Mode.MISSING_HEAL_RECEIPT)) {
            var fake = new Fixture(mode);
            var evidence = fake.execute();
            assertFalse(evidence.confirmed(), mode.name());
            assertEquals(1, fake.count("kill"));
            assertEquals(1, fake.count("heal"));
            assertEquals(0, fake.count("restart"), mode.name());
            assertEquals(1, fake.count("abort"));
        }
    }

    @Test void replacementIdentityAndAllKeySuffixAreRequiredAfterHealing() throws Exception {
        for (Mode mode : List.of(Mode.SAME_REPLACEMENT, Mode.MISSING_SUFFIX)) {
            var fake = new Fixture(mode);
            var evidence = fake.execute();
            assertFalse(evidence.confirmed());
            assertEquals(1, fake.count("heal"));
            assertEquals(1, fake.count("restart"));
            assertEquals(0, fake.count("abort"));
        }
    }

    @Test void finalCaptureRequiresCompleteQuiescentJournalsAndEveryEarlierReceipt() throws Exception {
        var fake = new Fixture(Mode.HEALTHY);
        var evidence = fake.execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors().toString());
        var complete = fake.snapshot(true);
        assertTrue(CheckpointStorageFault.finalConfirmed(evidence, complete));
        assertFalse(CheckpointStorageFault.finalConfirmed(evidence, copy(complete, false, true, complete.instances())));
        assertFalse(CheckpointStorageFault.finalConfirmed(evidence, copy(complete, true, false, complete.instances())));
        assertFalse(CheckpointStorageFault.finalConfirmed(evidence, new StorageFaultControl.Snapshot(
                complete.attemptId(), complete.storageId(), "foreign-namespace", "file:/flink/checkpoints/foreign-namespace",
                complete.helperManifestSha256(), complete.instances(), complete.materializedFiles(), true, true, List.of())));
        var lost = complete.instances().stream().map(instance -> instance.instance().equals(TM)
                ? new StorageFaultControl.InstanceEvidence(instance.instance(), instance.verification(), instance.status(),
                        instance.processDead(), instance.journalComplete(), instance.journalPath(), instance.journalSha256(),
                        instance.journalBytes(), instance.lastSequence(), instance.operations().stream().filter(op -> op.errno() != 5).toList(),
                        instance.commands(), instance.diagnostics()) : instance).toList();
        assertFalse(CheckpointStorageFault.finalConfirmed(evidence, copy(complete, true, true, lost)));
        for (var state : List.of(StorageFaultControl.State.EXPIRED, StorageFaultControl.State.ABORTED, StorageFaultControl.State.FATAL)) {
            var failed = complete.instances().stream().map(instance -> instance.instance().equals(JM)
                    ? new StorageFaultControl.InstanceEvidence(instance.instance(), instance.verification(), status(state, 2, evidence.triggerId()),
                            true, true, instance.journalPath(), instance.journalSha256(), instance.journalBytes(), instance.lastSequence(),
                            instance.operations(), instance.commands(), List.of()) : instance).toList();
            assertFalse(CheckpointStorageFault.finalConfirmed(evidence, copy(complete, true, true, failed)), state.name());
        }
        assertFalse(CheckpointStorageFault.healthyFinalCapture(fake.snapshot(false)));
    }

    @Test void finalCaptureExceptionRetainsUnavailableEvidence() {
        V1AttemptRuntime runtime = proxy(V1AttemptRuntime.class, (method, arguments) -> {
            if (method.equals("storageSnapshot")) throw new IllegalStateException("journal capture failed");
            throw new AssertionError(method);
        });
        var snapshot = CheckpointStorageFault.finalCapture(runtime);
        assertFalse(snapshot.complete());
        assertFalse(snapshot.quiescent());
        assertTrue(snapshot.diagnostics().getFirst().contains("journal capture failed"));
        assertFalse(CheckpointStorageFault.healthyFinalCapture(snapshot));
    }

    @Test void publicEvidenceCannotForgeRecoveryOrBorrowAnotherReplacementProcess() throws Exception {
        var evidence = new Fixture(Mode.HEALTHY).execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors().toString());
        var forged = new StateRecoveryEvidence.Recovery(true, "state-recovery.confirmed", evidence.readiness(), 3,
                Optional.empty(), Optional.empty(), List.of(), List.of());
        assertFalse(withRecovery(evidence, evidence.recoveryTrace(), forged).confirmed());

        var foreign = new StateRecoveryEvidence.ProcessIdentity("taskmanager-1", "foreign-runtime", "taskmanager-1#foreign-runtime");
        var trace = new StateRecoveryEvidence.Trace(evidence.recoveryTrace().markers().stream().map(marker ->
                marker.process().runtimeId().equals(NEW.runtimeId())
                        ? copyMarker(marker, foreign, marker.operatorUid(), marker.lineNumber()) : marker).toList(), List.of(), true);
        var effect = TaskManagerKillEffect.evaluate(List.of(evidence.kill()), Optional.of(new FlinkJobObservation.Attempt(
                Optional.of(evidence.samples().getLast().job()), Optional.empty()))).getFirst();
        var borrowed = StateRecoveryEvidence.evaluate(evidence.readiness(), effect, trace);
        assertTrue(borrowed.confirmed(), borrowed.reason());
        assertFalse(withRecovery(evidence, trace, borrowed).confirmed());
        var wrongOperator = new StateRecoveryEvidence.Trace(evidence.recoveryTrace().markers().stream().map(marker ->
                marker.process().runtimeId().equals(NEW.runtimeId())
                        ? copyMarker(marker, marker.process(), "foreign-operator", marker.lineNumber()) : marker).toList(), List.of(), true);
        var wrongOperatorRecovery = StateRecoveryEvidence.evaluate(evidence.readiness(), effect, wrongOperator);
        assertTrue(wrongOperatorRecovery.confirmed(), wrongOperatorRecovery.reason());
        assertFalse(withRecovery(evidence, wrongOperator, wrongOperatorRecovery).confirmed());
    }

    @Test void finalProofRequiresLaterSnapshotForTheSameOperatorAfterAllKeyProgress() throws Exception {
        var evidence = new Fixture(Mode.HEALTHY).execute();
        assertTrue(evidence.confirmed(), () -> evidence.errors().toString());
        var withoutSnapshot = new StateRecoveryEvidence.Trace(evidence.recoveryTrace().markers().stream()
                .filter(marker -> marker.checkpointId() != 5 || marker.event() != StateRecoveryEvidence.Event.SNAPSHOT).toList(), List.of(), true);
        assertFalse(withRecovery(evidence, withoutSnapshot, evidence.recovery()).confirmed());
        for (boolean wrongUid : List.of(true, false)) {
            var trace = new StateRecoveryEvidence.Trace(evidence.recoveryTrace().markers().stream().map(marker ->
                    marker.checkpointId() == 5 && marker.event() == StateRecoveryEvidence.Event.SNAPSHOT
                            ? copyMarker(marker, marker.process(), wrongUid ? "foreign-operator" : marker.operatorUid(),
                                    wrongUid ? marker.lineNumber() : 2) : marker).toList(), List.of(), true);
            assertFalse(withRecovery(evidence, trace, evidence.recovery()).confirmed(), wrongUid ? "wrong UID" : "snapshot before progress");
        }
    }

    @Test void correlatesFailureWithoutInventingCheckpointIdFromTrigger() throws Exception {
        assertEquals(4, CheckpointStorageFault.correlate(history(0), history(2), failedTrigger()).id());
        assertThrows(IOException.class, () -> CheckpointStorageFault.correlate(history(0), history(2),
                new FlinkCheckpointTrigger.Observation(FlinkCheckpointTrigger.State.COMPLETED, OptionalLong.of(4), Optional.empty())));
        assertThrows(IOException.class, () -> CheckpointStorageFault.correlate(history(0), history(2),
                new FlinkCheckpointTrigger.Observation(FlinkCheckpointTrigger.State.IN_PROGRESS, OptionalLong.empty(), Optional.empty())));
    }

    @Test void historyWaitAcceptsOnlyUnchangedBoundaryOrOneNewPendingCheckpoint() {
        var before = history(0);
        var pending = new FlinkCheckpointEvidence.Checkpoint(4, FlinkCheckpointEvidence.Status.IN_PROGRESS,
                false, "CHECKPOINT", 1050, -1, 0, 0, 1, 0,
                OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        var single = new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(2, 1, 1, 0, 0),
                List.of(checkpoint(3, false), pending), before.latestCompleted(), Optional.empty(), Optional.empty(), Optional.empty());
        assertTrue(CheckpointStorageFault.pendingFailureHistory(before, before));
        assertTrue(CheckpointStorageFault.pendingFailureHistory(before, single));
        var competitor = new FlinkCheckpointEvidence.Checkpoint(5, FlinkCheckpointEvidence.Status.IN_PROGRESS,
                false, "CHECKPOINT", 1060, -1, 0, 0, 1, 0,
                OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        for (var changed : List.of(
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 2, 1, 0, 0),
                        List.of(checkpoint(3, false), pending, competitor), before.latestCompleted(), Optional.empty(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(2, 0, 2, 0, 0),
                        List.of(checkpoint(3, false), checkpoint(4, false)), Optional.of(checkpoint(4, false)),
                        Optional.empty(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(2, 1, 1, 0, 1),
                        single.retained(), single.latestCompleted(), Optional.empty(), Optional.empty(),
                        Optional.of(new FlinkCheckpointEvidence.Restored(3, 1100, false, Optional.empty()))),
                history(2))) {
            assertFalse(CheckpointStorageFault.pendingFailureHistory(before, changed));
        }
    }

    @Test void correlationRejectsTwoCandidatesCounterChangesAndAbsentRetainedFailure() {
        var before = history(0); var after = history(2);
        for (var invalid : List.of(
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 0, 1, 2, 0),
                        List.of(checkpoint(3, false), checkpoint(4, true), checkpoint(5, true)), after.latestCompleted(),
                        Optional.of(checkpoint(5, true)), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, after.counts(), List.of(checkpoint(3, false)),
                        after.latestCompleted(), after.latestFailed(), Optional.empty(), Optional.empty()),
                new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(2, 0, 1, 1, 1),
                        after.retained(), after.latestCompleted(), after.latestFailed(), Optional.empty(),
                        Optional.of(new FlinkCheckpointEvidence.Restored(3, 1300, false, Optional.empty()))))) {
            assertThrows(IOException.class, () -> CheckpointStorageFault.correlate(before, invalid, failedTrigger()));
        }
    }

    private enum Mode { HEALTHY, EARLY_RESTORE, POST_KILL_FAILURES, DELAYED_POST_KILL_HISTORY, STALE_POST_KILL_HISTORY,
        UNKNOWN_POST, ARM_RACE, COMPETING_CHECKPOINT, MISSING_HISTORY, MISSING_EIO, FOREIGN_EIO_PATH,
        INCOMPLETE_JOURNAL, LEASE_EXPIRED, DAEMON_DEAD, NO_MATERIALIZATION, PROCESS_STILL_ALIVE, ACTIVE_TASKS,
        LATE_COMPLETION, GATE_DEADLINE, TRIGGER_DEADLINE, LOST_ARM_ACK, SAME_REPLACEMENT, MISSING_SUFFIX,
        WRONG_ARM_ACK_SEQUENCE, MISSING_ARM_RECEIPT, WRONG_HEAL_ACK_SEQUENCE, MISSING_HEAL_RECEIPT, EIO_BEFORE_ARM,
        DELAYED_FAILED_MARKER, MISSING_FAILED_MARKER, CHECKPOINT_CHANGE_DURING_MARKER,
        DELAYED_FAILED_HISTORY, STALE_FAILED_HISTORY, DELAYED_COMPETING_HISTORY }

    private final class Fixture {
        final Mode mode;
        final AtomicLong now = new AtomicLong();
        final List<String> actions = new ArrayList<>();
        final StateRecoveryEvidence.Readiness readiness;
        final Path oldLog, newLog;
        final FlinkScenarioControl flink;
        final V1AttemptRuntime runtime;
        int phase, failureLogPolls, failureHistoryReads, postKillHistoryReads;
        int observedTolerance = 10;
        String faultId = "";
        StorageFaultControl.Transition arm, heal;

        Fixture(Mode mode) throws Exception {
            this.mode = mode;
            oldLog = Files.createTempFile(temporary, "old-", ".log");
            newLog = Files.createTempFile(temporary, "new-", ".log");
            Files.writeString(oldLog, delayedFailureLog() ? preText() : failedLogText());
            Files.writeString(newLog, postKillFailures() ? postText(false).replace("checkpoint=5 ", "checkpoint=7 ")
                    : postText(mode == Mode.MISSING_SUFFIX));
            var beforeProcess = new StateRecoveryEvidence.ProcessIdentity(TM.logicalName(), TM.runtimeId(), TM.process());
            readiness = StateRecoveryEvidence.ready(JOB, 2, 20, OLD, observation(0),
                    new ObjectMapper().readTree("{\"counts\":{\"in_progress\":0},\"latest\":{\"completed\":{\"id\":3,\"status\":\"COMPLETED\",\"latest_ack_timestamp\":900}}}"),
                    StateRecoveryEvidence.parse(beforeProcess, preText(), JOB),
                    new StateRecoveryEvidence.CommittedPrefix(true, true, 8, 7, Map.of(0, 4L, 1, 4L), Map.of(0, 0L), Map.of(0, 10L)));
            assertTrue(readiness.ready(), readiness.reason());
            flink = proxy(FlinkScenarioControl.class, this::flinkCall);
            runtime = proxy(V1AttemptRuntime.class, this::runtimeCall);
        }

        CheckpointStorageFault.Evidence execute() {
            return execute(new ExecutableScenarioPlan.CheckpointControls(0, 1, 10));
        }

        CheckpointStorageFault.Evidence execute(ExecutableScenarioPlan.CheckpointControls declaredControls) {
            var runner = new CheckpointStorageFault(flink, runtime, (job, target, budget) -> {
                actions.add("gate"); assertEquals("taskmanager-1", target); assertEquals(JOB, job.jobId());
                if (mode == Mode.GATE_DEADLINE) now.set(Duration.ofSeconds(3).toNanos());
                return Optional.of(readiness);
            }, now::get, duration -> now.addAndGet(duration.toNanos()));
            return runner.execute("$/phases/0/steps/1", List.of(), new FlinkJobHandle(JOB),
                    new ExecutableScenarioPlan.CheckpointStorageFault("state-job", Duration.ofSeconds(2), Duration.ofSeconds(10)), declaredControls);
        }

        private Object flinkCall(String method, Object[] arguments) throws Exception {
            now.addAndGet(1_000_000);
            return switch (method) {
                case "latestCallEvidence" -> List.of();
                case "checkpointConfiguration" -> new FlinkCheckpointEvidence.Configuration(JOB, "exactly_once", 10000, 60000,
                        0, 1, observedTolerance, "HashMapStateBackend", "FileSystemCheckpointStorage", false, false, false, false);
                case "checkpointHistory" -> observedHistory();
                case "checkpointDetails" -> checkpoint(4, true);
                case "observe" -> observedJob();
                case "jobManagerTimeMillis" -> phase < 3 ? 1100L : 1200L;
                case "triggerCheckpoint" -> {
                    actions.add("trigger"); assertEquals(faultId, arguments[1]);
                    if (mode == Mode.UNKNOWN_POST) throw new IOException("lost POST response");
                    yield arguments[1];
                }
                case "checkpointStatus" -> {
                    actions.add("trigger-status"); assertEquals(faultId, arguments[1]); phase = 2;
                    if (mode == Mode.TRIGGER_DEADLINE) { now.set(Duration.ofSeconds(3).toNanos());
                        yield new FlinkCheckpointTrigger.Observation(FlinkCheckpointTrigger.State.IN_PROGRESS, OptionalLong.empty(), Optional.empty()); }
                    yield failedTrigger();
                }
                default -> throw new AssertionError("Unexpected Flink call: " + method);
            };
        }

        private Object runtimeCall(String method, Object[] arguments) throws IOException {
            now.addAndGet(1_000_000);
            return switch (method) {
                case "inspectCheckpoint" -> {
                    actions.add("materialize"); assertEquals(JOB, arguments[0]); assertEquals(3L, arguments[1]);
                    yield new StorageFaultControl.Materialization(JM, JOB, 3, StateRecoveryEvidence.OPERATOR_UID,
                            file("/flink/checkpoints/" + PREFIX + "3/_metadata"),
                            mode == Mode.NO_MATERIALIZATION ? List.of() : List.of(file("/flink/checkpoints/" + PREFIX + "3/state.bin")), true, List.of());
                }
                case "storageSnapshot" -> snapshot(false);
                case "armStorage" -> {
                    actions.add("arm"); faultId = (String) arguments[0]; phase = 1; arm = transition(false);
                    if (mode == Mode.LOST_ARM_ACK) throw new IllegalStateException("arm acknowledgement lost");
                    yield arm;
                }
                case "killTaskManager" -> { actions.add("kill"); assertEquals("taskmanager-1", arguments[0]); phase = 3; yield null; }
                case "healStorage" -> {
                    assertEquals(3, phase, "Healing must follow physical stop");
                    assertNotEquals(Mode.PROCESS_STILL_ALIVE, mode); assertNotEquals(Mode.ACTIVE_TASKS, mode);
                    actions.add("heal"); heal = transition(true); phase = 4; yield heal;
                }
                case "abortStorage" -> { actions.add("abort"); yield new StorageFaultControl.Transition(faultId, 2, 1, 2, List.of(), List.of(), false, List.of("aborted")); }
                case "restartTaskManager" -> { assertEquals(4, phase); actions.add("restart"); phase = 5; yield null; }
                case "taskManagerIdentity" -> Optional.of(mode == Mode.SAME_REPLACEMENT ? OLD : NEW);
                case "flinkProvisioningEvidence" -> phase >= 5 ? List.of(provisioning(TM), provisioning(REPLACEMENT)) : List.of(provisioning(TM));
                case "flinkComponentLogs" -> {
                    if (phase == 2) {
                        failureLogPolls++;
                        if (delayedFailureLog() && mode != Mode.MISSING_FAILED_MARKER && failureLogPolls == 3)
                            Files.writeString(oldLog, failedLogText());
                    }
                    yield phase >= 5 ? List.of(log(TM, oldLog), log(REPLACEMENT, newLog)) : List.of(log(TM, oldLog));
                }
                default -> throw new AssertionError("Unexpected runtime call: " + method);
            };
        }

        private FlinkJobObservation observedJob() {
            var original = observation(phase == 3 && mode == Mode.ACTIVE_TASKS ? 2 : phase);
            if (mode == Mode.EARLY_RESTORE && phase >= 3)
                return new FlinkJobObservation(original.jobManagerTimeMillis(), original.state(), original.completedCheckpoints(),
                        1, Optional.of(new FlinkJobObservation.Restore(3, 1250)), original.failures(), original.subtasks());
            return original;
        }

        private FlinkCheckpointEvidence.History observedHistory() {
            if (phase == 2) failureHistoryReads++;
            if (phase == 3) postKillHistoryReads++;
            var ordinary = history(phase);
            if (phase == 3 && (mode == Mode.STALE_POST_KILL_HISTORY
                    || mode == Mode.DELAYED_POST_KILL_HISTORY && postKillHistoryReads < 3)) {
                var pending = new FlinkCheckpointEvidence.Checkpoint(5, FlinkCheckpointEvidence.Status.IN_PROGRESS,
                        false, "CHECKPOINT", 1150, -1, 0, 0, 1, 0, OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty());
                return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 1, 1, 1, 0),
                        List.of(checkpoint(3, false), checkpoint(4, true), pending), ordinary.latestCompleted(),
                        ordinary.latestFailed(), Optional.empty(), Optional.empty());
            }
            if (postKillFailures() && phase >= 3) {
                var extra = postKillHistory(phase == 3 ? List.of(postKillCheckpoint(5, 1150))
                        : List.of(postKillCheckpoint(5, 1150), postKillCheckpoint(6, 1200)));
                if (phase < 5) return extra;
                var retained = new ArrayList<>(extra.retained());
                retained.add(checkpoint(7, false));
                return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(5, 0, 2, 3, 1),
                        retained, Optional.of(checkpoint(7, false)), extra.latestFailed(), Optional.empty(),
                        Optional.of(new FlinkCheckpointEvidence.Restored(3, 1300, false, Optional.empty())));
            }
            if (phase == 2 && (mode == Mode.STALE_FAILED_HISTORY
                    || (mode == Mode.DELAYED_FAILED_HISTORY || mode == Mode.DELAYED_COMPETING_HISTORY)
                        && failureHistoryReads < 3)) return history(1);
            if (phase == 2 && mode == Mode.CHECKPOINT_CHANGE_DURING_MARKER && failureLogPolls >= 3)
                return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 0, 2, 1, 0),
                        List.of(checkpoint(3, false), checkpoint(4, true), checkpoint(5, false)), Optional.of(checkpoint(5, false)),
                        Optional.of(checkpoint(4, true)), Optional.empty(), Optional.empty());
            if (phase == 1 && mode == Mode.ARM_RACE)
                return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(2, 0, 2, 0, 0),
                        List.of(checkpoint(3, false), checkpoint(4, false)), Optional.of(checkpoint(4, false)),
                        Optional.empty(), Optional.empty(), Optional.empty());
            if (mode == Mode.EARLY_RESTORE && phase >= 3)
                return new FlinkCheckpointEvidence.History(JOB,
                        new FlinkCheckpointEvidence.Counts(ordinary.counts().total(), 0, ordinary.counts().completed(), 1, 1),
                        ordinary.retained(), ordinary.latestCompleted(), ordinary.latestFailed(), Optional.empty(),
                        Optional.of(new FlinkCheckpointEvidence.Restored(3, 1250, false, Optional.empty())));
            if (phase == 2 && (mode == Mode.COMPETING_CHECKPOINT || mode == Mode.DELAYED_COMPETING_HISTORY))
                return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 0, 1, 2, 0),
                        List.of(checkpoint(3, false), checkpoint(4, true), checkpoint(5, true)), ordinary.latestCompleted(),
                        Optional.of(checkpoint(5, true)), Optional.empty(), Optional.empty());
            if (phase == 2 && mode == Mode.MISSING_HISTORY)
                return new FlinkCheckpointEvidence.History(JOB, ordinary.counts(), List.of(checkpoint(3, false)),
                        ordinary.latestCompleted(), ordinary.latestFailed(), Optional.empty(), Optional.empty());
            if (phase == 3 && mode == Mode.LATE_COMPLETION)
                return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(3, 0, 2, 1, 0),
                        List.of(checkpoint(3, false), checkpoint(4, true), checkpoint(5, false)), Optional.of(checkpoint(5, false)),
                        Optional.of(checkpoint(4, true)), Optional.empty(), Optional.empty());
            return ordinary;
        }

        private boolean postKillFailures() {
            return mode == Mode.POST_KILL_FAILURES || mode == Mode.DELAYED_POST_KILL_HISTORY || mode == Mode.STALE_POST_KILL_HISTORY;
        }

        StorageFaultControl.Snapshot snapshot(boolean fenced) {
            var instances = new ArrayList<StorageFaultControl.InstanceEvidence>();
            instances.add(instanceEvidence(JM, fenced)); instances.add(instanceEvidence(TM, fenced));
            if (phase >= 5) instances.add(instanceEvidence(REPLACEMENT, fenced));
            return new StorageFaultControl.Snapshot("attempt-1", "storage-1", NAMESPACE, "file:/flink/checkpoints/" + NAMESPACE,
                    HASH, instances, List.of(file("/flink/checkpoints/" + PREFIX + "3/state.bin")), true, fenced, List.of());
        }

        private StorageFaultControl.InstanceEvidence instanceEvidence(StorageFaultControl.Instance instance, boolean fenced) {
            boolean replacement = instance.equals(REPLACEMENT);
            boolean oldDead = phase >= 3 && instance.equals(TM) && mode != Mode.PROCESS_STILL_ALIVE;
            var state = replacement || phase == 0 || phase >= 4 && instance.equals(JM)
                    ? StorageFaultControl.State.HEALTHY : StorageFaultControl.State.ARMED;
            long revision = replacement || phase == 0 ? 0 : phase >= 4 && instance.equals(JM) ? 2 : 1;
            if (phase == 2 && instance.equals(TM) && mode == Mode.LEASE_EXPIRED) state = StorageFaultControl.State.EXPIRED;
            if (phase == 2 && instance.equals(TM) && mode == Mode.DAEMON_DEAD) state = StorageFaultControl.State.FATAL;
            var operations = new ArrayList<StorageFaultControl.Operation>();
            operations.add(operation(instance, 1, "READY", "", 0, 0, ""));
            if (instance.equals(TM)) operations.add(operation(instance, 2, "write", PREFIX + "3/state.bin", 0, 0, ""));
            if (instance.equals(JM)) operations.add(operation(instance, 2, "read", PREFIX + "3/state.bin", 0, 0, ""));
            boolean earlyEio = instance.equals(TM) && phase >= 1 && mode == Mode.EIO_BEFORE_ARM;
            if (earlyEio) operations.add(operation(instance, 3, "write", PREFIX + "4/state.bin", 5, 1, faultId));
            if (!replacement && phase >= 1) operations.add(operation(instance, earlyEio ? 4 : 3,
                    mode == Mode.MISSING_ARM_RECEIPT ? "STATUS" : "ARM", "", 0, 1, faultId));
            if (instance.equals(TM) && phase >= 2 && mode != Mode.MISSING_EIO && !earlyEio)
                operations.add(operation(instance, 4, "write", (mode == Mode.FOREIGN_EIO_PATH ? "other/" : "") + PREFIX + "4/state.bin", 5, 1, faultId));
            if (instance.equals(JM) && phase >= 4) operations.add(operation(instance, 4,
                    mode == Mode.MISSING_HEAL_RECEIPT ? "STATUS" : "HEAL", "", 0, 2, faultId));
            var commands = new ArrayList<StorageFaultControl.Command>();
            if (!replacement && arm != null) arm.acknowledgements().stream().filter(command -> command.instance().equals(instance)).forEach(commands::add);
            if (!replacement && heal != null) heal.acknowledgements().stream().filter(command -> command.instance().equals(instance)).forEach(commands::add);
            boolean complete = !(phase == 2 && instance.equals(TM) && mode == Mode.INCOMPLETE_JOURNAL);
            var observedStatus = new StorageFaultControl.Status(state, revision == 0 ? "" : faultId, revision, operations.size(),
                    revision == 1 ? 10000 : 0, revision == 1 ? 10_000_000_000L : 0, true, false);
            return new StorageFaultControl.InstanceEvidence(instance, verification(instance), observedStatus,
                    fenced || oldDead, complete, "/evidence/" + instance.instanceId() + ".jsonl", HASH, 1000, operations.size(),
                    operations, commands, complete ? List.of() : List.of("truncated journal"));
        }

        private StorageFaultControl.Transition transition(boolean healing) {
            var live = healing ? List.of(JM) : List.of(JM, TM);
            long started = now.get(), completed = started + 1000;
            var commands = live.stream().map(instance -> {
                var receipt = status(healing ? StorageFaultControl.State.HEALTHY : StorageFaultControl.State.ARMED, healing ? 2 : 1, faultId);
                long sequence = receipt.sequence();
                if (!healing && mode == Mode.WRONG_ARM_ACK_SEQUENCE) sequence = 2;
                if (healing && mode == Mode.WRONG_HEAL_ACK_SEQUENCE) sequence = 3;
                if (!healing && mode == Mode.EIO_BEFORE_ARM && instance.equals(TM)) sequence = 4;
                receipt = new StorageFaultControl.Status(receipt.state(), receipt.faultId(), receipt.revision(), sequence,
                        receipt.leaseMs(), receipt.leaseDeadlineMonotonicNs(), true, false);
                return new StorageFaultControl.Command(instance, healing ? "HEAL" : "ARM", started, completed, 0, receipt,
                        "receipt", "", true);
            }).toList();
            return new StorageFaultControl.Transition(faultId, healing ? 2 : 1, started, completed, commands,
                    healing ? List.of(TM) : List.of(), true, List.of());
        }
        long count(String action) { return actions.stream().filter(action::equals).count(); }
        private boolean delayedFailureLog() {
            return mode == Mode.DELAYED_FAILED_MARKER || mode == Mode.MISSING_FAILED_MARKER || mode == Mode.CHECKPOINT_CHANGE_DURING_MARKER;
        }
    }

    private static StorageFaultControl.Snapshot copy(StorageFaultControl.Snapshot original, boolean complete, boolean quiescent,
            List<StorageFaultControl.InstanceEvidence> instances) {
        return new StorageFaultControl.Snapshot(original.attemptId(), original.storageId(), original.namespace(), original.checkpointUri(),
                original.helperManifestSha256(), instances, original.materializedFiles(), complete, quiescent, List.of());
    }
    private static StorageFaultControl.Snapshot withCheckpointUri(StorageFaultControl.Snapshot original, String uri) {
        return new StorageFaultControl.Snapshot(original.attemptId(), original.storageId(), original.namespace(), uri,
                original.helperManifestSha256(), original.instances(), original.materializedFiles(),
                original.complete(), original.quiescent(), original.diagnostics());
    }
    private static CheckpointStorageFault.Evidence withRecovery(CheckpointStorageFault.Evidence original,
            StateRecoveryEvidence.Trace trace, StateRecoveryEvidence.Recovery recovery) {
        return new CheckpointStorageFault.Evidence(original.path(), original.jobId(), original.triggerId(), original.timeoutMillis(),
                original.checkpointId(), original.failedCheckpointId(), original.configuration(), original.readiness(), original.materialization(),
                original.triggerResult(), original.failedCheckpoint(), original.failureTrace(), trace, original.samples(), original.transitions(),
                original.kill(), original.restart(), recovery, original.errors(), original.declaredControls(),
                original.triggerSubmitEvidence(), original.beforeKillEvidence(), original.afterKillEvidence());
    }
    private static StateRecoveryEvidence.Marker copyMarker(StateRecoveryEvidence.Marker original,
            StateRecoveryEvidence.ProcessIdentity process, String uid, long line) {
        return new StateRecoveryEvidence.Marker(process, original.event(), original.jobId(), uid, original.executionAttempt(),
                original.subtask(), original.checkpointId(), original.restoredCheckpointId(), line, original.progress(),
                original.keyProgress(), original.buildMode());
    }
    private static StorageFaultControl.Instance instance(String name, String runtime, FlinkComponentRole role, long pid) {
        return new StorageFaultControl.Instance(name, name + "#" + runtime, runtime, "fuse-" + runtime, role, pid);
    }
    private static StorageFaultControl.Verification verification(StorageFaultControl.Instance instance) {
        return new StorageFaultControl.Verification("sha256:" + "6".repeat(64), HASH, "c".repeat(64),
                "fuse mount verified", List.of(9999L,9999L,9999L,9999L), List.of(9999L,9999L,9999L,9999L),
                "0000000000000000", instance.daemonPid() + 100, "java Flink", true, true, true, List.of());
    }
    private static StorageFaultControl.Status status(StorageFaultControl.State state, long revision, String fault) {
        return new StorageFaultControl.Status(state, fault, revision, revision == 1 ? 3 : 4,
                revision == 1 ? 10000 : 0, revision == 1 ? 10_000_000_000L : 0, true, false);
    }
    private static StorageFaultControl.Operation operation(StorageFaultControl.Instance instance, long sequence, String operation,
            String path, int errno, long revision, String fault) {
        boolean io = operation.equals("read") || operation.equals("write");
        return new StorageFaultControl.Operation(instance, sequence, errno == 0 ? 800 : 1060, sequence, revision,
                revision == 1 ? StorageFaultControl.State.ARMED : StorageFaultControl.State.HEALTHY, fault, operation,
                path.isEmpty() ? "" : "/" + path, errno == 0 ? "" : operation, errno == 0 ? io ? 16 : 0 : -1,
                errno, io ? 16 : 0, errno == 0 && io ? 16 : 0, null, "");
    }
    private static StorageFaultControl.FileEvidence file(String path) { return new StorageFaultControl.FileEvidence(path, 16, true, HASH, 1); }
    private static FlinkCheckpointEvidence.Checkpoint checkpoint(long id, boolean failed) {
        return new FlinkCheckpointEvidence.Checkpoint(id, failed ? FlinkCheckpointEvidence.Status.FAILED : FlinkCheckpointEvidence.Status.COMPLETED,
                false, "CHECKPOINT", id == 3 ? 700 : id == 4 ? 1050 : 1500, failed ? -1 : id == 3 ? 900 : 1700,
                16, 16, 1, failed ? 0 : 1, failed ? OptionalLong.of(1080) : OptionalLong.empty(),
                failed ? Optional.of("java.io.IOException: Input/output error") : Optional.empty(),
                failed ? Optional.empty() : Optional.of("file:/flink/checkpoints/" + PREFIX + id),
                failed ? Optional.empty() : Optional.of(false));
    }
    private static FlinkCheckpointEvidence.History history(int phase) {
        if (phase < 2) return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(1, 0, 1, 0, 0),
                List.of(checkpoint(3, false)), Optional.of(checkpoint(3, false)), Optional.empty(), Optional.empty(), Optional.empty());
        boolean restored = phase >= 5;
        return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(restored ? 3 : 2, 0, restored ? 2 : 1, 1, restored ? 1 : 0),
                restored ? List.of(checkpoint(3, false), checkpoint(4, true), checkpoint(5, false)) : List.of(checkpoint(3, false), checkpoint(4, true)),
                Optional.of(checkpoint(restored ? 5 : 3, false)), Optional.of(checkpoint(4, true)), Optional.empty(),
                restored ? Optional.of(new FlinkCheckpointEvidence.Restored(3, 1300, false, Optional.empty())) : Optional.empty());
    }
    private static FlinkCheckpointEvidence.Checkpoint postKillCheckpoint(long id, long triggered) {
        return new FlinkCheckpointEvidence.Checkpoint(id, FlinkCheckpointEvidence.Status.FAILED, false, "CHECKPOINT",
                triggered, -1, 0, 0, 1, 0, OptionalLong.of(triggered + 25), Optional.of("TaskManager RPC failure"),
                Optional.empty(), Optional.empty());
    }
    private static FlinkCheckpointEvidence.History postKillHistory(List<FlinkCheckpointEvidence.Checkpoint> additional) {
        var before = history(2);
        var retained = new ArrayList<>(before.retained());
        retained.addAll(additional);
        return new FlinkCheckpointEvidence.History(JOB, new FlinkCheckpointEvidence.Counts(2 + additional.size(), 0, 1, 1 + additional.size(), 0),
                retained, before.latestCompleted(), Optional.of(additional.getLast()), Optional.empty(), Optional.empty());
    }
    private static FlinkCheckpointTrigger.Observation failedTrigger() {
        return new FlinkCheckpointTrigger.Observation(FlinkCheckpointTrigger.State.COMPLETED, OptionalLong.empty(),
                Optional.of("java.io.IOException: Input/output error"));
    }
    private static FlinkJobObservation observation(int phase) {
        boolean restored = phase >= 5, stopped = phase == 3 || phase == 4;
        return new FlinkJobObservation(restored ? 2000 : stopped ? 1250 : 1000,
                stopped ? FlinkJobState.RESTARTING : FlinkJobState.RUNNING, restored ? 2 : 1, restored ? 1 : 0,
                restored ? Optional.of(new FlinkJobObservation.Restore(3, 1300)) : Optional.empty(),
                restored || stopped ? List.of(new FlinkJobObservation.Failure(1150, "TaskManagerException", "TaskManager lost", Optional.of("old-resource"))) : List.of(),
                stopped ? List.of() : List.of(new FlinkJobObservation.Subtask("Keyed Prefix v1 -> Kafka Sink: Writer", 0,
                        restored ? 1 : 0, "RUNNING", Optional.of(restored ? "new-resource" : "old-resource"))));
    }
    private static FlinkComponentProvisioningEvidence provisioning(StorageFaultControl.Instance instance) {
        return FlinkComponentProvisioningEvidence.verified(instance.logicalName(), instance.role(), instance.runtimeId(),
                "flink:2.2.0", "sha256:" + "6".repeat(64), HASH, "c".repeat(64), List.of())
                .withProcessConfiguration(Map.of(), instance.process());
    }
    private static FlinkComponentLog log(StorageFaultControl.Instance instance, Path path) { return new FlinkComponentLog(instance.process(), path, false, Optional.empty()); }
    private static String preText() {
        return marker("START", 0, -1, -1, "buildMode=correct")
                + marker("SNAPSHOT", 0, 3, -1, "keyCount=2 observedKeys=2 minimumObservedCount=4 processedThisAttempt=8 maximumInputId=7")
                + marker("COMPLETED", 0, 3, -1, "");
    }
    private static String failedLogText() {
        return preText() + marker("SNAPSHOT", 0, 4, -1,
                "keyCount=2 observedKeys=2 minimumObservedCount=5 processedThisAttempt=10 maximumInputId=9")
                + marker("ABORTED", 0, 4, -1, "");
    }
    private static String postText(boolean missingSuffix) {
        String text = marker("START", 1, 3, 3, "buildMode=correct");
        if (!missingSuffix) {
            text += marker("RESTORED_KEY", 1, 3, 3, "keyCount=2 key=0 inputId=8 previousCount=4 previousSum=16 newCount=5 newSum=25")
                    + marker("RESTORED_KEY", 1, 3, 3, "keyCount=2 key=1 inputId=9 previousCount=4 previousSum=20 newCount=5 newSum=30")
                    + marker("PROGRESS", 1, 3, 3, "keyCount=2 observedKeys=2 minimumObservedCount=5 processedThisAttempt=2 maximumInputId=9");
        }
        return text + marker("SNAPSHOT", 1, 5, 3, "keyCount=2 observedKeys=2 minimumObservedCount=6 processedThisAttempt=4 maximumInputId=11")
                + marker("COMPLETED", 1, 5, 3, "");
    }
    private static String marker(String event, int attempt, long checkpoint, long restored, String fields) {
        return "STATE_PREFIX_" + event + " version=1 jobId=" + JOB + " operatorUid=" + StateRecoveryEvidence.OPERATOR_UID
                + " attempt=" + attempt + " subtask=0 checkpoint=" + checkpoint + " restoredCheckpoint=" + restored
                + (fields.isEmpty() ? "" : " " + fields) + "\n";
    }
    @FunctionalInterface private interface Calls { Object call(String method, Object[] arguments) throws Exception; }
    @SuppressWarnings("unchecked") private static <T> T proxy(Class<T> type, Calls calls) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (ignored, method, arguments) ->
                calls.call(method.getName(), arguments == null ? new Object[0] : arguments));
    }
}
