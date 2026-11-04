package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateRecoveryEvidenceTest {
    private static final String JOB = "a".repeat(32);
    private static final StateRecoveryEvidence.ProcessIdentity BEFORE =
            new StateRecoveryEvidence.ProcessIdentity("taskmanager-1", "old-runtime", "taskmanager-1#0");
    private static final StateRecoveryEvidence.ProcessIdentity AFTER =
            new StateRecoveryEvidence.ProcessIdentity("taskmanager-1", "new-runtime", "taskmanager-1#1");
    private static final TaskManagerControl.Identity TARGET =
            new TaskManagerControl.Identity("taskmanager-1", "old-runtime", "old-resource");
    @TempDir Path temporary;

    @Test
    void bindsRunningExecutionCompletedCheckpointAndCommittedPrefixBeforeKill() throws Exception {
        var ready = ready(preTrace("correct"), checkpoints(3), prefix());
        assertTrue(ready.ready(), ready.reason());
        assertEquals(0, ready.executionAttempt());
        assertEquals(TARGET, ready.target());
        assertEquals(List.of(3L), ready.completedSnapshots().keySet().stream().toList());
        assertEquals(7, ready.completedSnapshots().get(3L).snapshot().progress().orElseThrow().maximumInputId());
        assertEquals(900, ready.completedSnapshots().get(3L).acknowledgedAtMillis());
    }

    @Test
    void restoresOnlyCheckpointThatAlreadyHadBoundStateProofBeforeKill() throws Exception {
        var ready = ready(preTrace("correct"), checkpoints(3), prefix());
        var recovered = StateRecoveryEvidence.evaluate(ready, effect(3), postTrace("correct", false));
        assertTrue(recovered.confirmed(), recovered.reason());
        assertEquals(3, recovered.restoredCheckpointId());
        assertEquals(2, recovered.restoredKeys().size());
        assertEquals(1, recovered.restoredStart().orElseThrow().executionAttempt());
        assertEquals("new-runtime", recovered.restoredStart().orElseThrow().process().runtimeId());
        assertTrue(recovered.mutantResets().isEmpty());

        var unknown = StateRecoveryEvidence.evaluate(ready, effect(4), postTrace("correct", false));
        assertFalse(unknown.confirmed());
        assertEquals("state-recovery.restore-checkpoint-unproven", unknown.reason());
    }

    @Test
    void acceptsKnownLaterCheckpointAndRejectsLateUnobservedCompletion() throws Exception {
        String later = marker("SNAPSHOT", 0, 4, -1,
                "keyCount=2 observedKeys=2 minimumObservedCount=5 processedThisAttempt=10 maximumInputId=9")
                + marker("COMPLETED", 0, 4, -1, "");
        var trace = StateRecoveryEvidence.parse(BEFORE, preText("correct") + later, JOB);
        var overview = new ObjectMapper().readTree("""
                {"latest":{"completed":{"id":4,"status":"COMPLETED","latest_ack_timestamp":950}},
                 "history":[{"id":3,"status":"COMPLETED","latest_ack_timestamp":900}]}
                """);
        var ready = ready(trace, overview, prefix());
        assertTrue(ready.ready(), ready.reason());
        assertEquals(2, ready.completedSnapshots().size());
        var tooLate = ready(trace, new ObjectMapper().readTree("""
                {"latest":{"completed":{"id":4,"status":"COMPLETED","latest_ack_timestamp":1001}}}
                """), prefix());
        assertFalse(tooLate.ready());
    }

    @Test
    void latestCompletedCheckpointMustBeProvenEvenWhenAnOlderCheckpointIsReady() throws Exception {
        var overview = new ObjectMapper().readTree("""
                {"counts":{"in_progress":0},
                 "latest":{"completed":{"id":4,"status":"COMPLETED","latest_ack_timestamp":950}},
                 "history":[{"id":3,"status":"COMPLETED","latest_ack_timestamp":900}]}
                """);
        var missingLatest = ready(preTrace("correct"), overview, prefix());
        assertFalse(missingLatest.ready());
        assertEquals("state-recovery.latest-checkpoint-unproven", missingLatest.reason());
        assertEquals(1, missingLatest.completedSnapshots().size());
        assertTrue(missingLatest.completedSnapshots().containsKey(3L));

        String later = marker("SNAPSHOT", 0, 4, -1,
                "keyCount=2 observedKeys=2 minimumObservedCount=5 processedThisAttempt=10 maximumInputId=9")
                + marker("COMPLETED", 0, 4, -1, "");
        var both = StateRecoveryEvidence.parse(BEFORE, preText("correct") + later, JOB);
        var futureAck = overview.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) futureAck.at("/latest/completed"))
                .put("latest_ack_timestamp", 1001);
        var staleClock = ready(both, futureAck, prefix());
        assertFalse(staleClock.ready());
        assertEquals("state-recovery.latest-checkpoint-unproven", staleClock.reason());
        assertTrue(staleClock.completedSnapshots().containsKey(3L));
        assertFalse(staleClock.completedSnapshots().containsKey(4L));
        assertTrue(ready(both, overview, prefix()).ready());
    }

    @Test
    void visibleInFlightCheckpointsPreventArmingDespiteCompletePreviousState() throws Exception {
        for (String extra : List.of("\"counts\":{\"in_progress\":1}",
                "\"history\":[{\"id\":4,\"status\":\"IN_PROGRESS\"}]",
                "\"observedCheckpointDetails\":{\"id\":4,\"status\":\"IN_PROGRESS\"}")) {
            var overview = new ObjectMapper().readTree("{\"latest\":{\"completed\":{\"id\":3,"
                    + "\"status\":\"COMPLETED\",\"latest_ack_timestamp\":900}}," + extra + "}");
            var readiness = ready(preTrace("correct"), overview, prefix());
            assertFalse(readiness.ready(), extra);
            assertEquals("state-recovery.checkpoint-in-flight", readiness.reason());
            assertTrue(readiness.completedSnapshots().containsKey(3L));
        }
    }

    @Test
    void newCheckpointReceiptsAfterRestObservationRequireAFreshReadinessBoundary() throws Exception {
        String later = marker("SNAPSHOT", 0, 4, -1,
                "keyCount=2 observedKeys=2 minimumObservedCount=5 processedThisAttempt=10 maximumInputId=9");
        var inFlight = ready(StateRecoveryEvidence.parse(BEFORE, preText("correct") + later, JOB), checkpoints(3), prefix());
        assertFalse(inFlight.ready());
        assertEquals("state-recovery.checkpoint-in-flight", inFlight.reason());
        var completed = ready(StateRecoveryEvidence.parse(BEFORE, preText("correct") + later
                + marker("COMPLETED", 0, 4, -1, ""), JOB), checkpoints(3), prefix());
        assertFalse(completed.ready());
        assertEquals("state-recovery.checkpoint-boundary-stale", completed.reason());
        var aborted = ready(StateRecoveryEvidence.parse(BEFORE, preText("correct") + later
                + marker("ABORTED", 0, 4, -1, ""), JOB), checkpoints(3), prefix());
        assertTrue(aborted.ready(), aborted.reason());
    }

    @Test
    void missingLatestIdentityAndMalformedInflightCountsCannotReuseHistory() throws Exception {
        var onlyHistory = new ObjectMapper().readTree("""
                {"history":[{"id":3,"status":"COMPLETED","latest_ack_timestamp":900}]}
                """);
        assertEquals("state-recovery.latest-checkpoint-unproven",
                ready(preTrace("correct"), onlyHistory, prefix()).reason());
        for (String count : List.of("\"0\"", "-1", "0.5", "null")) {
            var overview = new ObjectMapper().readTree("{\"latest\":{\"completed\":{\"id\":3,"
                    + "\"status\":\"COMPLETED\",\"latest_ack_timestamp\":900}},\"counts\":{\"in_progress\":" + count + "}}");
            var readiness = ready(preTrace("correct"), overview, prefix());
            assertFalse(readiness.ready());
            assertEquals("state-recovery.checkpoint-observation-invalid", readiness.reason());
        }
    }

    @Test
    void allKeyPostRestoreProgressIsProofEvenForDeliberateStateReset() throws Exception {
        var ready = ready(preTrace("reset-state-on-restore-v1"), checkpoints(3), prefix());
        var proof = StateRecoveryEvidence.evaluate(ready, effect(3), postTrace("reset-state-on-restore-v1", true));
        assertTrue(proof.confirmed(), proof.reason());
        assertEquals(2, proof.mutantResets().size());
        assertEquals(1, proof.restoredKeys().getFirst().keyProgress().orElseThrow().newCount());
        // State correctness is deliberately left to the independent terminal oracle.
        var absent = StateRecoveryEvidence.evaluate(ready, effect(3), postTrace("reset-state-on-restore-v1", false));
        assertFalse(absent.confirmed());
        assertEquals("state-recovery.mutant-activation-unconfirmed", absent.reason());
    }

    @Test
    void observedStateLossDoesNotHideConfirmedRecoveryProvenanceFromTheIndependentOracle() throws Exception {
        var readiness = ready(preTrace("correct"), checkpoints(3), prefix());
        String lostState = postText("correct", false).replace("previousCount=4", "previousCount=0")
                .replace("previousSum=16", "previousSum=0").replace("previousSum=20", "previousSum=0")
                .replace("newCount=5", "newCount=1").replace("newSum=25", "newSum=9")
                .replace("newSum=30", "newSum=10").replace("minimumObservedCount=5", "minimumObservedCount=1");
        var recovery = StateRecoveryEvidence.evaluate(readiness, effect(3),
                StateRecoveryEvidence.parse(AFTER, lostState, JOB));
        assertTrue(recovery.confirmed(), recovery.reason());
        assertEquals(0, recovery.restoredKeys().getFirst().keyProgress().orElseThrow().previousCount());
        // The terminal state oracle must still reject the reset prefixes. This receipt proof
        // establishes the restored checkpoint/attempt and suffix execution, not state correctness.
    }

    @Test
    void missingPartialOrMalformedReadinessProofNeverArmsKill() throws Exception {
        List<String> invalid = List.of(
                preText("correct").replace("observedKeys=2", "observedKeys=1"),
                preText("correct").replace("maximumInputId=7", "maximumInputId=19"),
                preText("correct").replace("minimumObservedCount=4", "minimumObservedCount=0"),
                preText("correct").replace("attempt=0", "attempt=1"),
                preText("correct").replace("STATE_PREFIX_COMPLETED", "STATE_PREFIX_ABORTED"),
                preText("correct") + marker("SNAPSHOT", 0, 3, -1,
                        "keyCount=2 observedKeys=2 minimumObservedCount=4 processedThisAttempt=8 maximumInputId=7"));
        for (String text : invalid) assertFalse(ready(StateRecoveryEvidence.parse(BEFORE, text, JOB),
                checkpoints(3), prefix()).ready(), text);
        var missingPrefix = new StateRecoveryEvidence.CommittedPrefix(false, true, 8, 7,
                Map.of(0, 4L, 1, 4L), Map.of(0, 0L), Map.of(0, 10L));
        assertFalse(ready(preTrace("correct"), checkpoints(3), missingPrefix).ready());
        var missingKey = new StateRecoveryEvidence.CommittedPrefix(true, true, 8, 7,
                Map.of(0, 8L), Map.of(0, 0L), Map.of(0, 10L));
        assertFalse(ready(preTrace("correct"), checkpoints(3), missingKey).ready());
        var finished = StateRecoveryEvidence.ready(JOB, 2, 20, TARGET,
                observation(FlinkJobState.FINISHED), checkpoints(3), preTrace("correct"), prefix());
        assertFalse(finished.ready());
    }

    @Test
    void cannotReuseOriginalProcessAttemptOrWrongCheckpointProgress() throws Exception {
        var ready = ready(preTrace("correct"), checkpoints(3), prefix());
        String valid = postText("correct", false);
        for (String invalid : List.of(valid.replace("attempt=1", "attempt=0"),
                valid.replace("restoredCheckpoint=3", "restoredCheckpoint=4"),
                valid.replace("inputId=8", "inputId=6"),
                valid.replace("STATE_PREFIX_RESTORED_KEY", "STATE_PREFIX_MUTANT_RESET"),
                valid.replace("observedKeys=2", "observedKeys=1"))) {
            assertFalse(StateRecoveryEvidence.evaluate(ready, effect(3),
                    StateRecoveryEvidence.parse(AFTER, invalid, JOB)).confirmed(), invalid);
        }
        assertFalse(StateRecoveryEvidence.evaluate(ready, effect(3),
                StateRecoveryEvidence.parse(BEFORE, valid, JOB)).confirmed());
        assertFalse(StateRecoveryEvidence.evaluate(ready, effect(3),
                StateRecoveryEvidence.parse(AFTER, valid + valid, JOB)).confirmed());
    }

    @Test
    void strictParserRejectsUnknownDuplicateOutOfRangeAndOverlongFields() {
        String valid = marker("START", 0, -1, -1, "buildMode=correct");
        for (String invalid : List.of(valid.replace("version=1", "version=2"),
                valid.replace("attempt=0", "attempt=2147483648"),
                valid.replace("attempt=0", "attempt=01"),
                valid.replace("attempt=0", "attempt=0 attempt=0"),
                valid.replace("subtask=0", "subtask=1"),
                valid.replace("buildMode=correct", "unknown=1"),
                valid.replace("buildMode=correct", "buildMode=" + "x".repeat(2048)))) {
            var trace = StateRecoveryEvidence.parse(BEFORE, invalid, JOB);
            assertFalse(trace.complete(), invalid);
            assertFalse(trace.diagnostics().isEmpty());
        }
        var unrelated = StateRecoveryEvidence.parse(BEFORE, valid.replace(JOB, "b".repeat(32)), JOB);
        assertTrue(unrelated.complete());
        assertTrue(unrelated.markers().isEmpty());
    }

    @Test
    void parserCapsRetainedMarkersAndDiagnostics() {
        var trace = StateRecoveryEvidence.parse(BEFORE,
                marker("COMPLETED", 0, 3, -1, "").repeat(StateRecoveryEvidence.MAX_MARKERS + 102), JOB);
        assertFalse(trace.complete());
        assertEquals(StateRecoveryEvidence.MAX_MARKERS, trace.markers().size());
        assertEquals(100, trace.diagnostics().size());
    }

    @Test
    void collectionBindsPhysicalProcessAndIgnoresOnlyIncompleteLiveTail() throws Exception {
        Path log = temporary.resolve("before.log");
        Files.writeString(log, preText("correct") + "STATE_PREFIX_START version=");
        var trace = StateRecoveryEvidence.collect(List.of(provisioning(BEFORE)),
                List.of(new FlinkComponentLog(BEFORE.process(), log, false, Optional.empty())), JOB);
        assertTrue(trace.complete(), trace.diagnostics().toString());
        assertEquals(3, trace.markers().size());
        assertEquals("old-runtime", trace.markers().getFirst().process().runtimeId());
        assertFalse(StateRecoveryEvidence.collect(List.of(provisioning(BEFORE)),
                List.of(new FlinkComponentLog(BEFORE.process(), log, true, Optional.empty())), JOB).complete());
        assertFalse(StateRecoveryEvidence.collect(List.of(provisioning(BEFORE), provisioning(AFTER)),
                List.of(new FlinkComponentLog(BEFORE.process(), log, false, Optional.empty()),
                        new FlinkComponentLog(AFTER.process(), log, false, Optional.empty())), JOB).complete());
    }

    @Test
    void collectionRejectsInvalidUtf8AndUnreadableOrMissingBindings() throws Exception {
        Path log = temporary.resolve("bad.log");
        Files.write(log, new byte[] {(byte) 0xc3, (byte) 0x28, (byte) '\n'});
        assertFalse(StateRecoveryEvidence.collect(List.of(provisioning(BEFORE)),
                List.of(new FlinkComponentLog(BEFORE.process(), log, false, Optional.empty())), JOB).complete());
        assertFalse(StateRecoveryEvidence.collect(List.of(provisioning(BEFORE)), List.of(), JOB).complete());
        assertFalse(StateRecoveryEvidence.collect(List.of(), List.of(), JOB).complete());
    }

    @Test
    void aPartialFinalLineIsUnconfirmedAfterFenceButMayBeAGrowingLiveTail() throws Exception {
        Path log = temporary.resolve("last-line.log");
        var components = List.of(provisioning(BEFORE));
        var logs = List.of(new FlinkComponentLog(BEFORE.process(), log, false, Optional.empty()));
        Files.writeString(log, preText("correct") + "STATE_PREFIX_MUTANT_RESET version=1");
        assertTrue(StateRecoveryEvidence.collect(components, logs, JOB).complete());
        var finalCapture = StateRecoveryEvidence.collectFenced(components, logs, JOB);
        assertFalse(finalCapture.complete());
        assertEquals(3, finalCapture.markers().size());
        assertTrue(finalCapture.diagnostics().stream().anyMatch(text -> text.contains("incomplete final output line")));
        Files.writeString(log, preText("correct"));
        assertTrue(StateRecoveryEvidence.collectFenced(components, logs, JOB).complete());
    }

    private StateRecoveryEvidence.Readiness ready(StateRecoveryEvidence.Trace trace,
                                                  JsonNode checkpoints, StateRecoveryEvidence.CommittedPrefix prefix) {
        return StateRecoveryEvidence.ready(JOB, 2, 20, TARGET, observation(FlinkJobState.RUNNING), checkpoints, trace, prefix);
    }

    private static StateRecoveryEvidence.CommittedPrefix prefix() {
        return new StateRecoveryEvidence.CommittedPrefix(true, true, 8, 7,
                Map.of(0, 4L, 1, 4L), Map.of(0, 0L), Map.of(0, 10L));
    }

    private static FlinkJobObservation observation(FlinkJobState state) {
        return new FlinkJobObservation(1000, state, 1, 0, Optional.empty(), List.of(),
                List.of(new FlinkJobObservation.Subtask("Keyed Prefix v1 -> Kafka Sink: Writer", 0, 0,
                        "RUNNING", Optional.of("old-resource"))));
    }

    private static TaskManagerKillEffect effect(long checkpoint) {
        var kill = new PhaseExecutionEvidence.TaskManagerKill("$/phases/1/steps/0", List.of(), "taskmanager-1",
                new FlinkJobObservation.Attempt(Optional.of(observation(FlinkJobState.RUNNING)), Optional.empty()),
                OptionalLong.of(1100), OptionalLong.of(1200), Optional.of(TARGET));
        var failure = new FlinkJobObservation.Failure(1150, "TaskManagerException", "lost", Optional.of("old-resource"));
        var after = new FlinkJobObservation(2000, FlinkJobState.FINISHED, 2, 1,
                Optional.of(new FlinkJobObservation.Restore(checkpoint, 1300)), List.of(failure), List.of());
        return TaskManagerKillEffect.evaluate(List.of(kill), Optional.of(new FlinkJobObservation.Attempt(
                Optional.of(after), Optional.empty()))).getFirst();
    }

    private static JsonNode checkpoints(long checkpoint) throws Exception {
        return new ObjectMapper().readTree("{\"latest\":{\"completed\":{\"id\":" + checkpoint
                + ",\"status\":\"COMPLETED\",\"latest_ack_timestamp\":900}}}");
    }

    private static StateRecoveryEvidence.Trace preTrace(String mode) {
        return StateRecoveryEvidence.parse(BEFORE, preText(mode), JOB);
    }

    private static String preText(String mode) {
        return marker("START", 0, -1, -1, "buildMode=" + mode)
                + marker("SNAPSHOT", 0, 3, -1,
                        "keyCount=2 observedKeys=2 minimumObservedCount=4 processedThisAttempt=8 maximumInputId=7")
                + marker("COMPLETED", 0, 3, -1, "");
    }

    private static StateRecoveryEvidence.Trace postTrace(String mode, boolean resets) {
        return StateRecoveryEvidence.parse(AFTER, postText(mode, resets), JOB);
    }

    private static String postText(String mode, boolean resets) {
        var text = new StringBuilder(marker("START", 1, 3, 3, "buildMode=" + mode));
        for (int key = 0; key < 2; key++) {
            String fields = "keyCount=2 key=" + key + " inputId=" + (8 + key)
                    + " previousCount=4 previousSum=" + (key == 0 ? 16 : 20)
                    + " newCount=" + (resets ? 1 : 5) + " newSum=" + (resets ? 9 + key : key == 0 ? 25 : 30);
            text.append(marker("RESTORED_KEY", 1, 3, 3, fields));
            if (resets) text.append(marker("MUTANT_RESET", 1, 3, 3, fields));
        }
        return text + marker("PROGRESS", 1, 3, 3, "keyCount=2 observedKeys=2 minimumObservedCount="
                + (resets ? 1 : 5) + " processedThisAttempt=2 maximumInputId=9");
    }

    private static String marker(String event, int attempt, long checkpoint, long restored, String fields) {
        return "2026-10-26 12:00:00 INFO State - STATE_PREFIX_" + event + " version=1 jobId=" + JOB
                + " operatorUid=" + StateRecoveryEvidence.OPERATOR_UID + " attempt=" + attempt
                + " subtask=0 checkpoint=" + checkpoint + " restoredCheckpoint=" + restored
                + (fields.isEmpty() ? "" : " " + fields) + "\n";
    }

    private static FlinkComponentProvisioningEvidence provisioning(StateRecoveryEvidence.ProcessIdentity process) {
        return FlinkComponentProvisioningEvidence.verified(process.logicalName(), FlinkComponentRole.TASK_MANAGER,
                process.runtimeId(), "flink:2.2.0", "sha256:" + "1".repeat(64), "2".repeat(64), "3".repeat(64), List.of())
                .withProcessConfiguration(Map.of(), process.process());
    }
}
