package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.StateRecoveryEvidence;
import org.savonitar.flink.stability.core.execution.StateRecoveryReport;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateRecoveryEvidenceRendererTest {
    private static final String JOB = "a".repeat(32);
    private static final String WORKLOAD_SHA = "0123456789abcdef".repeat(4);
    private static final String MUTANT = "reset-state-on-restore-v1";
    private static final StateRecoveryEvidence.ProcessIdentity ORIGINAL =
            new StateRecoveryEvidence.ProcessIdentity("taskmanager-1", "runtime-before", "taskmanager-1#1");
    private static final StateRecoveryEvidence.ProcessIdentity REPLACEMENT =
            new StateRecoveryEvidence.ProcessIdentity("taskmanager-1", "runtime-after", "taskmanager-1#2");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void rendersNestedTypedProofWithoutLosingMapKeysOptionalsOrMutantReceipts() throws Exception {
        var initial = marker(StateRecoveryEvidence.Event.START, false, -1, 1,
                Optional.empty(), Optional.empty(), Optional.of(MUTANT));
        var saved = marker(StateRecoveryEvidence.Event.SNAPSHOT, false, 3, 2,
                Optional.of(new StateRecoveryEvidence.Progress(2, 2, 4, 8, 7)), Optional.empty(), Optional.empty());
        var completed = marker(StateRecoveryEvidence.Event.COMPLETED, false, 3, 3,
                Optional.empty(), Optional.empty(), Optional.empty());
        var ready = readiness(saved, completed);
        var restored = marker(StateRecoveryEvidence.Event.START, true, 3, 1,
                Optional.empty(), Optional.empty(), Optional.of(MUTANT));
        var keyZero = new StateRecoveryEvidence.KeyProgress(2, 0, 8, 4, 16, 1, 9);
        var keyOne = new StateRecoveryEvidence.KeyProgress(2, 1, 9, 4, 20, 1, 10);
        var first = marker(StateRecoveryEvidence.Event.RESTORED_KEY, true, 3, 2,
                Optional.empty(), Optional.of(keyZero), Optional.empty());
        var resetZero = marker(StateRecoveryEvidence.Event.MUTANT_RESET, true, 3, 3,
                Optional.empty(), Optional.of(keyZero), Optional.empty());
        var second = marker(StateRecoveryEvidence.Event.RESTORED_KEY, true, 3, 4,
                Optional.empty(), Optional.of(keyOne), Optional.empty());
        var resetOne = marker(StateRecoveryEvidence.Event.MUTANT_RESET, true, 3, 5,
                Optional.empty(), Optional.of(keyOne), Optional.empty());
        var progress = marker(StateRecoveryEvidence.Event.PROGRESS, true, 3, 6,
                Optional.of(new StateRecoveryEvidence.Progress(2, 2, 1, 2, 9)), Optional.empty(), Optional.empty());
        var trace = new StateRecoveryEvidence.Trace(List.of(initial, saved, completed, restored,
                first, resetZero, second, resetOne, progress), List.of(), true);
        var recovery = new StateRecoveryEvidence.Recovery(true, "state-recovery.confirmed", ready, 3,
                Optional.of(restored), Optional.of(progress), List.of(first, second), List.of(resetZero, resetOne));
        // This is a serialization fixture; report business validation has its own core tests.
        var report = new StateRecoveryReport(JOB, WORKLOAD_SHA, 2, 20, 1, true, trace,
                List.of(ready), List.of(recovery), List.of(), false, StateRecoveryReport.UNCONFIRMED);

        JsonNode rendered = roundTrip(report);
        assertEquals(WORKLOAD_SHA, rendered.path("workloadSha256").textValue());
        assertEquals(JOB, rendered.path("jobId").textValue());
        assertEquals(1, rendered.path("expectedRestarts").intValue());
        assertFalse(rendered.path("confirmed").booleanValue());
        assertEquals(9, rendered.at("/trace/markers").size());
        assertEquals("START", rendered.at("/trace/markers/0/event").textValue());
        assertEquals("MUTANT_RESET", rendered.at("/trace/markers/5/event").textValue());

        var checkpointMap = rendered.at("/recoveries/0/readiness/completedSnapshots");
        assertTrue(checkpointMap.isObject());
        assertEquals(List.of("3"), fieldNames(checkpointMap));
        assertEquals(3, checkpointMap.at("/3/checkpointId").longValue());
        assertEquals(900, checkpointMap.at("/3/acknowledgedAtMillis").longValue());
        assertEquals("SNAPSHOT", checkpointMap.at("/3/snapshot/event").textValue());
        assertEquals("COMPLETED", checkpointMap.at("/3/completion/event").textValue());
        assertEquals(7, checkpointMap.at("/3/snapshot/progress/maximumInputId").longValue());
        assertTrue(checkpointMap.at("/3/completion/progress").isNull());
        assertEquals(4, rendered.at("/readinessAttempts/0/committedPrefix/perKeyCounts/0").longValue());
        assertEquals(4, rendered.at("/readinessAttempts/0/committedPrefix/perKeyCounts/1").longValue());
        assertEquals(0, rendered.at("/readinessAttempts/0/committedPrefix/beginningOffsets/0").longValue());
        assertEquals(10, rendered.at("/readinessAttempts/0/committedPrefix/endOffsets/0").longValue());
        assertEquals("resource-before", rendered.at("/readinessAttempts/0/target/resourceId").textValue());

        assertEquals("RUNNING", rendered.at("/readinessAttempts/0/jobObservation/state").textValue());
        assertEquals(2, rendered.at("/readinessAttempts/0/jobObservation/latestRestore/checkpointId").longValue());
        assertEquals("resource-before", rendered.at("/readinessAttempts/0/jobObservation/subtasks/0/taskManagerId").textValue());
        assertTrue(rendered.at("/readinessAttempts/0/jobObservation/failures/0/taskManagerId").isNull());
        assertEquals("runtime-after", rendered.at("/recoveries/0/restoredStart/process/runtimeId").textValue());
        assertEquals(MUTANT, rendered.at("/recoveries/0/restoredStart/buildMode").textValue());
        assertTrue(rendered.at("/recoveries/0/restoredStart/keyProgress").isNull());
        assertEquals(2, rendered.at("/recoveries/0/postRestoreProgress/progress/processedThisAttempt").longValue());
        assertEquals(2, rendered.at("/recoveries/0/restoredKeys").size());
        assertEquals(2, rendered.at("/recoveries/0/mutantResets").size());
        assertEquals(0, rendered.at("/recoveries/0/mutantResets/0/keyProgress/key").intValue());
        assertEquals(1, rendered.at("/recoveries/0/mutantResets/1/keyProgress/key").intValue());
        assertEquals(20, rendered.at("/recoveries/0/mutantResets/1/keyProgress/previousSum").longValue());
        assertEquals(10, rendered.at("/recoveries/0/mutantResets/1/keyProgress/newSum").longValue());
        assertEquals(1, rendered.at("/recoveries/0/mutantResets/1/keyProgress/newCount").longValue());
    }

    @Test
    void emitsExplicitNullOptionalsAndRetainsPartialEvidenceDiagnostics() throws Exception {
        var saved = marker(StateRecoveryEvidence.Event.SNAPSHOT, false, 3, 2,
                Optional.of(new StateRecoveryEvidence.Progress(2, 2, 4, 8, 7)), Optional.empty(), Optional.empty());
        var completed = marker(StateRecoveryEvidence.Event.COMPLETED, false, 3, 3,
                Optional.empty(), Optional.empty(), Optional.empty());
        var ready = readiness(saved, completed);
        var recovery = new StateRecoveryEvidence.Recovery(false, "state-recovery.restore-unconfirmed", ready, -1,
                Optional.empty(), Optional.empty(), List.of(), List.of());
        var report = new StateRecoveryReport(JOB, WORKLOAD_SHA, 2, 20, 1, false,
                new StateRecoveryEvidence.Trace(List.of(saved), List.of("incomplete captured output"), false),
                List.of(ready), List.of(recovery), List.of("replacement observation unavailable"),
                false, StateRecoveryReport.UNCONFIRMED);

        var rendered = roundTrip(report);
        assertTrue(rendered.at("/recoveries/0").has("restoredStart"));
        assertTrue(rendered.at("/recoveries/0/restoredStart").isNull());
        assertTrue(rendered.at("/recoveries/0/postRestoreProgress").isNull());
        assertEquals(0, rendered.at("/recoveries/0/mutantResets").size());
        assertEquals(0, rendered.at("/recoveries/0/restoredKeys").size());
        assertEquals(-1, rendered.at("/recoveries/0/restoredCheckpointId").longValue());
        assertEquals("incomplete captured output", rendered.at("/trace/diagnostics/0").textValue());
        assertEquals("replacement observation unavailable", rendered.at("/observationErrors/0").textValue());
        assertFalse(rendered.at("/trace/complete").booleanValue());
        assertFalse(rendered.path("physicalFenceConfirmed").booleanValue());
    }

    private static JsonNode roundTrip(StateRecoveryReport report) throws Exception {
        return JSON.readTree(JSON.writeValueAsString(StateRecoveryEvidenceRenderer.render(report, JSON)));
    }

    private static List<String> fieldNames(JsonNode value) {
        var names = new ArrayList<String>();
        value.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static StateRecoveryEvidence.Readiness readiness(StateRecoveryEvidence.Marker saved,
                                                             StateRecoveryEvidence.Marker completed) {
        var observation = new FlinkJobObservation(1_000, FlinkJobState.RUNNING, 1, 1,
                Optional.of(new FlinkJobObservation.Restore(2, 800)),
                List.of(new FlinkJobObservation.Failure(700, "ExampleFailure", "retained observation", Optional.empty())),
                List.of(new FlinkJobObservation.Subtask("Keyed Prefix v1", 0, 0, "RUNNING", Optional.of("resource-before"))));
        return new StateRecoveryEvidence.Readiness(true, "state-recovery.ready", JOB, 2, 20,
                new TaskManagerControl.Identity("taskmanager-1", "runtime-before", "resource-before"), 0,
                observation, Map.of(3L, new StateRecoveryEvidence.Checkpoint(3, 900, saved, completed)),
                new StateRecoveryEvidence.CommittedPrefix(true, true, 8, 7,
                        Map.of(0, 4L, 1, 4L), Map.of(0, 0L), Map.of(0, 10L)));
    }

    private static StateRecoveryEvidence.Marker marker(StateRecoveryEvidence.Event event, boolean restored,
            long checkpoint, long line, Optional<StateRecoveryEvidence.Progress> progress,
            Optional<StateRecoveryEvidence.KeyProgress> key, Optional<String> mode) {
        return new StateRecoveryEvidence.Marker(restored ? REPLACEMENT : ORIGINAL, event, JOB,
                StateRecoveryEvidence.OPERATOR_UID, restored ? 1 : 0, 0, checkpoint, restored ? 3 : -1,
                line, progress, key, mode);
    }
}
