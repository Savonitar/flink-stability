package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StateRecoveryReportTest {
    private static final String JOB = "a".repeat(32);
    private static final String SHA = "b".repeat(64);
    private static final StateRecoveryEvidence.ProcessIdentity PROCESS =
            new StateRecoveryEvidence.ProcessIdentity("taskmanager-1", "runtime-1", "taskmanager-1#0");

    @Test
    void bothUninterruptedBuildsRequireInitialStartAndObservedAllKeyProgress() {
        for (String mode : List.of("correct", "reset-state-on-restore-v1")) {
            var trace = trace(start(mode) + progress());
            var report = report(trace, 0, true, List.of());
            assertTrue(report.confirmed(), report.reason());
            assertEquals(SHA, report.workloadSha256());
            assertEquals("state-recovery.confirmed", report.reason());
        }
    }

    @Test
    void fewerInputRecordsThanKeysUsesCompleteInputSnapshotForUninterruptedControl() {
        String snapshot = marker("SNAPSHOT", 0, 3, -1,
                "keyCount=8 observedKeys=3 minimumObservedCount=1 processedThisAttempt=3 maximumInputId=2");
        for (String mode : List.of("correct", "reset-state-on-restore-v1")) {
            var report = StateRecoveryReport.evaluate(JOB, SHA, 8, 3, 0, true,
                    trace(start(mode) + snapshot), List.of(), List.of(), List.of());
            assertTrue(report.confirmed(), report.reason());
        }
        for (String incomplete : List.of(snapshot.replace("observedKeys=3", "observedKeys=2"),
                snapshot.replace("maximumInputId=2", "maximumInputId=1"),
                snapshot.replace("processedThisAttempt=3", "processedThisAttempt=2"))) {
            assertFalse(StateRecoveryReport.evaluate(JOB, SHA, 8, 3, 0, true,
                    trace(start("correct") + incomplete), List.of(), List.of(), List.of()).confirmed());
        }
        // A snapshot must not replace the all-key progress receipt when N >= K.
        assertFalse(StateRecoveryReport.evaluate(JOB, SHA, 2, 20, 0, true,
                trace(start("correct") + snapshot.replace("keyCount=8 observedKeys=3", "keyCount=2 observedKeys=2")),
                List.of(), List.of(), List.of()).confirmed());
    }

    @Test
    void missingStartProgressOrMatchingExecutionCannotConfirmControl() {
        for (String text : List.of("", start("correct"), progress(), start("correct") + start("correct") + progress(),
                start("correct") + progress().replace("attempt=0", "attempt=1"),
                start("correct") + progress().replace("observedKeys=2", "observedKeys=1"))) {
            var report = report(trace(text), 0, true, List.of());
            assertFalse(report.confirmed(), text);
            assertEquals(StateRecoveryReport.UNCONFIRMED, report.reason());
        }
    }

    @Test
    void uninterruptedMutantMustNotActivateOrClaimRestoreReceipts() {
        for (String event : List.of("RESTORED_KEY", "MUTANT_RESET")) {
            String activation = marker(event, 1, 3, 3,
                    "keyCount=2 key=0 inputId=4 previousCount=2 previousSum=4 newCount=1 newSum=5");
            assertFalse(report(trace(start("reset-state-on-restore-v1") + progress() + activation),
                    0, true, List.of()).confirmed());
        }
    }

    @Test
    void declaredRestartWithoutBoundRecoveryProofCannotConfirmEvenWithAllMarkers() {
        String after = marker("START", 1, 3, 3, "buildMode=correct")
                + marker("PROGRESS", 1, 3, 3,
                        "keyCount=2 observedKeys=2 minimumObservedCount=3 processedThisAttempt=2 maximumInputId=5");
        var report = report(trace(start("correct") + progress() + after), 1, true, List.of());
        assertFalse(report.confirmed());
        assertEquals(1, report.expectedRestarts());
        assertTrue(report.recoveries().isEmpty());
    }

    @Test
    void captureErrorsMissingFenceAndObservationErrorsRemainVisible() {
        var valid = trace(start("correct") + progress());
        assertFalse(report(valid, 0, false, List.of()).confirmed());
        var incomplete = new StateRecoveryEvidence.Trace(valid.markers(), List.of("truncated"), false);
        assertFalse(report(incomplete, 0, true, List.of()).confirmed());
        var failed = report(valid, 0, true, List.of("readiness timeout"));
        assertFalse(failed.confirmed());
        assertEquals(List.of("readiness timeout"), failed.observationErrors());
    }

    @Test
    void publicConstructorCannotForgeConfirmedFromEmptyOrUnboundTrace() {
        var empty = new StateRecoveryEvidence.Trace(List.of(), List.of(), true);
        assertThrows(IllegalArgumentException.class, () -> new StateRecoveryReport(JOB, SHA, 2, 20,
                0, true, empty, List.of(), List.of(), List.of(), true, "state-recovery.confirmed"));
        var valid = trace(start("correct") + progress());
        assertThrows(IllegalArgumentException.class, () -> new StateRecoveryReport("other-job", SHA, 2, 20,
                0, true, valid, List.of(), List.of(), List.of(), true, "state-recovery.confirmed"));
        assertThrows(IllegalArgumentException.class, () -> new StateRecoveryReport(JOB, SHA, 2, 20,
                1, true, valid, List.of(), List.of(), List.of(), true, "state-recovery.confirmed"));
        assertDoesNotThrow(() -> new StateRecoveryReport(JOB, SHA, 2, 20,
                0, true, valid, List.of(), List.of(), List.of(), true, "state-recovery.confirmed"));
    }

    private static StateRecoveryReport report(StateRecoveryEvidence.Trace trace, int expected,
                                               boolean fenced, List<String> errors) {
        return StateRecoveryReport.evaluate(JOB, SHA, 2, 20, expected, fenced, trace, List.of(), List.of(), errors);
    }

    private static StateRecoveryEvidence.Trace trace(String text) {
        return StateRecoveryEvidence.parse(PROCESS, text, JOB);
    }

    private static String start(String mode) { return marker("START", 0, -1, -1, "buildMode=" + mode); }

    private static String progress() {
        return marker("PROGRESS", 0, -1, -1,
                "keyCount=2 observedKeys=2 minimumObservedCount=1 processedThisAttempt=2 maximumInputId=1");
    }

    private static String marker(String event, int attempt, long checkpoint, long restored, String fields) {
        return "STATE_PREFIX_" + event + " version=1 jobId=" + JOB + " operatorUid=" + StateRecoveryEvidence.OPERATOR_UID
                + " attempt=" + attempt + " subtask=0 checkpoint=" + checkpoint + " restoredCheckpoint=" + restored
                + " " + fields + "\n";
    }
}
