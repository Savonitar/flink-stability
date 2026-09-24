package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan.ExpectedOutcome;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.validation.kafka.KafkaIdSetValidationResult;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScenarioVerdictTest {
    private static final String DUPLICATES = "validator.kafka.id-set.duplicate-ids";
    private static final ExpectedOutcome EXPECT_DUPLICATES =
            ExpectedOutcome.failure("kafka.id-set", DUPLICATES);

    @Test
    void anExpectedPassMatchesOnlyAPassingAttempt() {
        ScenarioVerdict passed = ScenarioVerdict.of(ExpectedOutcome.pass(),
                attempt(V1ScenarioExecutionResult.Status.PASS, "validator.kafka.id-set.match"));
        ScenarioVerdict failed = ScenarioVerdict.of(ExpectedOutcome.pass(),
                attempt(V1ScenarioExecutionResult.Status.FAIL, DUPLICATES));

        assertEquals(ScenarioVerdict.Status.PASS, passed.status());
        assertTrue(passed.matched());
        assertEquals(ScenarioVerdict.Status.FAIL, failed.status());
        assertEquals(DUPLICATES, failed.reason(), "a failing attempt keeps its own reason");
    }

    @Test
    void aNegativeControlPassesWhenItFailsExactlyAsPinned() {
        ScenarioVerdict verdict = ScenarioVerdict.of(EXPECT_DUPLICATES,
                attempt(V1ScenarioExecutionResult.Status.FAIL, DUPLICATES));

        assertEquals(ScenarioVerdict.Status.PASS, verdict.status());
        assertTrue(verdict.matched());
        assertEquals(DUPLICATES, verdict.reason());
        assertTrue(verdict.message().startsWith("The expected failure occurred"));
    }

    @Test
    void aNegativeControlFailsWhenTheOracleMissesTheDefectOrReportsAnotherOne() {
        ScenarioVerdict missed = ScenarioVerdict.of(EXPECT_DUPLICATES,
                attempt(V1ScenarioExecutionResult.Status.PASS, "validator.kafka.id-set.match"));
        ScenarioVerdict otherDefect = ScenarioVerdict.of(EXPECT_DUPLICATES,
                attempt(V1ScenarioExecutionResult.Status.FAIL,
                        "validator.kafka.id-set.missing-ids"));

        assertEquals(ScenarioVerdict.Status.FAIL, missed.status());
        assertEquals(ScenarioVerdict.EXPECTATION_MISMATCH, missed.reason());
        assertTrue(missed.message().contains("but the attempt passed"), missed.message());
        assertEquals(ScenarioVerdict.Status.FAIL, otherDefect.status());
        assertEquals(ScenarioVerdict.EXPECTATION_MISMATCH, otherDefect.reason());
        assertTrue(otherDefect.message().contains("missing-ids"), otherDefect.message());
    }

    @Test
    void inconclusiveAndVerificationResultsNeverMatchAnyExpectation() {
        for (ExpectedOutcome expected : List.of(ExpectedOutcome.pass(), EXPECT_DUPLICATES)) {
            ScenarioVerdict unevaluated = ScenarioVerdict.of(expected, attempt(
                    V1ScenarioExecutionResult.Status.INCONCLUSIVE,
                    "taskmanager.kill.effect-unconfirmed"));
            ScenarioVerdict unverified = ScenarioVerdict.of(expected, attempt(
                    V1ScenarioExecutionResult.Status.FAIL,
                    "verification.kafka.incomplete-after-timeout"));

            assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, unevaluated.status());
            assertFalse(unevaluated.matched());
            assertEquals(ScenarioVerdict.Status.FAIL, unverified.status());
            assertEquals("verification.kafka.incomplete-after-timeout", unverified.reason());
            assertFalse(unverified.matched());
        }
    }

    private static V1ScenarioExecutionResult attempt(
            V1ScenarioExecutionResult.Status status,
            String reason) {
        if (status == V1ScenarioExecutionResult.Status.PASS) {
            return passingAttempt();
        }
        return new V1ScenarioExecutionResult(
                status,
                reason,
                "attempt message",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                List.of());
    }

    /** A pass needs fence and oracle evidence (see the result's invariants). */
    private static V1ScenarioExecutionResult passingAttempt() {
        FlinkProcessWriteFenceEvidence processes =
                new FlinkProcessWriteFenceEvidence(List.of(), Instant.EPOCH);
        FlinkJobObservation.Attempt finished = new FlinkJobObservation.Attempt(
                Optional.of(new FlinkJobObservation(
                        1_000, FlinkJobState.FINISHED, 1, 0, Optional.empty(),
                        List.of(), List.of())),
                Optional.empty());
        KafkaIdSetValidationResult oracle = new KafkaIdSetValidationResult(
                KafkaIdSetValidationResult.Status.PASS,
                "validator.kafka.id-set.match",
                "Exact terminal ID set matched",
                new KafkaIdSetValidationResult.Evidence(
                        10, 10,
                        Optional.of(new KafkaIdSetValidationResult.DefectTotals(
                                10, 0, 0, 0, 0)),
                        List.of(), List.of(), List.of(), List.of(), List.of(),
                        Map.of(0, 0L), Map.of(0, 10L), true));
        return new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.PASS,
                oracle.reason(),
                "attempt message",
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FlinkTerminalWriteFence.Evidence(
                        FlinkJobState.FINISHED, processes, finished)),
                Optional.of(processes),
                Optional.of(finished),
                Optional.of(oracle),
                Optional.empty(),
                List.of(),
                List.of());
    }
}
