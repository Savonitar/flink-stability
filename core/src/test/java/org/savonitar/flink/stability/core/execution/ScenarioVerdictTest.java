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
import java.util.OptionalLong;

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

    @Test
    void aFailureReasonAloneDoesNotProveAnExpectedFailure() {
        V1ScenarioExecutionResult unsupported = new V1ScenarioExecutionResult(
                V1ScenarioExecutionResult.Status.FAIL, DUPLICATES, "unverified failure",
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), List.of(), List.of());

        ScenarioVerdict verdict = ScenarioVerdict.of(EXPECT_DUPLICATES, unsupported);

        assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
        assertEquals(ScenarioVerdict.EVIDENCE_UNCONFIRMED, verdict.reason());
        assertFalse(verdict.matched());
        assertEquals(V1ScenarioExecutionResult.Status.FAIL, unsupported.status());
        assertEquals(DUPLICATES, unsupported.reason());
    }

    @Test
    void matchingFailuresNeedSuccessfulPhasesBothFencesAndACompletePinnedOracle() {
        V1ScenarioExecutionResult complete = attempt(V1ScenarioExecutionResult.Status.FAIL, DUPLICATES);
        PhaseExecutionEvidence failedPhase = new PhaseExecutionEvidence(List.of(
                new PhaseExecutionEvidence.StepEvidence(0, "fault", "$/phases/0/steps/0",
                        List.of(), PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER,
                        PhaseExecutionEvidence.StepStatus.FAILED, "kill unavailable")));
        for (String missing : List.of("phases", "successful-phases", "write-fence",
                "both-fences", "oracle", "complete-snapshot", "pinned-reason", "failing-oracle")) {
            Optional<PhaseExecutionEvidence> phases = switch (missing) {
                case "phases" -> Optional.empty();
                case "successful-phases" -> Optional.of(failedPhase);
                default -> complete.phaseEvidence();
            };
            Optional<KafkaIdSetValidationResult> oracle = switch (missing) {
                case "oracle", "both-fences" -> Optional.empty();
                case "complete-snapshot" -> Optional.of(new KafkaIdSetValidationResult(
                        KafkaIdSetValidationResult.Status.FAIL, DUPLICATES, "partial",
                        KafkaIdSetValidationResult.Evidence.unavailable(10)));
                case "pinned-reason" -> Optional.of(new KafkaIdSetValidationResult(
                        KafkaIdSetValidationResult.Status.FAIL, "validator.kafka.id-set.missing-ids",
                        "different anomaly", complete.terminalValidation().orElseThrow().evidence()));
                case "failing-oracle" -> Optional.of(new KafkaIdSetValidationResult(
                        KafkaIdSetValidationResult.Status.PASS, DUPLICATES, "not a failure",
                        complete.terminalValidation().orElseThrow().evidence()));
                default -> complete.terminalValidation();
            };
            V1ScenarioExecutionResult invalid = new V1ScenarioExecutionResult(
                    complete.status(), complete.reason(), complete.message(), complete.inputManifest(),
                    phases, missing.endsWith("fence") || missing.equals("both-fences")
                            ? Optional.empty() : complete.writeFenceEvidence(),
                    missing.equals("both-fences") ? Optional.empty() : complete.processFenceEvidence(),
                    complete.finalJobObservation(), oracle, complete.sinkTransactions(),
                    complete.flinkProvisioningEvidence(), complete.diagnostics());

            ScenarioVerdict verdict = ScenarioVerdict.of(EXPECT_DUPLICATES, invalid);

            assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status(), missing);
            assertEquals(ScenarioVerdict.EVIDENCE_UNCONFIRMED, verdict.reason(), missing);
            assertFalse(verdict.matched(), missing);
            assertEquals(V1ScenarioExecutionResult.Status.FAIL, invalid.status(), missing);
        }
    }

    @Test
    void aMatchingFailureCannotPassWhenItsKillHitsAFinishedJob() {
        FlinkJobObservation.Attempt finished = new FlinkJobObservation.Attempt(
                Optional.of(new FlinkJobObservation(500, FlinkJobState.FINISHED,
                        1, 0, Optional.empty(), List.of(), List.of())), Optional.empty());
        PhaseExecutionEvidence phases = new PhaseExecutionEvidence(List.of(), List.of(
                new PhaseExecutionEvidence.TaskManagerKill("$/phases/0/steps/0", List.of(),
                        "taskmanager-1", finished, OptionalLong.of(600))));
        V1ScenarioExecutionResult attempt = terminalAttempt(
                V1ScenarioExecutionResult.Status.FAIL, DUPLICATES, phases);

        ScenarioVerdict verdict = ScenarioVerdict.of(EXPECT_DUPLICATES, attempt);

        assertEquals(ScenarioVerdict.Status.INCONCLUSIVE, verdict.status());
        assertEquals(ExecutablePhaseExecutor.TASKMANAGER_KILL_EFFECT_UNCONFIRMED, verdict.reason());
        assertFalse(verdict.matched());
        assertEquals(V1ScenarioExecutionResult.Status.FAIL, attempt.status());
    }

    private static V1ScenarioExecutionResult attempt(
            V1ScenarioExecutionResult.Status status,
            String reason) {
        if (status == V1ScenarioExecutionResult.Status.PASS
                || reason.startsWith("validator.kafka.id-set.")) {
            return terminalAttempt(status, reason, new PhaseExecutionEvidence(List.of()));
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

    /** Both positive and negative controls need a completed experiment. */
    private static V1ScenarioExecutionResult terminalAttempt(
            V1ScenarioExecutionResult.Status status,
            String reason,
            PhaseExecutionEvidence phases) {
        FlinkProcessWriteFenceEvidence processes =
                new FlinkProcessWriteFenceEvidence(List.of(), Instant.EPOCH);
        FlinkJobObservation.Attempt finished = new FlinkJobObservation.Attempt(
                Optional.of(new FlinkJobObservation(
                        1_000, FlinkJobState.FINISHED, 1, 0, Optional.empty(),
                        List.of(), List.of())),
                Optional.empty());
        KafkaIdSetValidationResult oracle = new KafkaIdSetValidationResult(
                status == V1ScenarioExecutionResult.Status.PASS
                        ? KafkaIdSetValidationResult.Status.PASS
                        : KafkaIdSetValidationResult.Status.FAIL,
                reason,
                "Terminal ID set checked",
                new KafkaIdSetValidationResult.Evidence(
                        10, 10,
                        Optional.of(new KafkaIdSetValidationResult.DefectTotals(
                                10, 0, 0, 0, 0)),
                        List.of(), List.of(), List.of(), List.of(), List.of(),
                        Map.of(0, 0L), Map.of(0, 10L), true));
        return new V1ScenarioExecutionResult(
                status,
                oracle.reason(),
                "attempt message",
                Optional.empty(),
                Optional.of(phases),
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
