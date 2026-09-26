package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.validation.kafka.KafkaIdSetValidationResult;

import java.util.Objects;

/**
 * The scenario verdict of the first runner: its single attempt result compared with the selected
 * expectation (SPEC-001 R8.7 without an experiment, SPEC-002 E4). An expected failure that
 * occurs with its pinned reason is a {@code pass}; a negative control is green when it fails
 * exactly as pinned with complete, confirmed experiment evidence.
 */
public record ScenarioVerdict(Status status, String reason, String message, boolean matched) {
    /** The attempt contradicted the selected expectation (SPEC-002 E4.5). */
    public static final String EXPECTATION_MISMATCH = "expectation.mismatch";
    /** A matching failure without the evidence needed to validate the experiment. */
    public static final String EVIDENCE_UNCONFIRMED = "expectation.evidence-unconfirmed";

    public enum Status {
        PASS,
        FAIL,
        INCONCLUSIVE
    }

    public ScenarioVerdict {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(message, "message");
        if (matched != (status == Status.PASS)) {
            throw new IllegalArgumentException("Only a matched expectation passes");
        }
    }

    public static ScenarioVerdict of(
            ExecutableScenarioPlan.ExpectedOutcome expected,
            V1ScenarioExecutionResult attempt) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(attempt, "attempt");
        // E4.2: an inconclusive attempt is never matched either way.
        if (attempt.status() == V1ScenarioExecutionResult.Status.INCONCLUSIVE) {
            return new ScenarioVerdict(
                    Status.INCONCLUSIVE, attempt.reason(), attempt.message(), false);
        }
        // R7.1a: a failed verification is never an expectation match, not even for a failure.
        if (attempt.reason().startsWith("verification.")) {
            return new ScenarioVerdict(Status.FAIL, attempt.reason(), attempt.message(), false);
        }
        boolean attemptPassed = attempt.status() == V1ScenarioExecutionResult.Status.PASS;
        if (expected.outcome() == ExecutableScenarioPlan.ExpectedOutcome.Outcome.PASS) {
            return attemptPassed
                    ? new ScenarioVerdict(Status.PASS, attempt.reason(), attempt.message(), true)
                    : new ScenarioVerdict(
                            Status.FAIL, attempt.reason(), attempt.message(), false);
        }
        String expectedReason = expected.reason().orElseThrow();
        if (!attemptPassed && attempt.reason().equals(expectedReason)) {
            return matchingFailure(attempt);
        }
        return new ScenarioVerdict(Status.FAIL, EXPECTATION_MISMATCH,
                "Expected " + expected.oracle().orElseThrow() + " to fail with "
                        + expectedReason + ", but the attempt "
                        + (attemptPassed ? "passed" : "failed with " + attempt.reason())
                        + ": " + attempt.message(),
                false);
    }

    /** Keep the observed data failure, but do not bless an invalid negative control. */
    private static ScenarioVerdict matchingFailure(V1ScenarioExecutionResult attempt) {
        if (attempt.phaseEvidence().isEmpty()
                || attempt.phaseEvidence().orElseThrow().steps().stream().anyMatch(step ->
                        step.status() == PhaseExecutionEvidence.StepStatus.FAILED)
                || attempt.writeFenceEvidence().isEmpty()
                || attempt.processFenceEvidence().isEmpty()
                || attempt.terminalValidation().filter(validation ->
                        validation.status() == KafkaIdSetValidationResult.Status.FAIL
                                && validation.reason().equals(attempt.reason())
                                && validation.evidence().snapshotComplete()).isEmpty()) {
            return unconfirmed(EVIDENCE_UNCONFIRMED,
                    "The expected failure lacks complete phase, fence, or oracle evidence");
        }
        SubjectClassOrigins.Outcome origins = attempt.subjectClassOrigins()
                .map(evidence -> evidence.outcome(
                        ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES))
                .orElse(SubjectClassOrigins.Outcome.UNCONFIRMED);
        if (origins != SubjectClassOrigins.Outcome.CONFIRMED) {
            return unconfirmed(origins == SubjectClassOrigins.Outcome.MISMATCH
                            ? V1ScenarioExecutor.SUBJECT_ORIGIN_MISMATCH
                            : V1ScenarioExecutor.SUBJECT_ORIGIN_UNCONFIRMED,
                    "The expected failure does not prove that the subject connector ran");
        }
        for (TaskManagerKillEffect effect : attempt.taskManagerKillEffects()) {
            if (!effect.outcome().confirmed()) {
                return unconfirmed(ExecutablePhaseExecutor.TASKMANAGER_KILL_EFFECT_UNCONFIRMED,
                        "The expected failure occurred, but the kill at " + effect.kill().path()
                                + " has unconfirmed effect: " + effect.outcome());
            }
        }
        for (PhaseExecutionEvidence.NetworkFault fault :
                attempt.phaseEvidence().orElseThrow().networkFaults()) {
            if (!fault.triggered()) {
                return unconfirmed(ExecutablePhaseExecutor.NETWORK_FAULT_TRIGGER_MISSED,
                        "The expected failure occurred, but the network fault at " + fault.path()
                                + " did not complete its requested occurrences in time");
            }
        }
        return new ScenarioVerdict(Status.PASS, attempt.reason(),
                "The expected failure occurred: " + attempt.message(), true);
    }

    private static ScenarioVerdict unconfirmed(String reason, String message) {
        return new ScenarioVerdict(Status.INCONCLUSIVE, reason, message, false);
    }
}
