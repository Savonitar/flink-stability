package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;

import java.util.Objects;

/**
 * The scenario verdict of the first runner: its single attempt result compared with the selected
 * expectation (SPEC-001 R8.7 without an experiment, SPEC-002 E4). An expected failure that
 * occurs with its pinned reason is a {@code pass}; a negative control is green when it fails
 * exactly as pinned.
 */
public record ScenarioVerdict(Status status, String reason, String message, boolean matched) {
    /** The attempt contradicted the selected expectation (SPEC-002 E4.5). */
    public static final String EXPECTATION_MISMATCH = "expectation.mismatch";

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
            return new ScenarioVerdict(Status.PASS, attempt.reason(),
                    "The expected failure occurred: " + attempt.message(), true);
        }
        return new ScenarioVerdict(Status.FAIL, EXPECTATION_MISMATCH,
                "Expected " + expected.oracle().orElseThrow() + " to fail with "
                        + expectedReason + ", but the attempt "
                        + (attemptPassed ? "passed" : "failed with " + attempt.reason())
                        + ": " + attempt.message(),
                false);
    }
}
