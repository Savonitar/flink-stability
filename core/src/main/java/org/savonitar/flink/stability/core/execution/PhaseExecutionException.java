package org.savonitar.flink.stability.core.execution;

import java.util.List;
import java.util.Objects;

/** Typed FAIL or INCONCLUSIVE result from one attempted phase step. */
public final class PhaseExecutionException extends Exception {
    private final Outcome outcome;
    private final String reason;
    private final String path;
    private final List<PhaseExecutionEvidence.LoopIteration> loopIterations;
    private final PhaseExecutionEvidence evidence;

    PhaseExecutionException(
            Outcome outcome,
            String reason,
            String path,
            List<PhaseExecutionEvidence.LoopIteration> loopIterations,
            PhaseExecutionEvidence evidence,
            Throwable cause) {
        super(message(outcome, reason, path), Objects.requireNonNull(cause, "cause"));
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.reason = requireNonBlank(reason, "reason");
        this.path = requireNonBlank(path, "path");
        this.loopIterations = List.copyOf(Objects.requireNonNull(
                loopIterations, "loopIterations"));
        this.evidence = Objects.requireNonNull(evidence, "evidence");
        if (evidence.steps().isEmpty()) {
            throw new IllegalArgumentException("Failure evidence must include the failed step");
        }
        PhaseExecutionEvidence.StepEvidence failed = evidence.steps().getLast();
        if (failed.status() != PhaseExecutionEvidence.StepStatus.FAILED
                || !failed.path().equals(path)
                || !failed.loopIterations().equals(this.loopIterations)) {
            throw new IllegalArgumentException(
                    "The final evidence entry must identify the failed execution context");
        }
    }

    public Outcome outcome() {
        return outcome;
    }

    public String reason() {
        return reason;
    }

    public String path() {
        return path;
    }

    public List<PhaseExecutionEvidence.LoopIteration> loopIterations() {
        return loopIterations;
    }

    public PhaseExecutionEvidence evidence() {
        return evidence;
    }

    private static String message(
            Outcome outcome,
            String reason,
            String path) {
        return "Phase execution " + Objects.requireNonNull(outcome, "outcome")
                + " at " + requireNonBlank(path, "path")
                + ": " + requireNonBlank(reason, "reason");
    }

    public enum Outcome {
        FAIL,
        INCONCLUSIVE
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
