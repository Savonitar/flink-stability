package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.PreparedExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** State progress proof is separate from, and can never erase, the independent data verdict. */
public record StateRecoveryReport(String jobId, String workloadSha256, int keyCount, long totalRecords,
        int expectedRestarts, boolean physicalFenceConfirmed, StateRecoveryEvidence.Trace trace,
        List<StateRecoveryEvidence.Readiness> readinessAttempts,
        List<StateRecoveryEvidence.Recovery> recoveries, List<String> observationErrors,
        boolean confirmed, String reason, boolean allowStateResetMutant, Optional<String> buildMode) {
    public static final String UNCONFIRMED = "state-recovery.evidence-unconfirmed";
    public static final String UNEXPECTED_BUILD_MODE = "state-recovery.unexpected-build-mode";
    private static final String MUTANT_MODE = "reset-state-on-restore-v1";

    public StateRecoveryReport(String jobId, String workloadSha256, int keyCount, long totalRecords,
            int expectedRestarts, boolean physicalFenceConfirmed, StateRecoveryEvidence.Trace trace,
            List<StateRecoveryEvidence.Readiness> readinessAttempts,
            List<StateRecoveryEvidence.Recovery> recoveries, List<String> observationErrors,
            boolean confirmed, String reason) {
        this(jobId, workloadSha256, keyCount, totalRecords, expectedRestarts, physicalFenceConfirmed, trace,
                readinessAttempts, recoveries, observationErrors, confirmed, reason, false, observedBuildMode(trace));
    }

    public StateRecoveryReport {
        readinessAttempts = List.copyOf(readinessAttempts);
        recoveries = List.copyOf(recoveries);
        observationErrors = List.copyOf(observationErrors);
        if (!observedBuildMode(trace).equals(buildMode))
            throw new IllegalArgumentException("Build-mode summary differs from workload START receipts");
        if (unexpectedBuildMode(trace, allowStateResetMutant)) reason = UNEXPECTED_BUILD_MODE;
        if (confirmed && !proofConfirmed(jobId, keyCount, totalRecords, expectedRestarts,
                physicalFenceConfirmed, trace, recoveries, observationErrors, allowStateResetMutant))
            throw new IllegalArgumentException("Confirmed recovery requires complete window and restore proof");
    }

    static StateRecoveryReport collect(PreparedExecutableScenarioPlan prepared, String jobId,
            V1ScenarioExecutionResult result, V1AttemptRuntime runtime, StateRecoveryGate gate) {
        var plan = prepared.executablePlan();
        int keys = plan.job().workloadConfiguration().behavior().orElseThrow().keyCount();
        int expected = result.phaseEvidence().map(StateRecoveryReport::countKills).orElse(0);
        boolean fenced = result.writeFenceEvidence().isPresent() && V1ScenarioExecutor.confirmedPhysicalFence(result);
        var errors = new ArrayList<String>();
        if (result.phaseEvidence().isEmpty()) errors.add("Phase execution evidence is unavailable");
        result.phaseEvidence().ifPresent(phases -> phases.steps().stream()
                .filter(step -> step.status() != PhaseExecutionEvidence.StepStatus.SUCCEEDED)
                .forEach(step -> errors.add("Phase " + step.status() + " at " + step.path() + ": " + step.detail())));
        if (gate != null) errors.addAll(gate.failures());
        StateRecoveryEvidence.Trace trace;
        try {
            trace = runtime == null ? new StateRecoveryEvidence.Trace(List.of(), List.of("Runtime unavailable"), false)
                    : fenced ? StateRecoveryEvidence.collectFenced(runtime.flinkProvisioningEvidence(), runtime.flinkComponentLogs(), jobId)
                    : StateRecoveryEvidence.collect(runtime.flinkProvisioningEvidence(), runtime.flinkComponentLogs(), jobId);
        } catch (RuntimeException failure) {
            trace = new StateRecoveryEvidence.Trace(List.of(), List.of("Capture failed: " + failure), false);
        }
        var recoveries = new ArrayList<StateRecoveryEvidence.Recovery>();
        for (var effect : result.taskManagerKillEffects()) {
            if (effect.kill().stateReadiness().isEmpty()) errors.add("Kill lacks bound state readiness: " + effect.kill().path());
            else recoveries.add(StateRecoveryEvidence.evaluate(effect.kill().stateReadiness().orElseThrow(), effect, trace));
        }
        return evaluate(jobId, prepared.workloadArtifact().sha256(), keys, plan.input().totalRecords(),
                expected, fenced, trace, gate == null ? List.of() : gate.attempts(), recoveries, errors,
                plan.allowStateResetMutant());
    }

    static StateRecoveryReport evaluate(String jobId, String workloadSha256, int keys, long totalRecords,
            int expected, boolean fenced, StateRecoveryEvidence.Trace trace,
            List<StateRecoveryEvidence.Readiness> readinessAttempts,
            List<StateRecoveryEvidence.Recovery> recoveries, List<String> errors) {
        return evaluate(jobId, workloadSha256, keys, totalRecords, expected, fenced, trace, readinessAttempts,
                recoveries, errors, false);
    }

    static StateRecoveryReport evaluate(String jobId, String workloadSha256, int keys, long totalRecords,
            int expected, boolean fenced, StateRecoveryEvidence.Trace trace,
            List<StateRecoveryEvidence.Readiness> readinessAttempts,
            List<StateRecoveryEvidence.Recovery> recoveries, List<String> errors, boolean allowStateResetMutant) {
        boolean confirmed = proofConfirmed(jobId, keys, totalRecords, expected, fenced, trace, recoveries, errors,
                allowStateResetMutant);
        return new StateRecoveryReport(jobId, workloadSha256, keys, totalRecords,
                expected, fenced, trace, readinessAttempts, recoveries, errors,
                confirmed, confirmed ? "state-recovery.confirmed" : UNCONFIRMED,
                allowStateResetMutant, observedBuildMode(trace));
    }

    private static boolean proofConfirmed(String jobId, int keys, long totalRecords, int expected,
            boolean fenced, StateRecoveryEvidence.Trace trace,
            List<StateRecoveryEvidence.Recovery> recoveries, List<String> errors, boolean allowStateResetMutant) {
        if (unexpectedBuildMode(trace, allowStateResetMutant)) return false;
        if (expected < 0 || keys < 1 || keys > 1024 || totalRecords < 1 || !fenced
                || !trace.complete() || !errors.isEmpty() || recoveries.size() != expected
                || recoveries.stream().anyMatch(value -> !value.confirmed())) return false;
        if (trace.markers().stream().anyMatch(marker -> !marker.jobId().equals(jobId)
                || !marker.operatorUid().equals(StateRecoveryEvidence.OPERATOR_UID))) return false;
        var starts = trace.markers().stream().filter(marker -> marker.event() == StateRecoveryEvidence.Event.START).toList();
        boolean startsBound = starts.size() == expected + 1
                && starts.stream().filter(marker -> marker.restoredCheckpointId() == -1 && marker.executionAttempt() == 0).count() == 1
                && starts.stream().allMatch(marker -> marker.buildMode()
                        .filter(mode -> mode.equals("correct") || mode.equals("reset-state-on-restore-v1")).isPresent())
                && starts.stream().map(marker -> marker.buildMode().orElse("")).distinct().count() == 1;
        boolean smallUninterrupted = expected == 0 && totalRecords < keys;
        boolean progress = trace.markers().stream().anyMatch(marker -> marker.event() == (smallUninterrupted
                        ? StateRecoveryEvidence.Event.SNAPSHOT : StateRecoveryEvidence.Event.PROGRESS)
                && marker.progress().filter(value -> value.keyCount() == keys
                        && value.observedKeys() == Math.min(keys, totalRecords) && value.minimumObservedCount() > 0
                        && (smallUninterrupted ? value.processedThisAttempt() >= totalRecords
                                && value.maximumInputId() == totalRecords - 1
                                : value.processedThisAttempt() >= keys && value.maximumInputId() >= keys - 1
                                        && value.maximumInputId() < totalRecords)).isPresent()
                && starts.stream().anyMatch(start -> start.process().equals(marker.process())
                        && start.executionAttempt() == marker.executionAttempt() && start.subtask() == marker.subtask()
                        && start.restoredCheckpointId() == marker.restoredCheckpointId()
                        && start.lineNumber() < marker.lineNumber()));
        boolean uninterruptedClean = expected != 0 || trace.markers().stream().noneMatch(marker ->
                marker.event() == StateRecoveryEvidence.Event.MUTANT_RESET || marker.event() == StateRecoveryEvidence.Event.RESTORED_KEY);
        return startsBound && progress && uninterruptedClean;
    }

    private static boolean unexpectedBuildMode(StateRecoveryEvidence.Trace trace, boolean allowStateResetMutant) {
        return !allowStateResetMutant && trace.markers().stream().anyMatch(marker ->
                marker.event() == StateRecoveryEvidence.Event.START && marker.buildMode().filter(MUTANT_MODE::equals).isPresent());
    }

    private static Optional<String> observedBuildMode(StateRecoveryEvidence.Trace trace) {
        var modes = trace.markers().stream().filter(marker -> marker.event() == StateRecoveryEvidence.Event.START)
                .map(StateRecoveryEvidence.Marker::buildMode).distinct().toList();
        return modes.size() == 1 ? modes.getFirst() : Optional.empty();
    }

    static int countKills(PhaseExecutionEvidence phases) {
        int count = 0;
        for (var step : phases.steps()) {
            boolean kill = step.kind() == PhaseExecutionEvidence.StepKind.KILL_TASKMANAGER
                    || step.kind() == PhaseExecutionEvidence.StepKind.CHECKPOINT_STORAGE_FAULT;
            if (kill && step.status() == PhaseExecutionEvidence.StepStatus.SUCCEEDED) count++;
            else if (step.kind() == PhaseExecutionEvidence.StepKind.CHECKPOINT_STORAGE_FAULT
                    && step.status() == PhaseExecutionEvidence.StepStatus.UNCONFIRMED
                    && phases.taskManagerKills().stream().anyMatch(actual -> actual.path().equals(step.path())
                        && actual.loopIterations().equals(step.loopIterations()))) count++;
        }
        return count;
    }
}
