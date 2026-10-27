package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.PreparedExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.util.ArrayList;
import java.util.List;

/** State progress proof is separate from, and can never erase, the independent data verdict. */
public record StateRecoveryReport(String jobId, String workloadSha256, int keyCount, long totalRecords,
        int expectedRestarts, boolean physicalFenceConfirmed, StateRecoveryEvidence.Trace trace,
        List<StateRecoveryEvidence.Readiness> readinessAttempts,
        List<StateRecoveryEvidence.Recovery> recoveries, List<String> observationErrors,
        boolean confirmed, String reason) {
    public static final String UNCONFIRMED = "state-recovery.evidence-unconfirmed";

    public StateRecoveryReport {
        readinessAttempts = List.copyOf(readinessAttempts);
        recoveries = List.copyOf(recoveries);
        observationErrors = List.copyOf(observationErrors);
        if (confirmed && !proofConfirmed(jobId, keyCount, totalRecords, expectedRestarts,
                physicalFenceConfirmed, trace, recoveries, observationErrors))
            throw new IllegalArgumentException("Confirmed recovery requires complete window and restore proof");
    }

    static StateRecoveryReport collect(PreparedExecutableScenarioPlan prepared, String jobId,
            V1ScenarioExecutionResult result, V1AttemptRuntime runtime, StateRecoveryGate gate) {
        var plan = prepared.executablePlan();
        int keys = plan.job().workloadConfiguration().behavior().orElseThrow().keyCount();
        int expected = countKills(plan.phases().stream().flatMap(phase -> phase.steps().stream()).toList());
        boolean fenced = result.writeFenceEvidence().isPresent() && V1ScenarioExecutor.confirmedPhysicalFence(result);
        var errors = new ArrayList<String>();
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
                expected, fenced, trace, gate == null ? List.of() : gate.attempts(), recoveries, errors);
    }

    static StateRecoveryReport evaluate(String jobId, String workloadSha256, int keys, long totalRecords,
            int expected, boolean fenced, StateRecoveryEvidence.Trace trace,
            List<StateRecoveryEvidence.Readiness> readinessAttempts,
            List<StateRecoveryEvidence.Recovery> recoveries, List<String> errors) {
        boolean confirmed = proofConfirmed(jobId, keys, totalRecords, expected, fenced, trace, recoveries, errors);
        return new StateRecoveryReport(jobId, workloadSha256, keys, totalRecords,
                expected, fenced, trace, readinessAttempts, recoveries, errors,
                confirmed, confirmed ? "state-recovery.confirmed" : UNCONFIRMED);
    }

    private static boolean proofConfirmed(String jobId, int keys, long totalRecords, int expected,
            boolean fenced, StateRecoveryEvidence.Trace trace,
            List<StateRecoveryEvidence.Recovery> recoveries, List<String> errors) {
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

    private static int countKills(List<ExecutableScenarioPlan.Step> steps) {
        int count = 0;
        for (var step : steps) {
            if (step instanceof ExecutableScenarioPlan.KillTaskManager) count++;
            else if (step instanceof ExecutableScenarioPlan.Loop loop) count += loop.times() * countKills(loop.steps());
        }
        return count;
    }
}
