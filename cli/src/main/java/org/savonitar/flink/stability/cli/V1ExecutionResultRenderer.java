package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.core.execution.V1AttemptContext;
import org.savonitar.flink.stability.core.execution.V1ScenarioExecutionResult;

import java.util.Locale;
import java.util.Objects;

/** Renders one stable, machine-readable summary without dumping record-level evidence. */
final class V1ExecutionResultRenderer {
    private static final ObjectMapper JSON = new ObjectMapper();

    String render(
            String scenarioName,
            V1AttemptContext context,
            V1ScenarioExecutionResult result) {
        Objects.requireNonNull(scenarioName, "scenarioName");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(result, "result");

        ObjectNode root = JSON.createObjectNode();
        root.put("scenario", scenarioName);
        ObjectNode attempt = root.putObject("attempt");
        attempt.put("ordinal", context.attemptOrdinal());
        attempt.put("nonce", context.attemptNonce8());
        attempt.put("checkpointRoot", context.checkpointStorageRoot().toString());
        root.put("status", result.status().name().toLowerCase(Locale.ROOT));
        root.put("reason", result.reason());
        root.put("message", result.message());

        ObjectNode evidence = root.putObject("evidence");
        ObjectNode input = evidence.putObject("input");
        input.put("status", "not-started");
        input.put("complete", false);
        input.put("reconciliationComplete", false);
        result.inputManifest().ifPresent(manifest -> {
            input.put("cluster", manifest.clusterAlias());
            input.put("topic", manifest.topic());
            input.put("records", manifest.totalRecords());
            input.put("partitions", manifest.exclusiveEndOffsets().size());
            input.put("status", manifest.evidenceStatus().name()
                    .toLowerCase(Locale.ROOT));
            input.put("complete", manifest.evidenceStatus().name().equals("COMPLETE"));
            input.put("observed", manifest.reconciliation().observedIds().size());
            input.put("reconciliationComplete",
                    manifest.reconciliation().reachedEveryExclusiveEnd());
        });
        ObjectNode phase = evidence.putObject("phases");
        phase.put("status", "not-run");
        phase.put("completed", false);
        phase.put("steps", 0);
        phase.put("succeeded", 0);
        phase.put("failed", 0);
        result.phaseEvidence().ifPresent(phases -> {
            long failed = phases.steps().stream()
                    .filter(step -> step.status()
                            == PhaseExecutionEvidence.StepStatus.FAILED)
                    .count();
            phase.put("status", failed == 0 ? "complete" : "partial");
            phase.put("completed", failed == 0);
            phase.put("steps", phases.steps().size());
            phase.put("succeeded", phases.steps().stream()
                    .filter(step -> step.status()
                            == PhaseExecutionEvidence.StepStatus.SUCCEEDED)
                    .count());
            phase.put("failed", failed);
        });
        ObjectNode writeFence = evidence.putObject("writeFence");
        writeFence.put("status", "not-run");
        writeFence.put("completed", false);
        result.writeFenceEvidence().ifPresent(fence -> {
            writeFence.put("status", "complete");
            writeFence.put("completed", true);
            writeFence.put("finalState", fence.finalState().name());
        });
        ObjectNode processFence = evidence.putObject("processFence");
        processFence.put("status", "not-run");
        processFence.put("completed", false);
        processFence.put("processes", 0);
        result.processFenceEvidence().ifPresent(fence -> {
            processFence.put("status", "complete");
            processFence.put("completed", true);
            processFence.put("processes", fence.components().size());
            processFence.put("completedAt", fence.completedAt().toString());
        });
        ObjectNode terminal = evidence.putObject("terminalValidation");
        terminal.put("status", "not-run");
        terminal.put("completed", false);
        terminal.put("snapshotComplete", false);
        result.terminalValidation().ifPresent(validation -> {
            terminal.put("status", validation.status().name().toLowerCase(Locale.ROOT));
            terminal.put("completed", true);
            terminal.put("reason", validation.reason());
            terminal.put("expected", validation.evidence().expectedCount());
            terminal.put("observed", validation.evidence().observedCount());
            terminal.put("snapshotComplete", validation.evidence().snapshotComplete());
            validation.evidence().defectTotals().ifPresent(totals -> {
                terminal.put("distinctExpected", totals.distinctExpectedCount());
                terminal.put("malformed", totals.malformedCount());
                terminal.put("unexpected", totals.unexpectedCount());
                terminal.put("duplicates", totals.duplicateCount());
                terminal.put("missing", totals.missingCount());
            });
        });
        evidence.put("flinkComponents", result.flinkProvisioningEvidence().size());

        ArrayNode diagnostics = root.putArray("diagnostics");
        result.diagnostics().forEach(diagnostics::add);
        try {
            return JSON.writeValueAsString(root);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not render scenario result", failure);
        }
    }
}
