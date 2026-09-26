package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.core.execution.ScenarioVerdict;
import org.savonitar.flink.stability.core.execution.TaskManagerKillEffect;
import org.savonitar.flink.stability.core.execution.V1AttemptContext;
import org.savonitar.flink.stability.core.execution.V1ScenarioExecutionResult;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Renders one stable, machine-readable summary without dumping record-level evidence. */
final class V1ExecutionResultRenderer {
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Renders the scenario verdict at the top level and the attempt result under
     * {@code attempt}. They differ only when the expectation is a failure.
     */
    String render(
            String scenarioName,
            V1AttemptContext context,
            ExecutableScenarioPlan.ExpectedOutcome expected,
            V1ScenarioExecutionResult result) {
        Objects.requireNonNull(scenarioName, "scenarioName");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(result, "result");
        ScenarioVerdict verdict = ScenarioVerdict.of(expected, result);

        ObjectNode root = JSON.createObjectNode();
        root.put("scenario", scenarioName);
        ObjectNode attempt = root.putObject("attempt");
        attempt.put("ordinal", context.attemptOrdinal());
        attempt.put("nonce", context.attemptNonce8());
        attempt.put("checkpointRoot", context.checkpointStorageRoot().toString());
        attempt.put("status", result.status().name().toLowerCase(Locale.ROOT));
        attempt.put("reason", result.reason());
        attempt.put("message", result.message());
        root.put("status", verdict.status().name().toLowerCase(Locale.ROOT));
        root.put("reason", verdict.reason());
        root.put("message", verdict.message());
        ObjectNode expectation = root.putObject("expectation");
        expectation.put("outcome", expected.outcome().name().toLowerCase(Locale.ROOT));
        expected.oracle().ifPresent(oracle -> expectation.put("oracle", oracle));
        expected.reason().ifPresent(reason -> expectation.put("reason", reason));
        expectation.put("matched", verdict.matched());

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
        ObjectNode flinkJob = evidence.putObject("flinkJob");
        flinkJob.put("status", "not-run");
        result.finalJobObservation().ifPresent(observed -> {
            observed.failure().ifPresent(failure -> {
                flinkJob.put("status", "unavailable");
                flinkJob.put("failure", failure);
            });
            observed.observation().ifPresent(job -> {
                flinkJob.put("status", "observed");
                flinkJob.put("state", job.state().name());
                flinkJob.put("completedCheckpoints", job.completedCheckpoints());
                flinkJob.put("restoredCheckpoints", job.restoredCheckpoints());
                job.latestRestore().ifPresent(restore ->
                        flinkJob.put("latestRestoredCheckpoint", restore.checkpointId()));
                flinkJob.put("failures", job.failures().size());
                if (!job.failures().isEmpty()) {
                    flinkJob.put("latestFailure", job.failures().getFirst().rootCause());
                }
            });
        });
        ArrayNode kills = evidence.putArray("taskManagerKills");
        for (TaskManagerKillEffect effect : result.taskManagerKillEffects()) {
            ObjectNode kill = kills.addObject();
            kill.put("path", effect.kill().path());
            ArrayNode iterations = kill.putArray("loopIterations");
            effect.kill().loopIterations().forEach(iteration -> iterations.add(
                    iteration.iteration() + "/" + iteration.totalIterations()));
            kill.put("target", effect.kill().target());
            kill.put("outcome", effect.outcome().name().toLowerCase(Locale.ROOT)
                    .replace('_', '-'));
            kill.put("confirmed", effect.outcome().confirmed());
            effect.kill().jobBeforeKill().observation().ifPresent(before -> {
                kill.put("jobStateBeforeKill", before.state().name());
                kill.put("completedCheckpointsBeforeKill", before.completedCheckpoints());
                kill.put("activeSubtasksBeforeKill", before.activeSubtasks().size());
                effect.restore().ifPresent(restore -> {
                    kill.put("restoredCheckpoint", restore.checkpointId());
                    effect.kill().jobManagerTimeAfterKill().ifPresent(sample ->
                            kill.put("restoredAfterKillObservationMs",
                                    restore.restoredAtMillis() - sample));
                });
            });
            effect.kill().jobManagerTimeAfterKill().ifPresent(sample ->
                    kill.put("jobManagerTimeAfterKill", sample));
            kill.put("failuresAfterKill", effect.failuresAfterKill().size());
            if (!effect.failuresAfterKill().isEmpty()) {
                kill.put("firstFailureAfterKill",
                        effect.failuresAfterKill().getLast().rootCause());
            }
            kill.put("detail", effect.detail());
        }
        ArrayNode networkFaults = evidence.putArray("networkFaults");
        result.phaseEvidence().ifPresent(phases -> phases.networkFaults().forEach(fault -> {
            ObjectNode rendered = networkFaults.addObject();
            rendered.put("path", fault.path());
            rendered.put("faultId", fault.faultId());
            rendered.put("proxy", fault.proxy());
            rendered.put("proxyImage", fault.proxyImage());
            rendered.put("action", fault.action().name().toLowerCase(Locale.ROOT)
                    .replace('_', '-'));
            rendered.put("occurrences", fault.occurrences());
            rendered.put("triggered", fault.triggered());
            rendered.put("triggerDeadline", fault.triggerDeadline().toString());
            rendered.put("armedAtMillis", fault.armedAtMillis());
            rendered.put("healedAtMillis", fault.healedAtMillis());
            ArrayNode forwardedErrors = rendered.putArray("forwardedErrors");
            fault.forwardedErrors().forEach(forwardedErrors::add);
            ArrayNode dropped = rendered.putArray("dropped");
            fault.dropped().forEach(message -> {
                ObjectNode drop = dropped.addObject();
                drop.put("occurrence", message.occurrence());
                drop.put("claim", message.claim());
                drop.put("droppedAtMillis", message.droppedAtMillis());
                drop.put("beforeDeadline", message.beforeDeadline());
                drop.put("transactionalId", message.transactionalId());
                drop.put("producerId", message.producerId());
                drop.put("producerEpoch", message.producerEpoch());
                drop.put("committed", message.committed());
                message.brokerAnswer().ifPresent(answer -> {
                    drop.put("brokerError", answer.error());
                    drop.put("brokerProducerEpoch", answer.producerEpoch());
                });
                message.retry().ifPresent(retry -> {
                    drop.put("retryObservedAtMillis", retry.observedAtMillis());
                    drop.put("retryAfterMillis",
                            retry.observedAtMillis() - message.droppedAtMillis());
                    drop.put("retryClientId", retry.clientId());
                });
            });
        }));
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
        ObjectNode subjectClasses = evidence.putObject("subjectClasses");
        subjectClasses.put("status", "not-run");
        result.subjectClassOrigins().ifPresent(origins -> {
            List<String> entryClasses = ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES;
            subjectClasses.put("status", origins.outcome(entryClasses).name()
                    .toLowerCase(Locale.ROOT));
            subjectClasses.put("expectedSource", origins.expectedSource());
            subjectClasses.put("detail", origins.detail(entryClasses));
            ArrayNode processes = subjectClasses.putArray("processes");
            origins.processes().forEach(process -> {
                ObjectNode loaded = processes.addObject();
                loaded.put("process", process.process());
                ObjectNode sources = loaded.putObject("sources");
                process.sources().forEach((entryClass, found) -> {
                    ArrayNode paths = sources.putArray(entryClass);
                    found.forEach(paths::add);
                });
            });
        });
        ObjectNode transactions = evidence.putObject("sinkTransactions");
        transactions.put("status", "not-listed");
        result.sinkTransactions().ifPresent(listing -> {
            transactions.put("status", "listed");
            transactions.put("transactionalIdPrefix", listing.transactionalIdPrefix());
            transactions.put("total", listing.transactions().size());
            ArrayNode unresolved = transactions.putArray("unresolved");
            listing.unresolved().forEach(transaction -> {
                ObjectNode open = unresolved.addObject();
                open.put("transactionalId", transaction.transactionalId());
                open.put("state", transaction.state());
                open.put("producerId", transaction.producerId());
                open.put("producerEpoch", transaction.producerEpoch());
                ArrayNode partitions = open.putArray("partitions");
                transaction.topicPartitions().forEach(partitions::add);
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
