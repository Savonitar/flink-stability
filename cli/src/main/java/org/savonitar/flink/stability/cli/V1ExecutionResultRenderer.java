package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.core.artifact.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.execution.ScenarioVerdict;
import org.savonitar.flink.stability.core.execution.TaskManagerKillEffect;
import org.savonitar.flink.stability.core.execution.V1AttemptContext;
import org.savonitar.flink.stability.core.execution.V1ScenarioExecutionResult;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.runtime.api.ProvisionedConnectorArtifact;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
        return render(scenarioName, context, expected, result, List.of());
    }

    String render(String scenarioName, V1AttemptContext context,
                  ExecutableScenarioPlan.ExpectedOutcome expected, V1ScenarioExecutionResult result,
                  List<PreparedScenarioPlan.ConnectorPrimaryEvidence> connectorPrimaries) {
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
        ArrayNode primaries = evidence.putArray("connectorPrimaries");
        connectorPrimaries.forEach(primary -> {
            ObjectNode item = primaries.addObject()
                    .put("side", primary.side().name().toLowerCase(Locale.ROOT))
                    .put("alias", primary.alias()).put("artifact", primary.artifact())
                    .put("observedSha256", primary.observedSha256());
            primary.declaredSha256().ifPresent(pin -> item.put("declaredSha256", pin));
        });
        FlinkHaEvidenceRenderer.render(evidence.putObject("flinkHa"), result.haEvidence(),
                result.phaseEvidence().map(PhaseExecutionEvidence::leaderFaults).orElse(List.of()));
        var selection = result.kafkaTransactionVersion();
        ObjectNode transactionVersion = evidence.putObject("kafkaTransactionVersion");
        transactionVersion.put("status", selection.requested().isEmpty() ? "not-requested"
                : selection.confirmed() ? "confirmed" : "unconfirmed");
        selection.requested().ifPresent(value -> transactionVersion.put("requested", value));
        selection.error().ifPresent(value -> transactionVersion.put("error", value));
        ArrayNode observations = transactionVersion.putArray("observations");
        selection.observations().forEach(observation -> {
            ObjectNode observed = observations.addObject();
            observation.metadataEpoch().ifPresent(value -> observed.put("metadataEpoch", value));
            observation.finalized().ifPresent(range -> observed.putObject("finalized")
                    .put("min", range.minimum()).put("max", range.maximum()));
            observation.supported().ifPresent(range -> observed.putObject("supported")
                    .put("min", range.minimum()).put("max", range.maximum()));
        });
        KafkaLogEvidenceRenderer.render(evidence.putObject("kafkaLogs"), result.kafkaLogs());
        ObjectNode componentErrorsNode = evidence.putObject("componentErrors");
        componentErrorsNode.put("eventCount", result.componentErrors().events().size());
        componentErrorsNode.put("coverage", result.componentErrors().diagnostics().isEmpty() ? "captured-prefix" : "partial");
        var componentEvents = componentErrorsNode.putArray("events");
        result.componentErrors().events().forEach(event -> {
            var item = componentEvents.addObject().put("process", event.process()).put("timestamp", event.timestamp())
                    .put("level", event.level()).put("logger", event.logger());
            event.producerId().ifPresent(value -> item.put("producerId", value));
            event.epoch().ifPresent(value -> item.put("epoch", value));
            event.transactionalId().ifPresent(value -> item.put("transactionalId", value));
            var kinds = item.putArray("kinds");
            event.kinds().forEach(kinds::add);
        });
        var componentDiagnostics = componentErrorsNode.putArray("diagnostics");
        result.componentErrors().diagnostics().forEach(componentDiagnostics::add);
        ObjectNode rest = evidence.putObject("flinkRest");
        rest.put("errorCount", result.flinkRestErrors().size());
        ArrayNode restErrors = rest.putArray("errors");
        result.flinkRestErrors().forEach(error -> restErrors.addObject()
                .put("sequence", error.sequence()).put("method", error.method())
                .put("endpoint", error.endpoint()).put("httpStatus", error.httpStatus())
                .put("body", error.body()));
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
            putIdentity(kill, "identity", effect.kill().identity());
            kill.put("identityFailure", effect.kill().identityFailure().orElse(null));
            kill.put("outcome", effect.outcome().name().toLowerCase(Locale.ROOT)
                    .replace('_', '-'));
            kill.put("confirmed", effect.outcome().confirmed());
            ArrayNode subtasks = kill.putArray("subtasksBeforeKill");
            ArrayNode targetedSubtasks = kill.putArray("targetedRunningSubtasksBeforeKill");
            ArrayNode qualifyingFailures = kill.putArray("qualifyingTargetFailures");
            effect.kill().identity()
                    .filter(identity -> effect.kill().target().equals(identity.logicalName()))
                    .ifPresent(identity -> {
                        effect.kill().jobBeforeKill().observation().ifPresent(before ->
                                before.subtasks().stream()
                                        .filter(subtask -> "RUNNING".equals(subtask.status()))
                                        .filter(subtask -> subtask.taskManagerId()
                                                .filter(identity.resourceId()::equals).isPresent())
                                        .forEach(subtask -> putSubtask(targetedSubtasks, subtask)));
                        effect.qualifyingTargetFailures().stream()
                                .forEach(failure -> {
                                    ObjectNode matching = qualifyingFailures.addObject();
                                    matching.put("taskManagerId", failure.taskManagerId().orElse(null));
                                    matching.put("targetResourceId", identity.resourceId());
                                    matching.put("attribution", failure.taskManagerId()
                                            .filter(identity.resourceId()::equals).isPresent()
                                            ? "reporter" : "remote-transport");
                                    matching.put("timestampMillis", failure.timestampMillis());
                                    matching.put("exceptionName", failure.exceptionName());
                                    matching.put("rootCause", failure.rootCause());
                                });
                    });
            effect.kill().jobBeforeKill().observation().ifPresent(before -> {
                before.subtasks().forEach(subtask -> putSubtask(subtasks, subtask));
                kill.put("jobStateBeforeKill", before.state().name());
                kill.put("completedCheckpointsBeforeKill", before.completedCheckpoints());
                kill.put("activeSubtasksBeforeKill", before.activeSubtasks().size());
                effect.restore().ifPresent(restore -> {
                    kill.put("restoredCheckpoint", restore.checkpointId());
                    kill.put("restoredAtMillis", restore.restoredAtMillis());
                    effect.kill().jobManagerTimeBeforeKill().ifPresent(sample ->
                            kill.put("restoredAfterPreInjectionMs",
                                    restore.restoredAtMillis() - sample));
                    effect.kill().jobManagerTimeAfterKill().ifPresent(sample ->
                            kill.put("restoredAfterKillObservationMs",
                                    restore.restoredAtMillis() - sample));
                });
            });
            effect.kill().jobManagerTimeBeforeKill().ifPresent(sample ->
                    kill.put("jobManagerTimeBeforeKill", sample));
            effect.kill().jobManagerTimeAfterKill().ifPresent(sample ->
                    kill.put("jobManagerTimeAfterKill", sample));
            kill.put("failuresAfterKill", effect.failuresAfterKill().size());
            if (!effect.failuresAfterKill().isEmpty()) {
                kill.put("firstFailureAfterKill",
                        effect.failuresAfterKill().getLast().rootCause());
            }
            kill.put("detail", effect.detail());
        }
        ArrayNode restarts = evidence.putArray("taskManagerRestarts");
        result.phaseEvidence().ifPresent(phases -> phases.taskManagerRestarts().forEach(restart -> {
            ObjectNode restarted = restarts.addObject();
            restarted.put("path", restart.path());
            ArrayNode iterations = restarted.putArray("loopIterations");
            restart.loopIterations().forEach(iteration -> iterations.add(
                    iteration.iteration() + "/" + iteration.totalIterations()));
            restarted.put("target", restart.target());
            putIdentity(restarted, "previousIdentity", restart.previousIdentity());
            putIdentity(restarted, "replacementIdentity", restart.replacementIdentity());
            restarted.put("previousIdentityFailure", restart.previousIdentityFailure().orElse(null));
            restarted.put("replacementIdentityFailure", restart.replacementIdentityFailure().orElse(null));
        }));
        var brokerOperations = evidence.putArray("brokerOperations");
        result.phaseEvidence().ifPresent(phases -> phases.brokerOperations().forEach(operation -> {
            var rendered = brokerOperations.addObject().put("path", operation.path())
                    .put("confirmed", operation.raw().confirmed());
            rendered.set("loopIterations", JSON.valueToTree(operation.loopIterations()));
            rendered.set("observations", JSON.valueToTree(operation.raw()));
        }));
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
        ArrayNode fencedComponents = processFence.putArray("components");
        ArrayNode processObservations = processFence.putArray("observations");
        processFence.put("observationOverflow", false);
        ObjectNode health = processFence.putObject("health");
        health.put("outcome", result.processHealth().outcome().name().toLowerCase(Locale.ROOT).replace('_', '-'));
        health.put("reason", result.processHealth().reason());
        health.put("detail", result.processHealth().detail());
        result.processObservations().ifPresent(snapshot -> {
            if (!snapshot.fenced().isEmpty() || snapshot.observations().stream().anyMatch(observation ->
                    observation.moment() != org.savonitar.flink.stability.runtime.api
                            .FlinkProcessWriteFenceEvidence.Moment.AFTER_DECLARED_KILL)) {
                processFence.put("status", "partial");
            }
            processFence.put("observationOverflow", snapshot.overflow());
            snapshot.observations().forEach(observation -> {
                ObjectNode node = processObservations.addObject();
                node.put("logicalName", observation.logicalName());
                node.put("role", observation.role().name().toLowerCase(Locale.ROOT).replace('_', '-'));
                node.put("runtimeId", observation.runtimeId().orElse(null));
                node.put("moment", observation.moment().name().toLowerCase(Locale.ROOT).replace('_', '-'));
                node.put("observedAt", observation.observedAt().toString());
                node.put("missing", observation.missing());
                node.put("diagnostic", observation.diagnostic().orElse(null));
                node.putNull("state");
                observation.state().ifPresent(state ->
                        FlinkHaEvidenceRenderer.state(node.putObject("state"), state));
            });
        });
        result.processFenceEvidence().map(fence -> fence.components())
                .orElseGet(() -> result.processObservations().map(snapshot -> snapshot.fenced()).orElse(List.of()))
                .forEach(component -> {
                    ObjectNode node = fencedComponents.addObject();
                    node.put("logicalName", component.logicalName());
                    node.put("role", component.role().name().toLowerCase(Locale.ROOT).replace('_', '-'));
                    node.put("runtimeId", component.runtimeId().orElse(null));
                    node.put("outcome", component.outcome().name().toLowerCase(Locale.ROOT).replace('_', '-'));
                });
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
            ArrayNode missingSamples = terminal.putArray("missingSamples");
            validation.evidence().missingSamples().forEach(missingSamples::add);
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
        ObjectNode runtime = evidence.putObject("flinkRuntime");
        runtime.put("identityKind", "docker-image-id");
        runtime.put("status", result.flinkRuntimeIdentity().outcome().name().toLowerCase(Locale.ROOT));
        runtime.put("detail", result.flinkRuntimeIdentity().detail());
        result.expectedFlinkRuntime().imageId().ifPresent(id -> runtime.put("expectedImageId", id));
        ObjectNode runtimeJar = runtime.putObject("runtimeJar");
        runtimeJar.put("status", "not-requested");
        result.expectedFlinkRuntime().runtimeJar().ifPresent(expectedJar -> {
            ObjectNode expectedJarNode = runtimeJar.putObject("expected");
            expectedJarNode.put("containerPath", expectedJar.containerPath());
            expectedJarNode.put("sha256", expectedJar.sha256());
        });
        result.runtimeJarIdentity().ifPresent(identity -> {
            runtimeJar.put("status", identity.outcome().name().toLowerCase(Locale.ROOT));
            runtimeJar.put("detail", identity.detail());
        });
        result.runtimeClassOrigins().ifPresent(origins -> {
            ArrayNode classes = runtimeJar.putArray("classes");
            origins.processes().forEach(process -> {
                ObjectNode loaded = classes.addObject();
                loaded.put("process", process.process());
                ObjectNode sources = loaded.putObject("sources");
                process.sources().forEach((entryClass, found) -> {
                    ArrayNode paths = sources.putArray(entryClass);
                    found.forEach(paths::add);
                });
            });
            origins.failure().ifPresent(failure -> runtimeJar.put("failure", failure));
        });
        ArrayNode artifactSets = runtime.putArray("connectorArtifactSets");
        Map<List<ProvisionedConnectorArtifact>, Integer> artifactRefs = new LinkedHashMap<>();
        ArrayNode components = runtime.putArray("components");
        result.flinkProvisioningEvidence().forEach(component -> {
            ObjectNode rendered = components.addObject();
            rendered.put("logicalName", component.logicalName());
            rendered.put("role", component.role().name().toLowerCase(Locale.ROOT));
            rendered.put("runtimeId", component.runtimeId());
            rendered.put("imageReference", component.imageReference());
            rendered.put("imageId", component.imageId());
            rendered.put("targetBindingSha256", component.targetBindingSha256());
            rendered.put("classpathManifestSha256", component.classpathManifestSha256());
            component.runtimeJarEvidence().ifPresent(observed -> {
                ObjectNode jar = rendered.putObject("runtimeJar");
                jar.put("containerPath", observed.jar().containerPath());
                jar.put("sha256", observed.jar().sha256());
                jar.put("classLoadProcess", observed.classLoadProcess());
            });
            // Share only identical observed entries, never merely equal declared hashes.
            int artifactRef = artifactRefs.computeIfAbsent(component.connectorArtifacts(), entries -> {
                int index = artifactSets.size();
                ArrayNode artifacts = artifactSets.addArray();
                entries.forEach(artifact -> {
                    ObjectNode copied = artifacts.addObject();
                    copied.put("index", artifact.index());
                    copied.put("containerPath", artifact.containerPath());
                    copied.put("sha256", artifact.sha256());
                });
                return index;
            });
            rendered.put("connectorArtifactsRef", artifactRef);
        });

        ArrayNode diagnostics = root.putArray("diagnostics");
        result.diagnostics().forEach(diagnostics::add);
        try {
            return JSON.writeValueAsString(root);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not render scenario result", failure);
        }
    }

    private static void putIdentity(
            ObjectNode parent, String name, Optional<TaskManagerControl.Identity> identity) {
        if (identity.isEmpty()) {
            parent.putNull(name);
            return;
        }
        TaskManagerControl.Identity observed = identity.orElseThrow();
        ObjectNode rendered = parent.putObject(name);
        rendered.put("logicalName", observed.logicalName());
        rendered.put("runtimeId", observed.runtimeId());
        rendered.put("resourceId", observed.resourceId());
    }

    private static void putSubtask(ArrayNode parent, FlinkJobObservation.Subtask subtask) {
        ObjectNode rendered = parent.addObject();
        rendered.put("vertexName", subtask.vertexName());
        rendered.put("index", subtask.index());
        rendered.put("attempt", subtask.attempt());
        rendered.put("status", subtask.status());
        rendered.put("taskManagerId", subtask.taskManagerId().orElse(null));
    }
}
