package org.savonitar.flink.stability.core.execution.plan;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import java.nio.file.Path;
import java.util.*;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.*;

/** Phase capability checks and mapping, separated from artifact binding and topology. */
final class PhasePlanCompiler {
    private static final Set<String> SUPPORTED_STEP_KEYS = Set.of(
            "await", "wait", "loop", "kill", "restart", "network_fault", "leader_fault", "broker_fault", "packet_fault", "savepoint_restore");
    static void validatePhases(
            Path source,
            ObjectNode document,
            List<Diagnostic> issues) {
        ArrayNode phases = (ArrayNode) document.get("phases");
        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            ObjectNode phase = (ObjectNode) phases.get(phaseIndex);
            validateSteps(
                    source,
                    (ArrayNode) phase.get("steps"),
                    "$/phases/" + phaseIndex + "/steps",
                    document.at("/workload/jobs/0/alias").textValue(),
                    false,
                    issues);
        }
        ProcessLifecycleCompiler.validate(source, document, issues);
        BrokerFaultPlanCompiler.validate(source, document, issues);
        PacketFaultPlanCompiler.validate(source, document, issues);
        SavepointPlanCompiler.validate(source, document, issues);
    }

    private static void validateSteps(
            Path source,
            ArrayNode steps,
            String stepsPath,
            String jobAlias,
            boolean inLoop,
            List<Diagnostic> issues) {
        for (int index = 0; index < steps.size(); index++) {
            ObjectNode step = (ObjectNode) steps.get(index);
            String stepPath = stepsPath + "/" + index;
            String key = step.fieldNames().next();
            if (!SUPPORTED_STEP_KEYS.contains(key)) {
                issues.add(issue(
                        source,
                        "runner.phase.step-unsupported",
                        stepPath + "/" + pointer(key),
                        "The first runner does not execute '" + key + "' steps"));
                continue;
            }
            switch (key) {
                case "await" -> validateAwait(
                        source, (ObjectNode) step.get(key), stepPath + "/await", jobAlias, issues);
                case "wait" -> requireDuration(
                        source,
                        step.at("/wait/duration"),
                        stepPath + "/wait/duration",
                        issues);
                case "loop" -> {
                    ObjectNode loop = (ObjectNode) step.get("loop");
                    requirePositiveInt(source, loop.path("times"),
                            stepPath + "/loop/times", issues);
                    validateSteps(
                            source,
                            (ArrayNode) loop.get("steps"),
                            stepPath + "/loop/steps",
                            jobAlias,
                            true,
                            issues);
                }
                case "kill", "restart", "leader_fault", "broker_fault", "packet_fault", "savepoint_restore" -> { /* Validated with their topology/lifecycle. */ }
                case "network_fault" -> NetworkFaultCompiler.validateStep(
                        source, (ObjectNode) step.get(key), stepPath + "/network_fault",
                        inLoop, issues);
                default -> throw new IllegalStateException("Unexpected supported step " + key);
            }
        }
    }

    private static void validateAwait(
            Path source,
            ObjectNode await,
            String path,
            String jobAlias,
            List<Diagnostic> issues) {
        requireDuration(source, await.path("timeout"), path + "/timeout", issues);
        ObjectNode condition = (ObjectNode) await.get("condition");
        String type = condition.path("type").textValue();
        if (!Set.of("job-state", "checkpoint-completed", "checkpoint-in-progress").contains(type)) {
            issues.add(issue(
                    source,
                    "runner.phase.await-condition-unsupported",
                    path + "/condition/type",
                    "The first runner awaits job-state, checkpoint-completed and checkpoint-in-progress"));
            return;
        }
        if (!jobAlias.equals(condition.path("job").textValue())) {
            issues.add(issue(
                    source,
                    "runner.phase.await-job-unsupported",
                    path + "/condition/job",
                    "Await must target the one compiled workload job '" + jobAlias + "'"));
        }
        if ("job-state".equals(type)
                && !"RUNNING".equals(condition.path("state").textValue())) {
            issues.add(issue(
                    source,
                    "runner.phase.job-state-unsupported",
                    path + "/condition/state",
                    "The first runner can await only the RUNNING state"));
        }
        if ("checkpoint-completed".equals(type)) {
            requirePositiveLong(
                    source,
                    condition.path("count"),
                    path + "/condition/count",
                    issues);
        }
    }

    static List<ExecutableScenarioPlan.Phase> mapPhases(ArrayNode phaseNodes, ObjectNode document) {
        List<ExecutableScenarioPlan.Phase> phases = new ArrayList<>(phaseNodes.size());
        for (JsonNode value : phaseNodes) {
            ObjectNode phase = (ObjectNode) value;
            phases.add(new ExecutableScenarioPlan.Phase(
                    phase.path("name").textValue(),
                    mapSteps((ArrayNode) phase.get("steps"), document)));
        }
        return List.copyOf(phases);
    }

    private static List<ExecutableScenarioPlan.Step> mapSteps(ArrayNode stepNodes, ObjectNode document) {
        List<ExecutableScenarioPlan.Step> steps = new ArrayList<>(stepNodes.size());
        for (JsonNode value : stepNodes) {
            ObjectNode step = (ObjectNode) value;
            if (step.get("await") instanceof ObjectNode await) {
                ObjectNode condition = (ObjectNode) await.get("condition");
                ExecutableScenarioPlan.TimeoutOutcome onTimeout =
                        ExecutableScenarioPlan.TimeoutOutcome.valueOf(
                                await.path("on_timeout").textValue().toUpperCase(Locale.ROOT));
                if ("job-state".equals(condition.path("type").textValue())) {
                    steps.add(new ExecutableScenarioPlan.AwaitJobState(
                            condition.path("job").textValue(),
                            ExecutableScenarioPlan.JobState.RUNNING,
                            parseDuration(await.path("timeout").textValue()),
                            onTimeout));
                } else if ("checkpoint-in-progress".equals(condition.path("type").textValue())) {
                    steps.add(new ExecutableScenarioPlan.AwaitCheckpointInProgress(
                            condition.path("job").textValue(), parseDuration(await.path("timeout").textValue()), onTimeout));
                } else {
                    steps.add(new ExecutableScenarioPlan.AwaitCheckpoints(
                            condition.path("job").textValue(),
                            condition.path("count").longValue(),
                            parseDuration(await.path("timeout").textValue()),
                            onTimeout));
                }
            } else if (step.get("wait") instanceof ObjectNode wait) {
                steps.add(new ExecutableScenarioPlan.Wait(
                        parseDuration(wait.path("duration").textValue())));
            } else if (step.get("kill") instanceof ObjectNode kill) {
                steps.add("broker".equals(kill.at("/target/role").asText())
                        ? new ExecutableScenarioPlan.BrokerOperation(kill.at("/target/name").textValue(), false)
                        : new ExecutableScenarioPlan.KillTaskManager(kill.at("/target/name").textValue()));
            } else if (step.has("restart")) {
                steps.add("kafka".equals(step.at("/restart/component").asText())
                        ? new ExecutableScenarioPlan.BrokerOperation(step.at("/restart/name").asText(), true)
                        : new ExecutableScenarioPlan.RestartTaskManager(step.path("restart").path("name").asText("taskmanager-1")));
            } else if (step.get("network_fault") instanceof ObjectNode networkFault) {
                steps.add(NetworkFaultCompiler.step(networkFault));
            } else if (step.has("savepoint_restore")) {
                steps.add(SavepointPlanCompiler.step(step.path("savepoint_restore"), document));
            } else if (step.has("packet_fault")) {
                steps.add(PacketFaultPlanCompiler.fault(step.path("packet_fault"), document));
            } else if (step.has("broker_fault")) {
                steps.add(BrokerFaultPlanCompiler.fault(step.path("broker_fault"), document));
            } else if (step.has("leader_fault")) {
                steps.add(HighAvailabilityPlanCompiler.fault(step.path("leader_fault")));
            } else if (step.get("loop") instanceof ObjectNode loop) {
                steps.add(new ExecutableScenarioPlan.Loop(
                        loop.path("times").intValue(),
                        mapSteps((ArrayNode) loop.get("steps"), document)));
            } else {
                throw new IllegalStateException("Unsupported step escaped capability validation");
            }
        }
        return List.copyOf(steps);
    }

}
