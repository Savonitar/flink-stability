package org.savonitar.flink.stability.core.spec.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.util.Objects;
import static org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightValidator.issue;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** Resolves restart targets without changing the existing image-inheritance rules. */
final class RestartPreflightValidator {
    private RestartPreflightValidator() {}

    static void validate(
            Path source,
            ObjectNode restart,
            String path,
            Set<String> kafkaClusters,
            Consumer<ObjectNode> validateNamedTarget,
            List<Diagnostic> issues) {
        String component = restart.path("component").textValue();
        if (("taskmanager".equals(component) || "kafka".equals(component)) && restart.has("name")) {
            ObjectNode target = restart.objectNode().put("kind", "named")
                    .put("role", "kafka".equals(component) ? "broker" : "taskmanager").put("name", restart.path("name").textValue());
            validateNamedTarget.accept(target);
        }
        if ("kafka".equals(component) && kafkaClusters.size() != 1) {
            issues.add(new Diagnostic(source, ResolutionScope.COMMON,
                    "preflight.restart.kafka-cluster-ambiguous", path + "/component",
                    "Kafka restart is ambiguous because the scenario declares clusters "
                            + kafkaClusters.stream().sorted().toList()));
        }
    }

    static void validateImageState(
            Path source,
            ResolutionScope scope,
            ObjectNode document,
            BigInteger taskmanagerCount,
            ArrayNode phases,
            List<Diagnostic> issues) {
        String setupTarget = document.at("/setup/flink/image").textValue();
        FlinkTargetState state = new FlinkTargetState(setupTarget, setupTarget);
        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            ObjectNode phase = (ObjectNode) phases.get(phaseIndex);
            state = validateFlinkRestartImageStateInSteps(
                    source,
                    scope,
                    taskmanagerCount,
                    (ArrayNode) phase.get("steps"),
                    "$/phases/" + phaseIndex + "/steps",
                    state,
                    issues);
        }
    }

    private static FlinkTargetState validateFlinkRestartImageStateInSteps(
            Path source,
            ResolutionScope scope,
            BigInteger taskmanagerCount,
            ArrayNode steps,
            String stepsPath,
            FlinkTargetState initial,
            List<Diagnostic> issues) {
        FlinkTargetState state = initial;
        for (int stepIndex = 0; stepIndex < steps.size(); stepIndex++) {
            ObjectNode step = (ObjectNode) steps.get(stepIndex);
            String stepPath = stepsPath + "/" + stepIndex;
            if (step.get("restart") instanceof ObjectNode restart) {
                state = applyFlinkRestartImageState(
                        source,
                        scope,
                        taskmanagerCount,
                        restart,
                        stepPath + "/restart",
                        state,
                        issues);
            } else if (step.get("loop") instanceof ObjectNode loop) {
                BigInteger times = loop.path("times").bigIntegerValue();
                // Restart-image transitions consist only of retaining or assigning constants.
                // A second pass is sufficient to expose a loop that is valid from its initial
                // state but invalid after its own first iteration; further valid passes are
                // state-identical to the second.
                FlinkTargetState before = state;
                state = validateFlinkRestartImageStateInSteps(
                        source,
                        scope,
                        taskmanagerCount,
                        (ArrayNode) loop.get("steps"),
                        stepPath + "/loop/steps",
                        state,
                        issues);
                if (times.compareTo(BigInteger.ONE) > 0 && !state.equals(before)) {
                    state = validateFlinkRestartImageStateInSteps(
                            source,
                            scope,
                            taskmanagerCount,
                            (ArrayNode) loop.get("steps"),
                            stepPath + "/loop/steps",
                            state,
                            issues);
                }
            }
        }
        return state;
    }

    private static FlinkTargetState applyFlinkRestartImageState(
            Path source,
            ResolutionScope scope,
            BigInteger taskmanagerCount,
            ObjectNode restart,
            String path,
            FlinkTargetState state,
            List<Diagnostic> issues) {
        String component = restart.path("component").textValue();
        JsonNode imageNode = restart.get("image");
        String image = imageNode == null ? null : imageNode.textValue();
        if ("jobmanager".equals(component)) {
            return image == null ? state : state.withJobmanager(image);
        }
        if ("taskmanager".equals(component)) {
            if (image == null) {
                return state;
            }
            if (!BigInteger.ONE.equals(taskmanagerCount)) {
                issues.add(issue(
                        source,
                        scope,
                        "preflight.restart.taskmanager-image-ambiguous",
                        path + "/image",
                        "TaskManager image restart identifies no logical slot when the resolved "
                                + "TaskManager count is " + taskmanagerCount));
                return state;
            }
            return state.withTaskmanagers(image);
        }
        if (!"flink".equals(component)) {
            return state;
        }
        if (image != null) {
            return new FlinkTargetState(image, image);
        }
        if (!state.uniform()) {
            issues.add(issue(
                    source,
                    scope,
                    "preflight.restart.flink-image-inheritance-ambiguous",
                    path,
                    "Full Flink restart cannot inherit one image because desired targets differ: "
                            + state.components(taskmanagerCount)));
        }
        return state;
    }

    private record FlinkTargetState(String jobmanager, String taskmanagers) {
        private FlinkTargetState {
            Objects.requireNonNull(jobmanager, "jobmanager");
            Objects.requireNonNull(taskmanagers, "taskmanagers");
        }

        private FlinkTargetState withJobmanager(String image) {
            return new FlinkTargetState(image, taskmanagers);
        }

        private FlinkTargetState withTaskmanagers(String image) {
            return new FlinkTargetState(jobmanager, image);
        }

        private boolean uniform() {
            return jobmanager.equals(taskmanagers);
        }

        private List<String> components(BigInteger taskmanagerCount) {
            return List.of(
                    "jobmanager-1=" + jobmanager,
                    "taskmanagers(" + taskmanagerCount + ")=" + taskmanagers);
        }
    }

}
