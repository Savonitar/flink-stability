package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.issue;

/** Resolves named process operations and checks each TaskManager's kill/restart lifecycle. */
final class TaskManagerLifecycleCompiler {
    private TaskManagerLifecycleCompiler() {}

    static void validate(Path source, ObjectNode document, List<Diagnostic> issues) {
        JsonNode count = document.at("/setup/flink/taskmanagers");
        if (!count.canConvertToInt() || count.intValue() < 1) {
            return; // The topology validator reports invalid counts before any mapping occurs.
        }
        Map<String, String> stopped = new LinkedHashMap<>();
        ArrayNode phases = (ArrayNode) document.path("phases");
        for (int index = 0; index < phases.size(); index++) {
            validateSteps(source, (ArrayNode) phases.get(index).path("steps"),
                    "$/phases/" + index + "/steps", count.intValue(), stopped, issues);
        }
        stopped.forEach((name, path) -> issues.add(issue(source,
                "runner.phase.taskmanager-kill-unhealed", path,
                "A killed TaskManager must have a reachable restart of the same target: " + name)));
    }

    private static void validateSteps(Path source, ArrayNode steps, String path, int count,
                                      Map<String, String> stopped, List<Diagnostic> issues) {
        for (int index = 0; index < steps.size(); index++) {
            JsonNode step = steps.get(index);
            String stepPath = path + "/" + index;
            if (step.get("kill") instanceof ObjectNode kill) {
                JsonNode target = kill.path("target");
                String name = target.path("name").asText();
                if (!"named".equals(target.path("kind").asText())
                        || !"taskmanager".equals(target.path("role").asText())
                        || !isDeclaredTaskManager(name, count)) {
                    issues.add(issue(source, "runner.phase.kill-target-unsupported",
                            stepPath + "/kill/target",
                            "A kill must name a declared TaskManager"));
                } else if (stopped.putIfAbsent(name, stepPath + "/kill") != null) {
                    issues.add(issue(source, "runner.phase.taskmanager-already-stopped",
                            stepPath + "/kill", "Cannot kill a stopped TaskManager: " + name));
                } else if (stopped.size() > 1) {
                    issues.add(issue(source, "runner.phase.taskmanager-kill-overlap-unsupported",
                            stepPath + "/kill",
                            "Restart the preceding killed TaskManager before injecting another kill"));
                }
            } else if (step.get("restart") instanceof ObjectNode restart) {
                if (!"taskmanager".equals(restart.path("component").asText())) {
                    issues.add(issue(source, "runner.phase.restart-component-unsupported",
                            stepPath + "/restart/component", "The runner restarts only TaskManagers"));
                    continue;
                }
                if (restart.has("image")) {
                    issues.add(issue(source, "runner.phase.restart-image-unsupported",
                            stepPath + "/restart/image",
                            "A restart preserves the verified runtime target"));
                    continue;
                }
                if (!restart.has("name") && count > 1) {
                    issues.add(issue(source, "runner.phase.restart-target-required",
                            stepPath + "/restart", "Multiple TaskManagers require an explicit restart name"));
                    continue;
                }
                String name = restart.path("name").asText("taskmanager-1");
                if (!isDeclaredTaskManager(name, count)) {
                    issues.add(issue(source, "runner.phase.restart-target-unsupported",
                            stepPath + "/restart/name", "A restart must name a declared TaskManager"));
                } else if (stopped.remove(name) == null) {
                    issues.add(issue(source, "runner.phase.taskmanager-already-running",
                            stepPath + "/restart", "A restart must heal a preceding kill of " + name));
                }
            } else if (step.get("loop") instanceof ObjectNode loop) {
                Map<String, String> before = new LinkedHashMap<>(stopped);
                validateSteps(source, (ArrayNode) loop.path("steps"), stepPath + "/loop/steps",
                        count, stopped, issues);
                if (loop.path("times").bigIntegerValue().compareTo(BigInteger.ONE) > 0
                        && !before.keySet().equals(stopped.keySet())) {
                    issues.add(issue(source, "runner.phase.loop-taskmanager-lifecycle-unstable",
                            stepPath + "/loop",
                            "A repeated loop must restore every TaskManager to its entry state"));
                }
            }
        }
    }

    static boolean isDeclaredTaskManager(String name, int count) {
        try {
            return org.savonitar.flink.stability.runtime.api.Checks.taskManagerOrdinal(name) <= count;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }
}
