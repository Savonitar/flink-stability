package org.savonitar.flink.stability.core.spec.resolution;

import com.fasterxml.jackson.databind.node.ObjectNode;
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
}
