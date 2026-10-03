package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.*;

/** A single terminal atomic transition; intermediate stopped-job states cannot escape the slice. */
final class SavepointPlanCompiler {
    static void validate(Path source, JsonNode document, List<Diagnostic> issues) {
        var steps = new ArrayList<JsonNode>();
        for (var phase : document.path("phases")) phase.path("steps").forEach(steps::add);
        if (steps.stream().noneMatch(step -> step.has("savepoint_restore"))) {
            // Nested transitions are refused as well; the executor has only one active submission.
            if (!document.path("phases").findValues("savepoint_restore").isEmpty())
                issues.add(issue(source,"runner.savepoint.loop-unsupported","$/phases","Savepoint restore cannot be nested"));
            return;
        }
        try {
            if (steps.stream().filter(step -> step.has("savepoint_restore")).count() != 1 || !steps.getLast().has("savepoint_restore")
                    || steps.stream().anyMatch(step -> !step.has("await") && !step.has("wait") && !step.has("savepoint_restore")))
                throw new IllegalArgumentException("Savepoint restore must be the only lifecycle/fault and the final step");
            if (document.at("/setup/flink").has("high_availability") || document.at("/setup/flink").has("token_provider"))
                throw new IllegalArgumentException("Savepoint restore excludes HA and token experiments in this slice");
            if (!"EXACTLY_ONCE".equals(document.at("/workload/jobs/0/sink/delivery_guarantee").asText())
                    || !"exactly-once".equals(document.at("/terminal_validations/0/mode").asText("exactly-once")))
                throw new IllegalArgumentException("Savepoint restore requires the EOS sink and exact-ID oracle");
            var value = steps.getLast().path("savepoint_restore");
            if (!value.path("job").asText().equals(document.at("/workload/jobs/0/alias").asText()))
                throw new IllegalArgumentException("Savepoint restore must target the submitted job");
            var step = step(value, document);
            int slots = document.at("/setup/flink/taskmanagers").asInt() * org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget.TASK_SLOTS_PER_TASK_MANAGER;
            if (step.parallelism() > slots) throw new IllegalArgumentException("Restored parallelism exceeds provisioned slots");
            if ("POOLING".equals(document.at("/workload/jobs/0/sink/transaction_id_naming_strategy").asText())
                    && step.strategy() != ExecutableScenarioPlan.TransactionIdNamingStrategy.POOLING)
                throw new IllegalArgumentException("POOLING to INCREMENTING restore is unsupported by the connector");
        } catch (IllegalArgumentException failure) {
            issues.add(issue(source,"runner.savepoint.invalid","$/phases",failure.getMessage()));
        }
    }
    static ExecutableScenarioPlan.SavepointRestore step(JsonNode value, JsonNode document) {
        var parallelism = value.has("parallelism") ? value.path("parallelism") : document.at("/workload/jobs/0/parallelism");
        if (!parallelism.isIntegralNumber() || !parallelism.canConvertToInt() || parallelism.intValue() < 1)
            throw new IllegalArgumentException("Restored parallelism must be a positive 32-bit integer");
        return new ExecutableScenarioPlan.SavepointRestore(parallelism.intValue(),
                ExecutableScenarioPlan.TransactionIdNamingStrategy.valueOf(value.path("transaction_id_naming_strategy")
                        .asText(document.at("/workload/jobs/0/sink/transaction_id_naming_strategy").asText())),
                value.has("timeout") ? parseDuration(value.path("timeout").asText()) : Duration.ofMinutes(3));
    }
}
