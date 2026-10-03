package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan.IdSetMode;
import java.nio.file.Path;
import java.util.List;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.issue;

/** An opt-in oracle mode; historical at-least-once negative controls keep strict comparison. */
final class IdSetModeCompiler {
    static IdSetMode mode(JsonNode validator) {
        return "at-least-once".equals(validator.path("mode").asText()) ? IdSetMode.AT_LEAST_ONCE : IdSetMode.EXACTLY_ONCE;
    }
    static void validate(Path source, JsonNode document, List<Diagnostic> issues) {
        if (mode(document.at("/terminal_validations/0")) == IdSetMode.AT_LEAST_ONCE
                && !"AT_LEAST_ONCE".equals(document.at("/workload/jobs/0/sink/delivery_guarantee").asText()))
            issues.add(issue(source, "runner.validation.at-least-once-sink-required", "$/terminal_validations/0/mode",
                    "The at-least-once oracle requires an AT_LEAST_ONCE sink"));
    }
}
