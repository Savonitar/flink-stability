package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/** Checks and maps the job's Kafka sink for the first runner (SPEC-001 R5.4, R5.6c). */
final class SinkCompiler {
    private static final Set<String> GUARANTEES = Set.of("EXACTLY_ONCE", "AT_LEAST_ONCE");
    private static final Set<String> NAMING_STRATEGIES = Set.of("INCREMENTING", "POOLING");

    private SinkCompiler() {}

    static void validate(
            Path source,
            String sinkPath,
            ObjectNode sink,
            List<Diagnostic> issues) {
        String guarantee = sink.path("delivery_guarantee").textValue();
        if (!GUARANTEES.contains(guarantee)) {
            issues.add(issue(source, "runner.workload.delivery-guarantee-unsupported",
                    sinkPath + "/delivery_guarantee",
                    "The first runner tests EXACTLY_ONCE and AT_LEAST_ONCE Kafka sinks"));
        }
        if (!"EXACTLY_ONCE".equals(guarantee)) {
            return;
        }
        if (!NAMING_STRATEGIES.contains(
                sink.path("transaction_id_naming_strategy").textValue())) {
            issues.add(issue(source, "runner.workload.transaction-id-naming-unsupported",
                    sinkPath + "/transaction_id_naming_strategy",
                    "Supported transaction ID naming strategies are INCREMENTING and POOLING"));
        }
        if (sink.has("transaction_timeout")) {
            int before = issues.size();
            ExecutableScenarioPlanCompiler.requireDuration(
                    source, sink.get("transaction_timeout"),
                    sinkPath + "/transaction_timeout", issues);
            if (issues.size() == before && transactionTimeout(sink).compareTo(
                    KafkaBrokerPolicy.V1_TRANSACTION_MAX_TIMEOUT) > 0) {
                issues.add(issue(source, "runner.workload.transaction-timeout-unsupported",
                        sinkPath + "/transaction_timeout",
                        "The transaction timeout must not exceed the broker's "
                                + KafkaBrokerPolicy.V1_TRANSACTION_MAX_TIMEOUT.toHours()
                                + "h transaction.max.timeout.ms"));
            }
        }
    }

    static ExecutableScenarioPlan.Sink map(
            ObjectNode sink,
            ExecutableScenarioPlan.TopicReference topic) {
        return "AT_LEAST_ONCE".equals(sink.path("delivery_guarantee").textValue())
                ? ExecutableScenarioPlan.Sink.atLeastOnce(topic)
                : ExecutableScenarioPlan.Sink.exactlyOnce(
                        topic,
                        sink.path("transactional_id_prefix").textValue(),
                        ExecutableScenarioPlan.TransactionIdNamingStrategy.valueOf(
                                sink.path("transaction_id_naming_strategy").textValue()));
    }

    /** The declared exactly-once transaction timeout, or the two-hour v1 default. */
    static Duration transactionTimeout(ObjectNode sink) {
        return sink.has("transaction_timeout")
                ? ExecutableScenarioPlanCompiler.parseDuration(
                        sink.path("transaction_timeout").textValue())
                : ExecutableScenarioPlan.KAFKA_TRANSACTION_TIMEOUT;
    }

    private static Diagnostic issue(
            Path source, String code, String path, String message) {
        return new Diagnostic(source, ResolutionScope.SINGLE, code, path, message);
    }
}
