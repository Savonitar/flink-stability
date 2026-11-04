package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;

import java.nio.file.Path;
import java.util.List;

/** The deliberately narrow first state-recovery workload slice. */
final class StatePrefixPlanCompiler {
    private StatePrefixPlanCompiler() {}

    static void validate(Path source, JsonNode document, List<Diagnostic> issues) {
        JsonNode job = document.at("/workload/jobs/0");
        boolean keyed = "keyed-prefix-v1".equals(job.at("/behavior/type").asText());
        JsonNode validator = document.at("/terminal_validations/0");
        boolean stateOracle = "kafka.state-prefix".equals(validator.path("type").asText());
        if (!keyed && !stateOracle) return;
        if (!keyed || !stateOracle || !job.path("alias").asText().equals(validator.path("job").asText())
                || !job.at("/sink/cluster").equals(validator.path("cluster"))
                || !job.at("/sink/topic").equals(validator.path("topic"))) {
            reject(source, issues, "binding-unsupported", "$/terminal_validations",
                    "keyed-prefix-v1 requires kafka.state-prefix bound to this job and sink");
        }
        JsonNode flink = document.at("/setup/flink");
        if (flink.path("jobmanagers").asInt() != 1 || flink.path("taskmanagers").asInt() != 1
                || flink.has("high_availability") || job.path("parallelism").asInt() != 1
                || job.at("/state_ttl/enabled").asBoolean()
                || !"filesystem".equals(job.at("/checkpointing/storage/type").asText())
                || !"EXACTLY_ONCE".equals(job.at("/sink/delivery_guarantee").asText())
                || !"INCREMENTING".equals(job.at("/sink/transaction_id_naming_strategy").asText())) {
            reject(source, issues, "topology-unsupported", "$/workload/jobs/0",
                    "keyed-prefix-v1 requires one JM/TM, parallelism 1, no HA/TTL, filesystem checkpoints and EOS/INCREMENTING");
        }
        JsonNode clusters = document.at("/setup/kafka/clusters");
        for (JsonNode cluster : clusters) {
            for (JsonNode topic : cluster.path("topics")) {
                String name = topic.path("name").asText();
                if ((name.equals(job.at("/source/topic").asText()) || name.equals(job.at("/sink/topic").asText()))
                        && topic.path("partitions").asInt() != 1) {
                    reject(source, issues, "partitions-unsupported", "$/setup/kafka/clusters",
                            "keyed-prefix-v1 requires one source and one sink partition");
                }
                if (topic.path("input_source").has("connect_via_proxy")) {
                    reject(source, issues, "input-faults-unsupported", "$/setup/kafka/clusters",
                            "keyed-prefix-v1 requires a complete generated input manifest without preload faults");
                }
                if (topic.has("input_source") && hasKill(document.path("phases"))
                        && topic.at("/input_source/total").asLong() < 2L * job.at("/behavior/key_count").asInt()) {
                    reject(source, issues, "input-too-small", "$/setup/kafka/clusters",
                            "State recovery requires at least two events per key: nonzero checkpoint state and a suffix after restore");
                }
            }
        }
        rejectRestoreSteps(source, document.path("phases"), issues);
    }

    private static boolean hasKill(JsonNode node) {
        if (node.isObject() && node.has("kill")) return true;
        if (node.isContainerNode()) for (JsonNode child : node) if (hasKill(child)) return true;
        return false;
    }

    private static void rejectRestoreSteps(Path source, JsonNode node, List<Diagnostic> issues) {
        if (node.isObject() && node.has("savepoint_restore")) {
            reject(source, issues, "restore-unsupported", "$/phases",
                    "The first keyed-prefix-v1 slice does not support savepoint restore or rescaling");
        }
        if (node.isContainerNode()) node.forEach(child -> rejectRestoreSteps(source, child, issues));
    }

    private static void reject(Path source, List<Diagnostic> issues, String suffix, String path, String detail) {
        issues.add(ExecutableScenarioPlanCompiler.issue(source, "runner.state-prefix." + suffix, path, detail));
    }
}
