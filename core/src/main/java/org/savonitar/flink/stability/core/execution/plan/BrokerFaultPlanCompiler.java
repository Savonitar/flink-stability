package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerControl;
import java.nio.file.Path;
import java.util.*;
import java.math.BigInteger;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.*;

final class BrokerFaultPlanCompiler {
    static void validate(Path source, ObjectNode document, List<Diagnostic> issues) {
        BigInteger count = BigInteger.ZERO;
        for (int i = 0; i < document.path("phases").size(); i++)
            count = count.add(validateSteps(source, document.path("phases").get(i).path("steps"),
                    "$/phases/" + i + "/steps", document, issues));
        if (count.compareTo(BigInteger.valueOf(100)) > 0)
            issues.add(issue(source, "runner.phase.broker-fault-count-unsupported", "$/phases", "At most 100 expanded broker faults"));
    }
    private static BigInteger validateSteps(Path source, JsonNode steps, String path, ObjectNode document, List<Diagnostic> issues) {
        BigInteger count = BigInteger.ZERO;
        for (int i = 0; i < steps.size(); i++) {
            var step = steps.get(i); var location = path + "/" + i;
            if (step.has("broker_fault")) {
                count = count.add(BigInteger.ONE);
                try { fault(step.path("broker_fault"), document); }
                catch (IllegalArgumentException failure) {
                    issues.add(issue(source, "runner.phase.broker-fault-invalid", location + "/broker_fault", failure.getMessage()));
                }
            } else if (step.has("loop")) count = count.add(validateSteps(source, step.at("/loop/steps"),
                    location + "/loop/steps", document, issues).multiply(step.at("/loop/times").bigIntegerValue()));
        }
        return count;
    }
    static ExecutableScenarioPlan.BrokerFault fault(JsonNode value, ObjectNode document) {
        var target = value.path("target");
        String prefix = null;
        JsonNode selectedJob = null;
        if ("transaction-coordinator".equals(target.path("type").asText())) {
            for (var job : document.at("/workload/jobs")) if (job.path("alias").asText().equals(target.path("job").asText())) {
                selectedJob = job;
                prefix = job.at("/sink/transactional_id_prefix").asText();
            }
        }
        var kind = "named".equals(target.path("kind").asText()) ? KafkaBrokerControl.TargetKind.NAMED
                : "partition-leader".equals(target.path("type").asText()) ? KafkaBrokerControl.TargetKind.PARTITION_LEADER
                : KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR;
        Integer commitVersion = null;
        if (value.path("require_commit").asBoolean()) {
            if (kind != KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR || selectedJob == null
                    || !"INCREMENTING".equals(selectedJob.at("/sink/transaction_id_naming_strategy").asText()))
                throw new IllegalArgumentException("require_commit needs an INCREMENTING sink transaction coordinator");
            var version = document.at("/setup/kafka/clusters").path(target.path("cluster").asText()).path("transaction_version");
            if (!version.isInt() || version.intValue() < 1 || version.intValue() > 2)
                throw new IllegalArgumentException("require_commit needs explicit transaction_version 1 or 2");
            commitVersion = version.intValue();
        }
        return new ExecutableScenarioPlan.BrokerFault(new KafkaBrokerControl.Request(
                new KafkaBrokerControl.Target(kind, target.path("name").textValue(), target.path("topic").textValue(),
                        target.path("partition").asInt(-1), prefix),
                KafkaBrokerControl.Action.valueOf(value.path("mode").asText().toUpperCase(Locale.ROOT)),
                parseDuration(value.path("duration").asText()), parseDuration(value.path("timeout").asText()), commitVersion));
    }
}
