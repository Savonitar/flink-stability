package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.PreparedExecutableScenarioPlan;

import java.util.Optional;

/** Immutable artifact and job settings used with an explicitly selected custom runtime. */
final class CustomRuntimeSubjectEvidence {
    private static final ObjectMapper JSON = new ObjectMapper();

    private CustomRuntimeSubjectEvidence() {}

    static Optional<JsonNode> from(PreparedExecutableScenarioPlan prepared) {
        var plan = prepared.executablePlan();
        var naming = plan.job().sink().transactionIdNamingStrategy();
        boolean custom = plan.flink().declaredLine().isPresent() || !plan.flink().config().isEmpty()
                || plan.kafka().runtimeTarget().customConfiguration()
                || plan.kafka().transactionVersionBrokerDefault()
                || !prepared.connectorBundle().imageConnectors().isEmpty()
                || naming.filter(value -> value == ExecutableScenarioPlan.TransactionIdNamingStrategy.CONNECTOR_DEFAULT).isPresent();
        if (!custom) return Optional.empty();
        var evidence = JSON.createObjectNode();
        if (plan.flink().declaredLine().isPresent() || plan.kafka().imageId().isPresent()) {
            var assertions = evidence.putObject("compatibilityAssertions");
            plan.flink().declaredLine().ifPresent(line -> assertions.put("flink", "author-assertion"));
            plan.kafka().imageId().ifPresent(id -> assertions.put("kafka", "author-assertion"));
        }
        var workload = prepared.workloadArtifact();
        evidence.putObject("workload").put("artifact", workload.declaredReference())
                .put("sha256", workload.sha256());
        naming.ifPresent(value -> evidence.put("transactionIdNamingStrategy", value.wireValue()));
        evidence.set("jobFlinkConfiguration", JSON.valueToTree(plan.job().standardFlinkConfiguration()));
        return Optional.of(evidence);
    }
}
