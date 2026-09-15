package org.savonitar.flink.stability.core.execution.kafka;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;

import java.util.Objects;

/** Binds the capability-compiled Kafka plan to the shared runtime contract. */
public final class KafkaRuntimeTargetFactory {

    private KafkaRuntimeTargetFactory() {}

    public static KafkaRuntimeTarget from(ExecutableScenarioPlan plan) {
        Objects.requireNonNull(plan, "plan");
        return from(plan.kafka());
    }

    public static KafkaRuntimeTarget from(ExecutableScenarioPlan.KafkaCluster cluster) {
        Objects.requireNonNull(cluster, "cluster");
        return new KafkaRuntimeTarget(
                cluster.alias(),
                cluster.imageReference(),
                cluster.brokerPolicy());
    }
}
