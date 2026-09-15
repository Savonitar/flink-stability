package org.savonitar.flink.stability.core.execution.plan;

import org.savonitar.flink.stability.core.artifact.PreparedConnectorBundle;
import org.savonitar.flink.stability.core.artifact.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.artifact.ResolvedArtifact;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;

import java.util.Objects;

/**
 * Executable plan bound to immutable prepared artifact and connector snapshots.
 *
 * <p>This object deliberately does not implement {@link AutoCloseable}: the caller continues to
 * own {@link #preparedScenarioPlan()} and must keep it open for the complete execution. Retaining
 * it here prevents accidental garbage-collection of the workspace owner but does not introduce a
 * second close owner.</p>
 */
public final class PreparedExecutableScenarioPlan {
    private final PreparedScenarioPlan preparedScenarioPlan;
    private final ExecutableScenarioPlan executablePlan;
    private final ResolvedArtifact workloadArtifact;
    private final PreparedConnectorBundle connectorBundle;
    private final FlinkRuntimeTarget flinkRuntimeTarget;

    PreparedExecutableScenarioPlan(
            PreparedScenarioPlan preparedScenarioPlan,
            ExecutableScenarioPlan executablePlan,
            ResolvedArtifact workloadArtifact,
            PreparedConnectorBundle connectorBundle,
            FlinkRuntimeTarget flinkRuntimeTarget) {
        this.preparedScenarioPlan = Objects.requireNonNull(
                preparedScenarioPlan, "preparedScenarioPlan");
        this.executablePlan = Objects.requireNonNull(executablePlan, "executablePlan");
        this.workloadArtifact = Objects.requireNonNull(workloadArtifact, "workloadArtifact");
        this.connectorBundle = Objects.requireNonNull(connectorBundle, "connectorBundle");
        this.flinkRuntimeTarget = Objects.requireNonNull(
                flinkRuntimeTarget, "flinkRuntimeTarget");
        if (preparedScenarioPlan.scenarioPlan() != executablePlan.sourcePlan()) {
            throw new IllegalArgumentException(
                    "Prepared and executable plans must share the exact resolved scenario plan");
        }
        if (!flinkRuntimeTarget.imageReference().equals(executablePlan.flink().imageReference())) {
            throw new IllegalArgumentException("Runtime target image differs from executable plan");
        }
    }

    public PreparedScenarioPlan preparedScenarioPlan() {
        return preparedScenarioPlan;
    }

    public ExecutableScenarioPlan executablePlan() {
        return executablePlan;
    }

    public ResolvedArtifact workloadArtifact() {
        return workloadArtifact;
    }

    public PreparedConnectorBundle connectorBundle() {
        return connectorBundle;
    }

    public FlinkRuntimeTarget flinkRuntimeTarget() {
        return flinkRuntimeTarget;
    }
}
