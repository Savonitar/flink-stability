package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TaskManagerActionTimeoutException;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Testcontainers implementation for one v1 run attempt. */
public final class DockerV1AttemptRuntime implements V1AttemptRuntime {
    private static final String PRIMARY_TASK_MANAGER = "taskmanager-1";

    private final ClusterManager clusters;

    public DockerV1AttemptRuntime(Path checkpointStorageRoot) {
        this(new ClusterManager(Objects.requireNonNull(
                checkpointStorageRoot, "checkpointStorageRoot")));
    }

    DockerV1AttemptRuntime(ClusterManager clusters) {
        this.clusters = Objects.requireNonNull(clusters, "clusters");
    }

    @Override
    public KafkaRuntimeEndpoints startKafka(KafkaRuntimeTarget target) {
        return clusters.startKafka(target);
    }

    @Override
    public String startFlink(FlinkRuntimeTarget target) {
        return clusters.startFlink(target);
    }

    @Override
    public void killTaskManager(String targetName, Duration timeout)
            throws TaskManagerActionTimeoutException {
        try {
            clusters.killTaskManager(targetName, timeout);
        } catch (ContainerOperationTimeoutException failure) {
            throw new TaskManagerActionTimeoutException(
                    TaskManagerActionTimeoutException.Action.KILL, timeout, failure);
        }
    }

    @Override
    public void restartTaskManager(Duration timeout)
            throws TaskManagerActionTimeoutException {
        try {
            clusters.restartTaskManager(PRIMARY_TASK_MANAGER, timeout);
        } catch (ContainerOperationTimeoutException failure) {
            throw new TaskManagerActionTimeoutException(
                    TaskManagerActionTimeoutException.Action.RESTART, timeout, failure);
        }
    }

    @Override
    public FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout) {
        return clusters.establishFlinkProcessWriteFence(timeout);
    }

    @Override
    public List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence() {
        return clusters.provisioningHistory();
    }

    @Override
    public void close() {
        clusters.close();
    }
}
