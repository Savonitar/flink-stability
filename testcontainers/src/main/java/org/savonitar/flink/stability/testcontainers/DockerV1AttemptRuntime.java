package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;
import org.savonitar.flink.stability.runtime.api.KafkaProxyEndpoint;
import org.savonitar.flink.stability.runtime.api.KafkaProxyTarget;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.TaskManagerActionTimeoutException;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    public KafkaProxyEndpoint startKafkaProxy(KafkaProxyTarget target) {
        return clusters.startKafkaProxy(target);
    }

    @Override
    public String startFlink(FlinkRuntimeTarget target) {
        return clusters.startFlink(target);
    }

    @Override
    public String currentFlinkRestEndpoint(Duration timeout) {
        return clusters.currentFlinkRestEndpoint(timeout);
    }

    @Override
    public FlinkHaControl.LeaderFaultEvidence faultLeader(
            FlinkHaControl.LeaderFaultRequest request, Duration remainingBudget) {
        return clusters.faultLeader(request, remainingBudget);
    }

    @Override
    public Optional<String> tokenPluginSha256() {
        return clusters.tokenPluginSha256();
    }

    @Override
    public Optional<TokenServiceControl.Snapshot> tokenServiceEvidence() {
        return clusters.tokenServiceEvidence();
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
        restartTaskManager(PRIMARY_TASK_MANAGER, timeout);
    }

    @Override
    public void restartTaskManager(String targetName, Duration timeout)
            throws TaskManagerActionTimeoutException {
        try {
            clusters.restartTaskManager(targetName, timeout);
        } catch (ContainerOperationTimeoutException failure) {
            throw new TaskManagerActionTimeoutException(
                    TaskManagerActionTimeoutException.Action.RESTART, timeout, failure);
        }
    }

    @Override
    public Optional<Identity> taskManagerIdentity(String targetName) {
        return clusters.taskManagerIdentity(targetName);
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
    public List<FlinkClassLoadLog> flinkClassLoadLogs() {
        return clusters.classLoadLogs();
    }

    @Override
    public void close() {
        clusters.close();
    }
}
