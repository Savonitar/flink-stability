package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
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
import java.util.concurrent.TimeoutException;

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
    public String currentFlinkRestEndpoint(Duration timeout) throws TimeoutException {
        try {
            return clusters.currentFlinkRestEndpoint(timeout);
        } catch (ContainerOperationTimeoutException failure) {
            TimeoutException expired = new TimeoutException(failure.getMessage());
            expired.initCause(failure);
            throw expired;
        }
    }

    @Override
    public Optional<FlinkHaControl.Observations> haObservations() {
        return clusters.haObservations();
    }

    @Override
    public FlinkHaControl.LeaderFaultEvidence faultLeader(
            FlinkHaControl.LeaderFaultRequest request, Duration remainingBudget) {
        return clusters.faultLeader(request, remainingBudget);
    }

    @Override
    public java.util.List<org.savonitar.flink.stability.runtime.api.FlinkComponentLog> flinkComponentLogs() {
        return clusters.componentLogs();
    }

    @Override
    public FlinkHaControl.LeaderFaultEvidence faultLeader(
            FlinkHaControl.LeaderFaultRequest request, Duration remainingBudget,
            TokenServiceControl.JobTarget target) {
        return clusters.faultLeader(request, remainingBudget, Optional.of(target));
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
    public Optional<Identity> taskManagerIdentity(String targetName, Duration timeout) {
        return clusters.taskManagerIdentity(targetName, timeout);
    }

    @Override
    public FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout) {
        return clusters.establishFlinkProcessWriteFence(timeout);
    }

    @Override
    public Optional<FlinkProcessWriteFenceEvidence.Observations> flinkProcessObservations() {
        return Optional.of(clusters.processObservations());
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
    public java.util.List<org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence> brokerFault(
            org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Request request,
            java.util.List<org.savonitar.flink.stability.runtime.api.KafkaLogCapture.Partition> partitions) {
        return clusters.brokerFault(request, partitions);
    }

    public org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Evidence brokerOperation(String name, boolean restart,
            List<KafkaLogCapture.Partition> partitions, Duration timeout) {
        return clusters.brokerOperation(name, restart, partitions, timeout);
    }

    public KafkaLogCapture captureKafkaLogs(
            List<KafkaLogCapture.Partition> partitions,
            Path directory, MonotonicDeadline deadline) {
        return clusters.captureKafkaLogs(partitions, directory, deadline);
    }
}
