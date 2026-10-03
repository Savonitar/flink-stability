package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Infrastructure boundary owned by one isolated v1 attempt. */
public interface V1AttemptRuntime extends KafkaBrokerControl, TaskManagerControl, FlinkHaControl, AutoCloseable {
    KafkaRuntimeEndpoints startKafka(KafkaRuntimeTarget target);

    /**
     * Starts a protocol-aware proxy in front of the started Kafka cluster. Its rules and evidence
     * live in the returned control directory, which exists only for this attempt.
     */
    KafkaProxyEndpoint startKafkaProxy(KafkaProxyTarget target);

    String startFlink(FlinkRuntimeTarget target) throws Exception;

    FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout);

    List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence();

    /** Process observations captured before cleanup, including an incomplete process fence. */
    default Optional<FlinkProcessWriteFenceEvidence.Observations> flinkProcessObservations() {
        return Optional.empty();
    }

    /**
     * Class-load logs written so far by every Flink process of this attempt, including killed
     * and replaced ones. Complete only after the process fence.
     */
    List<FlinkClassLoadLog> flinkClassLoadLogs();

    default List<FlinkComponentLog> flinkComponentLogs() { return List.of(); }

    default Optional<String> tokenPluginSha256() {
        return Optional.empty();
    }

    default Optional<TokenServiceControl.Snapshot> tokenServiceEvidence() {
        return Optional.empty();
    }

    /**
     * Releases physical attempt infrastructure only. Implementations may read prepared artifact
     * snapshots while constructing both initial and replacement Flink containers, so the prepared
     * plan owner must remain alive through phase execution. Cleanup may be detached after its
     * caller's deadline and therefore must not dereference those files.
     */
    @Override
    void close();
    default KafkaLogCapture captureKafkaLogs(
            List<KafkaLogCapture.Partition> partitions,
            Path directory, MonotonicDeadline deadline) {
        throw new UnsupportedOperationException("Owned Kafka log collection is unavailable");
    }
}
