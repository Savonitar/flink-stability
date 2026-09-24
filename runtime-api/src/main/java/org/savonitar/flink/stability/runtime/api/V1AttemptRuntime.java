package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;

/** Infrastructure boundary owned by one isolated v1 attempt. */
public interface V1AttemptRuntime extends TaskManagerControl, AutoCloseable {
    KafkaRuntimeEndpoints startKafka(KafkaRuntimeTarget target);

    String startFlink(FlinkRuntimeTarget target) throws Exception;

    FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout);

    List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence();

    /**
     * Class-load logs written so far by every Flink process of this attempt, including killed
     * and replaced ones. Complete only after the process fence.
     */
    List<FlinkClassLoadLog> flinkClassLoadLogs();

    /**
     * Releases physical attempt infrastructure only. Implementations may read prepared artifact
     * snapshots while constructing both initial and replacement Flink containers, so the prepared
     * plan owner must remain alive through phase execution. Cleanup may be detached after its
     * caller's deadline and therefore must not dereference those files.
     */
    @Override
    void close();
}
