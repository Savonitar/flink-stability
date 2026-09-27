package org.savonitar.flink.stability.core.flink;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

/** Typed Flink operations used by the first v1 scenario executor. */
public interface FlinkScenarioControl extends FlinkJobControl, AutoCloseable {
    String uploadJar(Path jar, String expectedSha256) throws IOException;

    FlinkJobHandle submit(FlinkJobSubmission submission) throws IOException;

    FlinkJobState awaitState(
            FlinkJobHandle job, FlinkJobState expected, Duration timeout) throws IOException;

    long awaitCompletedCheckpoints(
            FlinkJobHandle job, long minimumCompleted, Duration timeout) throws IOException;

    /** One POST only; an unknown submission outcome must never be replayed. */
    default String triggerCheckpoint(FlinkJobHandle job, String triggerId, Duration timeout) throws IOException {
        throw new UnsupportedOperationException("Explicit checkpoint triggering is unsupported");
    }

    /** Polls the exact previously submitted trigger, never a checkpoint count. */
    default FlinkCheckpointTrigger.Observation checkpointStatus(
            FlinkJobHandle job, String triggerId, Duration timeout) throws IOException {
        throw new UnsupportedOperationException("Explicit checkpoint status is unsupported");
    }

    @Override
    void close();
}
