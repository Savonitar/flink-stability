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

    @Override
    void close();
}
