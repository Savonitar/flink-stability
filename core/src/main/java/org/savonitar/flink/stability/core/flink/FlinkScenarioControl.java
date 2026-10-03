package org.savonitar.flink.stability.core.flink;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Typed Flink operations used by the first v1 scenario executor. */
public interface FlinkScenarioControl extends FlinkJobControl, AutoCloseable {
    String uploadJar(Path jar, String expectedSha256) throws IOException;

    FlinkJobHandle submit(FlinkJobSubmission submission) throws IOException;

    FlinkJobState awaitState(
            FlinkJobHandle job, FlinkJobState expected, Duration timeout) throws IOException;

    long awaitCompletedCheckpoints(
            FlinkJobHandle job, long minimumCompleted, Duration timeout) throws IOException;

    default String stopWithSavepoint(FlinkJobHandle job, String trigger, String directory, Duration timeout) throws IOException {
        throw new UnsupportedOperationException("Stop with savepoint unavailable");
    }
    default FlinkSavepoint.Status savepointStatus(FlinkJobHandle job, String trigger, Duration timeout) throws IOException {
        throw new UnsupportedOperationException("Savepoint status unavailable");
    }
    default FlinkSavepoint.RestoreProof restoredSavepoint(FlinkJobHandle job, Duration timeout) throws IOException {
        throw new UnsupportedOperationException("Savepoint restoration proof unavailable");
    }
    default FlinkJobHandle submit(FlinkJobSubmission submission, Duration timeout) throws IOException {
        throw new UnsupportedOperationException("Deadline-bound restored submission unavailable");
    }

    /** Every received HTTP error, including failures followed by a successful retry. */
    default List<RestError> restErrors() {
        return List.of();
    }

    /** Original response text, never a truncated exception-message preview. */
    record RestError(long sequence, String method, String endpoint, int httpStatus, String body) {
        public RestError {
            if (sequence < 1 || httpStatus < 100 || httpStatus > 599
                    || httpStatus >= 200 && httpStatus < 300) {
                throw new IllegalArgumentException("Invalid Flink REST error sequence or HTTP status");
            }
            method = requireNonBlank(method, "method");
            endpoint = requireNonBlank(endpoint, "endpoint");
            Objects.requireNonNull(body, "body");
        }
    }

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
