package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkJobControl;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.flink.FlinkRestTimeoutException;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Establishes the v1 terminal write fence: finish the bounded job, then stop every Flink process.
 * Kafka validation may begin only after this object returns successfully.
 */
public final class FlinkTerminalWriteFence {
    static final Duration PROCESS_FENCE_TIMEOUT = Duration.ofMinutes(2);

    private final FlinkJobControl jobs;
    private final FlinkProcessFence processes;

    public FlinkTerminalWriteFence(FlinkJobControl jobs, FlinkProcessFence processes) {
        this.jobs = Objects.requireNonNull(jobs, "jobs");
        this.processes = Objects.requireNonNull(processes, "processes");
    }

    public Evidence awaitBoundedCompletion(FlinkJobHandle job, Duration timeout)
            throws TerminalWriteFenceException {
        Objects.requireNonNull(job, "job");
        final FlinkJobState state;
        try {
            state = jobs.awaitFinished(job, timeout);
        } catch (Exception failure) {
            throw terminalizationFailure(failure, "Flink bounded completion failed");
        }
        if (state != FlinkJobState.FINISHED) {
            throw terminalizationFailure(
                    new IllegalStateException(
                            "Flink bounded completion returned " + state + " instead of FINISHED"),
                    "Flink bounded completion did not establish FINISHED");
        }
        FlinkProcessWriteFenceEvidence processEvidence = stopProcesses();
        return new Evidence(state, processEvidence);
    }

    private TerminalWriteFenceException terminalizationFailure(
            Exception primary,
            String message) {
        boolean processFenceFailed = false;
        FlinkProcessWriteFenceEvidence processEvidence = null;
        try {
            processEvidence = Objects.requireNonNull(
                    processes.stopAllFlinkProcesses(PROCESS_FENCE_TIMEOUT),
                    "Flink process fence returned null evidence");
        } catch (RuntimeException fenceFailure) {
            processFenceFailed = true;
            primary.addSuppressed(fenceFailure);
        }
        String reason = processFenceFailed
                ? "verification.flink.process-fence-failed"
                : primary instanceof FlinkRestTimeoutException
                        ? "verification.flink.job-completion-timeout"
                        : "verification.flink.job-terminalization-failed";
        return new TerminalWriteFenceException(
                reason,
                message,
                primary,
                Optional.ofNullable(processEvidence));
    }

    private FlinkProcessWriteFenceEvidence stopProcesses()
            throws TerminalWriteFenceException {
        try {
            return Objects.requireNonNull(
                    processes.stopAllFlinkProcesses(PROCESS_FENCE_TIMEOUT),
                    "Flink process fence returned null evidence");
        } catch (RuntimeException failure) {
            throw new TerminalWriteFenceException(
                    "verification.flink.process-fence-failed",
                    "Flink reached a terminal state but its processes could not be fenced",
                    failure);
        }
    }

    @FunctionalInterface
    public interface FlinkProcessFence {
        /**
         * Proves process death within the supplied timeout. Physical resource cleanup may happen
         * later during normal attempt cleanup.
         */
        FlinkProcessWriteFenceEvidence stopAllFlinkProcesses(Duration timeout);
    }

    public record Evidence(
            FlinkJobState finalState,
            FlinkProcessWriteFenceEvidence processFenceEvidence) {
        public Evidence {
            Objects.requireNonNull(finalState, "finalState");
            Objects.requireNonNull(processFenceEvidence, "processFenceEvidence");
            if (finalState != FlinkJobState.FINISHED) {
                throw new IllegalArgumentException("A successful write fence requires FINISHED");
            }
        }
    }
}
