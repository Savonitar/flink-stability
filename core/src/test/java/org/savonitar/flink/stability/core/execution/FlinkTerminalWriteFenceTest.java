package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.flink.FlinkJobControl;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.flink.FlinkRestTimeoutException;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkTerminalWriteFenceTest {
    private static final FlinkJobHandle JOB = new FlinkJobHandle("job-1");

    @Test
    void boundedJobFinishesBeforeEveryFlinkProcessIsFenced() throws Exception {
        List<String> events = new ArrayList<>();
        FakeJobs jobs = new FakeJobs(events);
        FlinkTerminalWriteFence fence = new FlinkTerminalWriteFence(
                jobs, timeout -> recordProcessFence(events, timeout));

        FlinkTerminalWriteFence.Evidence evidence =
                fence.awaitBoundedCompletion(JOB, Duration.ofMinutes(2));

        assertEquals(List.of("await-finished", "fence-processes"), events);
        assertEquals(FlinkJobState.FINISHED, evidence.finalState());
        assertEquals(processFenceEvidence(), evidence.processFenceEvidence());
    }

    @Test
    void completionTimeoutStillForceFencesFlinkAndFailsTheExperiment() {
        List<String> events = new ArrayList<>();
        FakeJobs jobs = new FakeJobs(events);
        jobs.awaitFailure = new FlinkRestTimeoutException("deadline");
        FlinkTerminalWriteFence fence = new FlinkTerminalWriteFence(
                jobs, timeout -> recordProcessFence(events, timeout));

        TerminalWriteFenceException failure = assertThrows(
                TerminalWriteFenceException.class,
                () -> fence.awaitBoundedCompletion(JOB, Duration.ofMinutes(2)));

        assertEquals("verification.flink.job-completion-timeout", failure.reason());
        assertEquals(List.of("await-finished", "fence-processes"), events);
        assertEquals(Optional.of(processFenceEvidence()),
                failure.processFenceEvidence());
    }

    @Test
    void nonFinishedBoundedResultStillForceFencesAndPreservesEvidence() {
        List<String> events = new ArrayList<>();
        FakeJobs jobs = new FakeJobs(events);
        jobs.awaitResult = FlinkJobState.CANCELED;
        FlinkTerminalWriteFence fence = new FlinkTerminalWriteFence(
                jobs, timeout -> recordProcessFence(events, timeout));

        TerminalWriteFenceException failure = assertThrows(
                TerminalWriteFenceException.class,
                () -> fence.awaitBoundedCompletion(JOB, Duration.ofMinutes(2)));

        assertEquals("verification.flink.job-terminalization-failed", failure.reason());
        assertEquals(List.of("await-finished", "fence-processes"), events);
        assertEquals(Optional.of(processFenceEvidence()), failure.processFenceEvidence());
        assertTrue(failure.getCause().getMessage().contains("CANCELED instead of FINISHED"));
    }

    @Test
    void processFenceFailurePreventsKafkaValidationBoundary() {
        List<String> events = new ArrayList<>();
        FakeJobs jobs = new FakeJobs(events);
        FlinkTerminalWriteFence fence = new FlinkTerminalWriteFence(jobs, timeout -> {
            assertEquals(FlinkTerminalWriteFence.PROCESS_FENCE_TIMEOUT, timeout);
            events.add("fence-processes");
            throw new IllegalStateException("taskmanager still alive");
        });

        TerminalWriteFenceException failure = assertThrows(
                TerminalWriteFenceException.class,
                () -> fence.awaitBoundedCompletion(JOB, Duration.ofMinutes(2)));

        assertEquals("verification.flink.process-fence-failed", failure.reason());
        assertEquals(List.of("await-finished", "fence-processes"), events);
        assertTrue(failure.processFenceEvidence().isEmpty());
    }

    @Test
    void terminalizationAndFenceFailuresPreserveBothCauses() {
        List<String> events = new ArrayList<>();
        FakeJobs jobs = new FakeJobs(events);
        jobs.awaitFailure = new IOException("job status unavailable");
        FlinkTerminalWriteFence fence = new FlinkTerminalWriteFence(jobs, timeout -> {
            assertEquals(FlinkTerminalWriteFence.PROCESS_FENCE_TIMEOUT, timeout);
            events.add("fence-processes");
            throw new IllegalStateException("docker stop failed");
        });

        TerminalWriteFenceException failure = assertThrows(
                TerminalWriteFenceException.class,
                () -> fence.awaitBoundedCompletion(JOB, Duration.ofMinutes(2)));

        assertEquals("verification.flink.process-fence-failed", failure.reason());
        assertEquals(1, failure.getCause().getSuppressed().length);
        assertTrue(failure.getCause().getSuppressed()[0].getMessage().contains("docker stop"));
        assertTrue(failure.processFenceEvidence().isEmpty());
    }

    @Test
    void nullForcedFenceEvidenceIsAFenceFailureRatherThanAnUnfencedTimeout() {
        FakeJobs jobs = new FakeJobs(new ArrayList<>());
        jobs.awaitFailure = new FlinkRestTimeoutException("deadline");
        FlinkTerminalWriteFence fence = new FlinkTerminalWriteFence(jobs, timeout -> null);

        TerminalWriteFenceException failure = assertThrows(
                TerminalWriteFenceException.class,
                () -> fence.awaitBoundedCompletion(JOB, Duration.ofMinutes(2)));

        assertEquals("verification.flink.process-fence-failed", failure.reason());
        assertTrue(failure.processFenceEvidence().isEmpty());
        assertEquals(1, failure.getCause().getSuppressed().length);
        assertTrue(failure.getCause().getSuppressed()[0].getMessage()
                .contains("null evidence"));
    }

    private static FlinkProcessWriteFenceEvidence recordProcessFence(
            List<String> events, Duration timeout) {
        assertEquals(FlinkTerminalWriteFence.PROCESS_FENCE_TIMEOUT, timeout);
        events.add("fence-processes");
        return processFenceEvidence();
    }

    private static FlinkProcessWriteFenceEvidence processFenceEvidence() {
        return new FlinkProcessWriteFenceEvidence(
                List.of(new FlinkProcessWriteFenceEvidence.Component(
                        "taskmanager-1",
                        FlinkComponentRole.TASK_MANAGER,
                        Optional.of("tm-runtime-1"),
                        FlinkProcessWriteFenceEvidence.Outcome.SIGKILLED)),
                Instant.parse("2026-08-26T12:00:00Z"));
    }

    private static final class FakeJobs implements FlinkJobControl {
        private final List<String> events;
        private IOException awaitFailure;
        private FlinkJobState awaitResult = FlinkJobState.FINISHED;

        private FakeJobs(List<String> events) {
            this.events = events;
        }

        @Override
        public FlinkJobState jobState(FlinkJobHandle job) {
            return FlinkJobState.FINISHED;
        }

        @Override
        public FlinkJobState awaitFinished(FlinkJobHandle job, Duration timeout)
                throws IOException {
            events.add("await-finished");
            if (awaitFailure != null) {
                throw awaitFailure;
            }
            return awaitResult;
        }

    }
}
