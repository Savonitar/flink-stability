package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContainerDriverCallBoundaryTest {

    @Test
    void submissionOverheadConsumesTheFixedDeadlineAndCancelsQueuedWork() {
        AtomicLong monotonicNanos = new AtomicLong(10L);
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "strict action", Duration.ofNanos(5), monotonicNanos::get);
        AtomicReference<FutureTask<String>> submitted = new AtomicReference<>();
        Function<Callable<String>, java.util.concurrent.Future<String>> submitter = call -> {
            FutureTask<String> future = new FutureTask<>(call);
            submitted.set(future);
            monotonicNanos.addAndGet(5L);
            return future;
        };

        ContainerOperationTimeoutException timeout = assertThrows(
                ContainerOperationTimeoutException.class,
                () -> ContainerDriverCallBoundary.call(
                        deadline,
                        "submitting driver call",
                        () -> "late",
                        submitter));

        assertEquals("submitting driver call", timeout.operation());
        assertTrue(submitted.get().isCancelled());
    }

    @Test
    void precheckDoesNotSubmitWorkAfterTheDeadline() {
        AtomicLong monotonicNanos = new AtomicLong(20L);
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "expired action", Duration.ofNanos(5), monotonicNanos::get);
        monotonicNanos.addAndGet(5L);
        AtomicReference<FutureTask<String>> submitted = new AtomicReference<>();

        assertThrows(
                ContainerOperationTimeoutException.class,
                () -> ContainerDriverCallBoundary.call(
                        deadline,
                        "late driver call",
                        () -> "never",
                        call -> {
                            FutureTask<String> future = new FutureTask<>(call);
                            submitted.set(future);
                            return future;
                        }));

        assertNull(submitted.get());
    }
}
