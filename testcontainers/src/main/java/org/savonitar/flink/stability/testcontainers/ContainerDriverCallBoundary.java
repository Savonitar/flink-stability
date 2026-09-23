package org.savonitar.flink.stability.testcontainers;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** Deadline-aware boundary for synchronous container-driver calls. */
final class ContainerDriverCallBoundary {
    private static final AtomicLong THREAD_ORDINAL = new AtomicLong();
    private static final ExecutorService DRIVER_CALLS = Executors.newCachedThreadPool(
            daemonThreadFactory());

    private ContainerDriverCallBoundary() {
    }

    static void run(
            ContainerOperationDeadline deadline,
            String operation,
            Runnable call) {
        call(deadline, operation, () -> {
            call.run();
            return null;
        });
    }

    static <T> T call(
            ContainerOperationDeadline deadline,
            String operation,
            Callable<T> call) {
        return call(deadline, operation, call, DRIVER_CALLS::submit);
    }

    static <T> T call(
            ContainerOperationDeadline deadline,
            String operation,
            Callable<T> call,
            Function<Callable<T>, Future<T>> submitter) {
        Objects.requireNonNull(deadline, "deadline");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(call, "call");
        Objects.requireNonNull(submitter, "submitter");

        // Reject work that is already late. Then charge executor submission/queueing overhead to
        // the same fixed deadline by reading the clock again immediately after submit.
        deadline.remaining(operation);
        Future<T> future = Objects.requireNonNull(
                submitter.apply(call), "driver call submitter returned null");
        final Duration remaining;
        try {
            remaining = deadline.remaining(operation);
        } catch (ContainerOperationTimeoutException timeout) {
            future.cancel(true);
            throw timeout;
        }
        try {
            return future.get(remaining.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException timeout) {
            future.cancel(true);
            throw deadline.timedOut(operation, timeout);
        } catch (InterruptedException interrupted) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while " + operation,
                    interrupted);
        } catch (ExecutionException failed) {
            throwUnchecked(failed.getCause(), operation);
            throw new AssertionError("unreachable");
        }
    }

    private static void throwUnchecked(Throwable cause, String operation) {
        if (cause instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("Failed while " + operation, cause);
    }

    private static ThreadFactory daemonThreadFactory() {
        return task -> {
            Thread thread = new Thread(
                    task,
                    "flink-container-driver-" + THREAD_ORDINAL.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
