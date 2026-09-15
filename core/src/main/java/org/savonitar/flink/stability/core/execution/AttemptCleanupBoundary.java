package org.savonitar.flink.stability.core.execution;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/** Bounds best-effort physical attempt cleanup without retaining prepared-plan ownership. */
final class AttemptCleanupBoundary {
    private static final AtomicLong THREAD_ORDINAL = new AtomicLong();
    private static final ExecutorService CLEANUP_CALLS = Executors.newCachedThreadPool(
            daemonThreadFactory());

    private final Duration timeout;
    private final long timeoutNanos;
    private final ExecutorService cleanupCalls;

    AttemptCleanupBoundary(Duration timeout) {
        this(timeout, CLEANUP_CALLS);
    }

    AttemptCleanupBoundary(Duration timeout, ExecutorService cleanupCalls) {
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.cleanupCalls = Objects.requireNonNull(cleanupCalls, "cleanupCalls");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Attempt cleanup timeout must be positive");
        }
        try {
            this.timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "Attempt cleanup timeout is too large: " + timeout,
                    overflow);
        }
    }

    RuntimeException await(Callable<RuntimeException> cleanup) {
        Objects.requireNonNull(cleanup, "cleanup");
        boolean interruptedOnEntry = Thread.currentThread().isInterrupted();
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        Future<RuntimeException> future = null;
        long startedAtNanos = System.nanoTime();
        try {
            future = cleanupCalls.submit(() -> {
                cleanupStarted.countDown();
                return cleanup.call();
            });
            if (interruptedOnEntry) {
                if (!awaitCleanupStart(cleanupStarted, startedAtNanos)) {
                    return cleanupTimeout("Attempt cleanup worker did not start before its deadline");
                }
                future.cancel(true);
                return interruptedFailure(new InterruptedException(
                        "Caller thread was interrupted before attempt cleanup"));
            }
            long remainingNanos = remainingNanos(startedAtNanos);
            if (remainingNanos <= 0L) {
                cancelStartedCleanup(future, cleanupStarted);
                return cleanupTimeout("Attempt cleanup deadline expired");
            }
            return future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException timeoutFailure) {
            cancelStartedCleanup(future, cleanupStarted);
            return new AttemptCleanupTimeoutException(timeout, timeoutFailure);
        } catch (InterruptedException interrupted) {
            boolean started = awaitCleanupStart(cleanupStarted, startedAtNanos);
            if (started) {
                future.cancel(true);
            }
            Thread.currentThread().interrupt();
            if (!started) {
                AttemptCleanupTimeoutException timeoutFailure = cleanupTimeout(
                        "Attempt cleanup worker did not start before its deadline");
                timeoutFailure.addSuppressed(interrupted);
                return timeoutFailure;
            }
            return interruptedFailure(interrupted);
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            if (cause instanceof RuntimeException runtime) {
                return runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            return new IllegalStateException("Attempt cleanup failed", cause);
        } finally {
            if (interruptedOnEntry) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean awaitCleanupStart(
            CountDownLatch cleanupStarted, long startedAtNanos) {
        boolean interrupted = false;
        try {
            while (cleanupStarted.getCount() != 0L) {
                long remainingNanos = remainingNanos(startedAtNanos);
                if (remainingNanos <= 0L) {
                    return false;
                }
                try {
                    return cleanupStarted.await(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            return true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private long remainingNanos(long startedAtNanos) {
        return timeoutNanos - (System.nanoTime() - startedAtNanos);
    }

    private static void cancelStartedCleanup(
            Future<RuntimeException> future, CountDownLatch cleanupStarted) {
        if (cleanupStarted.getCount() == 0L) {
            future.cancel(true);
        }
    }

    private AttemptCleanupTimeoutException cleanupTimeout(String message) {
        return new AttemptCleanupTimeoutException(timeout, new TimeoutException(message));
    }

    private static IllegalStateException interruptedFailure(InterruptedException interrupted) {
        return new IllegalStateException(
                "Interrupted while waiting for attempt cleanup",
                interrupted);
    }

    private static ThreadFactory daemonThreadFactory() {
        return task -> {
            Thread thread = new Thread(
                    task,
                    "v1-attempt-cleanup-" + THREAD_ORDINAL.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
