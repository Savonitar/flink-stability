package org.savonitar.flink.stability.testcontainers;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/** One monotonic deadline shared by every driver call in one container operation. */
final class ContainerOperationDeadline {
    private final String scope;
    private final Duration timeout;
    private final long timeoutNanos;
    private final LongSupplier monotonicNanos;
    private final long startedAtNanos;

    static ContainerOperationDeadline start(
            String scope,
            Duration timeout,
            LongSupplier monotonicNanos) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Container operation timeout must be positive");
        }
        final long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "Container operation timeout is too large: " + timeout, overflow);
        }
        return new ContainerOperationDeadline(
                scope,
                timeout,
                timeoutNanos,
                Objects.requireNonNull(monotonicNanos, "monotonicNanos"));
    }

    private ContainerOperationDeadline(
            String scope,
            Duration timeout,
            long timeoutNanos,
            LongSupplier monotonicNanos) {
        this.scope = scope;
        this.timeout = timeout;
        this.timeoutNanos = timeoutNanos;
        this.monotonicNanos = monotonicNanos;
        this.startedAtNanos = monotonicNanos.getAsLong();
    }

    Duration remaining(String operation) {
        long elapsed = monotonicNanos.getAsLong() - startedAtNanos;
        // nanoTime has an arbitrary (possibly negative) origin and wraps as a signed long.
        // Subtraction remains correct across that wrap for intervals below 2^63 nanos. A
        // negative result instead means the supplied clock moved backwards or the interval is
        // no longer representable; fail closed rather than extending the deadline.
        if (elapsed < 0L || elapsed >= timeoutNanos) {
            throw timedOut(operation, null);
        }
        return Duration.ofNanos(timeoutNanos - elapsed);
    }

    ContainerOperationTimeoutException timedOut(String operation, Throwable cause) {
        return new ContainerOperationTimeoutException(
                scope, timeout, operation, cause);
    }
}
