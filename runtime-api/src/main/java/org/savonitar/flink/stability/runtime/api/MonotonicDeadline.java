package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A fixed time budget on a monotonic nanosecond clock such as {@code System::nanoTime}.
 *
 * <p>Elapsed time is a subtraction, which stays correct when the clock's arbitrary origin is
 * negative or wraps. A clock that moves backwards fails closed: the deadline counts as expired,
 * never extended. A timeout too long for nanoseconds (about 292 years) is clamped to that range.
 */
public final class MonotonicDeadline {
    private final Duration timeout;
    private final long timeoutNanos;
    private final LongSupplier nanoTime;
    private final long startedAtNanos;

    private MonotonicDeadline(Duration timeout, long timeoutNanos, LongSupplier nanoTime) {
        this.timeout = timeout;
        this.timeoutNanos = timeoutNanos;
        this.nanoTime = nanoTime;
        this.startedAtNanos = nanoTime.getAsLong();
    }

    /** Starts a positive budget now. */
    public static MonotonicDeadline start(Duration timeout, LongSupplier nanoTime) {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(nanoTime, "nanoTime");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException tooLong) {
            timeoutNanos = Long.MAX_VALUE;
        }
        return new MonotonicDeadline(timeout, timeoutNanos, nanoTime);
    }

    public Duration timeout() {
        return timeout;
    }

    /** The time left, or zero once the deadline has passed. */
    public Duration remaining() {
        long elapsed = nanoTime.getAsLong() - startedAtNanos;
        return elapsed < 0 || elapsed >= timeoutNanos
                ? Duration.ZERO
                : Duration.ofNanos(timeoutNanos - elapsed);
    }

    /** The time left, or the caller's exception once the deadline has passed. */
    public <X extends Throwable> Duration remainingOrThrow(Supplier<? extends X> expired)
            throws X {
        Duration remaining = remaining();
        if (remaining.isZero()) {
            throw expired.get();
        }
        return remaining;
    }
}
