package org.savonitar.flink.stability.core.execution.kafka;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/** A single monotonic budget for all Kafka input-preparation work, including cleanup. */
final class KafkaInputPreparationDeadline {

    private final long startedAtNanos;
    private final long timeoutNanos;
    private final LongSupplier nanoTime;

    KafkaInputPreparationDeadline(Duration timeout, LongSupplier nanoTime) {
        Objects.requireNonNull(timeout, "timeout");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        try {
            this.timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "Kafka input preparation timeout is too large: " + timeout,
                    overflow);
        }
        this.startedAtNanos = nanoTime.getAsLong();
    }

    Duration remaining() {
        return remaining(startedAtNanos, nanoTime.getAsLong(), timeoutNanos);
    }

    Duration requireRemaining(String operation)
            throws KafkaInputPreparationDeadlineExceededException {
        Duration remaining = remaining();
        if (remaining.isZero()) {
            throw new KafkaInputPreparationDeadlineExceededException(operation);
        }
        return remaining;
    }

    KafkaInputPreparationDeadlineExceededException exceeded(
            String operation, Throwable cause) {
        return new KafkaInputPreparationDeadlineExceededException(operation, cause);
    }

    static Duration remaining(long startedAtNanos, long nowNanos, long timeoutNanos) {
        long elapsed = nowNanos - startedAtNanos;
        return elapsed < 0 || elapsed >= timeoutNanos
                ? Duration.ZERO
                : Duration.ofNanos(timeoutNanos - elapsed);
    }

    static final class KafkaInputPreparationDeadlineExceededException extends Exception {
        private KafkaInputPreparationDeadlineExceededException(String operation) {
            super("Kafka input preparation timed out before " + operation);
        }

        private KafkaInputPreparationDeadlineExceededException(
                String operation, Throwable cause) {
            super("Kafka input preparation timed out while " + operation, cause);
        }
    }
}
