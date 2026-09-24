package org.savonitar.flink.stability.core.execution.kafka;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.time.Duration;
import java.util.function.LongSupplier;

/** A single monotonic budget for all Kafka input-preparation work, including cleanup. */
final class KafkaInputPreparationDeadline {
    private final MonotonicDeadline budget;

    KafkaInputPreparationDeadline(Duration timeout, LongSupplier nanoTime) {
        this.budget = MonotonicDeadline.start(timeout, nanoTime);
    }

    Duration remaining() {
        return budget.remaining();
    }

    Duration requireRemaining(String operation)
            throws KafkaInputPreparationDeadlineExceededException {
        return budget.remainingOrThrow(
                () -> new KafkaInputPreparationDeadlineExceededException(operation));
    }

    KafkaInputPreparationDeadlineExceededException exceeded(
            String operation, Throwable cause) {
        return new KafkaInputPreparationDeadlineExceededException(operation, cause);
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
