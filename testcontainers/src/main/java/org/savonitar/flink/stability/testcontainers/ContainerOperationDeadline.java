package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/** One monotonic deadline shared by every driver call in one container operation. */
final class ContainerOperationDeadline {
    private final String scope;
    private final MonotonicDeadline budget;

    private ContainerOperationDeadline(String scope, MonotonicDeadline budget) {
        this.scope = scope;
        this.budget = budget;
    }

    static ContainerOperationDeadline start(
            String scope,
            Duration timeout,
            LongSupplier monotonicNanos) {
        return new ContainerOperationDeadline(
                Objects.requireNonNull(scope, "scope"),
                MonotonicDeadline.start(timeout, monotonicNanos));
    }

    Duration remaining(String operation) {
        return budget.remainingOrThrow(() -> timedOut(operation, null));
    }

    ContainerOperationTimeoutException timedOut(String operation, Throwable cause) {
        return new ContainerOperationTimeoutException(
                scope, budget.timeout(), operation, cause);
    }
}
