package org.savonitar.flink.stability.testcontainers;

import java.time.Duration;
import java.util.Objects;

/** A synchronous container-driver operation exhausted its one shared monotonic deadline. */
public final class ContainerOperationTimeoutException extends IllegalStateException {
    private final String scope;
    private final Duration timeout;
    private final String operation;

    ContainerOperationTimeoutException(
            String scope,
            Duration timeout,
            String operation,
            Throwable cause) {
        super("Timed out " + requireNonBlank(scope, "scope")
                + " after " + Objects.requireNonNull(timeout, "timeout")
                + " while " + requireNonBlank(operation, "operation"), cause);
        this.scope = scope;
        this.timeout = timeout;
        this.operation = operation;
    }

    public String scope() {
        return scope;
    }

    public Duration timeout() {
        return timeout;
    }

    public String operation() {
        return operation;
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
