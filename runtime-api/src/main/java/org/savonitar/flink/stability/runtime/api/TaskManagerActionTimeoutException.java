package org.savonitar.flink.stability.runtime.api;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/** A named TaskManager phase action exhausted its fixed infrastructure deadline. */
public final class TaskManagerActionTimeoutException extends IOException {
    private final Action action;
    private final Duration timeout;

    public TaskManagerActionTimeoutException(
            Action action,
            Duration timeout,
            Throwable cause) {
        super("TaskManager " + Objects.requireNonNull(action, "action").displayName
                + " timed out after " + Objects.requireNonNull(timeout, "timeout"),
                Objects.requireNonNull(cause, "cause"));
        this.action = action;
        this.timeout = timeout;
    }

    public Action action() {
        return action;
    }

    public Duration timeout() {
        return timeout;
    }

    public enum Action {
        KILL("kill"),
        RESTART("restart");

        private final String displayName;

        Action(String displayName) {
            this.displayName = displayName;
        }
    }
}
