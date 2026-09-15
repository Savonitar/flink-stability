package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.util.Objects;
import java.util.Optional;

/** Failure to establish the required no-more-Flink-writes boundary before validation. */
public final class TerminalWriteFenceException extends Exception {
    private final String reason;
    private final Optional<FlinkProcessWriteFenceEvidence> processFenceEvidence;

    public TerminalWriteFenceException(String reason, String message, Throwable cause) {
        this(reason, message, cause, Optional.empty());
    }

    public TerminalWriteFenceException(
            String reason,
            String message,
            Throwable cause,
            Optional<FlinkProcessWriteFenceEvidence> processFenceEvidence) {
        super(message, cause);
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason must not be blank");
        }
        this.reason = reason;
        this.processFenceEvidence = Objects.requireNonNull(
                processFenceEvidence, "processFenceEvidence");
    }

    public String reason() {
        return reason;
    }

    /** Present when forced process fencing succeeded after job terminalization failed. */
    public Optional<FlinkProcessWriteFenceEvidence> processFenceEvidence() {
        return processFenceEvidence;
    }
}
