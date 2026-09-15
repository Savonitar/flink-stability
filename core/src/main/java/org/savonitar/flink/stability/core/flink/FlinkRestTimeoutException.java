package org.savonitar.flink.stability.core.flink;

import java.io.IOException;

/** One non-resetting Flink operation deadline expired before authoritative completion. */
public final class FlinkRestTimeoutException extends IOException {
    public FlinkRestTimeoutException(String message) {
        super(message);
    }

    public FlinkRestTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
