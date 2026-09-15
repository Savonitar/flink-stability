package org.savonitar.flink.stability.core.execution;

import java.time.Duration;
import java.util.Objects;

/** The runner stopped waiting for physical attempt cleanup after its fixed safety bound. */
final class AttemptCleanupTimeoutException extends RuntimeException {
    AttemptCleanupTimeoutException(Duration timeout, Throwable cause) {
        super(
                "Attempt cleanup did not finish within "
                        + Objects.requireNonNull(timeout, "timeout"),
                cause);
    }
}
