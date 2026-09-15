package org.savonitar.flink.stability.core.execution;

import java.time.Duration;

/** Injectable wait boundary that keeps phase tests independent of wall-clock time. */
@FunctionalInterface
public interface PhaseSleeper {
    void sleep(Duration duration) throws InterruptedException;
}
