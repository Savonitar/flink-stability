package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ContainerOperationDeadlineTest {

    @Test
    void sharedAdapterKeepsTheOriginalElapsedBudget() {
        long[] now = {100L};
        var caller = org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(
                Duration.ofNanos(10), () -> now[0]);
        now[0] = 107;
        var deadline = ContainerOperationDeadline.shared("owned capture", caller);
        assertEquals(Duration.ofNanos(3), deadline.remaining("copy"));
        now[0] = 110;
        assertThrows(ContainerOperationTimeoutException.class, () -> deadline.remaining("copy"));
    }

    @Test
    void anExpiredDeadlineNamesTheScopeTimeoutAndOperation() {
        long[] now = {100L};
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "stopping taskmanager-1", Duration.ofNanos(10), () -> now[0]);
        assertEquals(Duration.ofNanos(10), deadline.remaining("checking liveness"));
        now[0] += 10L;

        ContainerOperationTimeoutException timeout = assertThrows(
                ContainerOperationTimeoutException.class,
                () -> deadline.remaining("confirming process termination"));

        assertEquals("stopping taskmanager-1", timeout.scope());
        assertEquals(Duration.ofNanos(10), timeout.timeout());
        assertEquals("confirming process termination", timeout.operation());
    }

    @Test
    void aTimedOutDriverCallKeepsItsCause() {
        ContainerOperationDeadline deadline = ContainerOperationDeadline.start(
                "starting kafka", Duration.ofSeconds(1), System::nanoTime);
        RuntimeException cause = new RuntimeException("driver still blocked");

        ContainerOperationTimeoutException timeout = deadline.timedOut("pulling image", cause);

        assertEquals("pulling image", timeout.operation());
        assertSame(cause, timeout.getCause());
    }
}
