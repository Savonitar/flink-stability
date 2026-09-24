package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ContainerOperationDeadlineTest {

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
