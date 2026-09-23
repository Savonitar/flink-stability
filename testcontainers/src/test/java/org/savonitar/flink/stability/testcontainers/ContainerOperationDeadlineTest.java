package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ContainerOperationDeadlineTest {

    @Test
    void computesRemainingTimeAcrossNegativeAndWrappedNanoTimeOrigins() {
        MutableNanoClock negative = new MutableNanoClock(-500L);
        ContainerOperationDeadline negativeDeadline = ContainerOperationDeadline.start(
                "negative-origin action", Duration.ofNanos(100), negative::nanoTime);
        negative.advance(50L);

        assertEquals(
                Duration.ofNanos(50),
                negativeDeadline.remaining("checking negative-origin action"));

        MutableNanoClock wrapped = new MutableNanoClock(Long.MAX_VALUE - 10L);
        ContainerOperationDeadline wrappedDeadline = ContainerOperationDeadline.start(
                "wrapped action", Duration.ofNanos(100), wrapped::nanoTime);
        wrapped.advance(21L);

        assertEquals(
                Duration.ofNanos(79),
                wrappedDeadline.remaining("checking wrapped action"));
    }

    @Test
    void expiresAtTheExactBoundaryAndFailsClosedWhenClockMovesBackward() {
        MutableNanoClock exact = new MutableNanoClock(100L);
        ContainerOperationDeadline exactDeadline = ContainerOperationDeadline.start(
                "exact action", Duration.ofNanos(10), exact::nanoTime);
        exact.advance(10L);

        ContainerOperationTimeoutException exactTimeout = assertThrows(
                ContainerOperationTimeoutException.class,
                () -> exactDeadline.remaining("exact boundary"));
        assertEquals("exact boundary", exactTimeout.operation());

        MutableNanoClock backwards = new MutableNanoClock(100L);
        ContainerOperationDeadline backwardsDeadline = ContainerOperationDeadline.start(
                "backwards action", Duration.ofNanos(10), backwards::nanoTime);
        backwards.advance(-1L);

        assertThrows(
                ContainerOperationTimeoutException.class,
                () -> backwardsDeadline.remaining("backwards clock"));
    }

    @Test
    void rejectsNonPositiveAndNanosecondOverflowingTimeouts() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ContainerOperationDeadline.start(
                        "zero", Duration.ZERO, System::nanoTime));
        assertThrows(
                IllegalArgumentException.class,
                () -> ContainerOperationDeadline.start(
                        "negative", Duration.ofNanos(-1), System::nanoTime));
        assertThrows(
                IllegalArgumentException.class,
                () -> ContainerOperationDeadline.start(
                        "overflow", Duration.ofSeconds(Long.MAX_VALUE), System::nanoTime));
    }

    private static final class MutableNanoClock {
        private long now;

        private MutableNanoClock(long now) {
            this.now = now;
        }

        private long nanoTime() {
            return now;
        }

        private void advance(long nanos) {
            now += nanos;
        }
    }
}
