package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MonotonicDeadlineTest {

    @Test
    void measuresElapsedTimeAcrossNegativeAndWrappedClockOrigins() {
        MutableNanoClock negative = new MutableNanoClock(-500L);
        MonotonicDeadline negativeDeadline =
                MonotonicDeadline.start(Duration.ofNanos(100), negative::nanoTime);
        negative.advance(50L);

        assertEquals(Duration.ofNanos(50), negativeDeadline.remaining());

        MutableNanoClock wrapped = new MutableNanoClock(Long.MAX_VALUE - 10L);
        MonotonicDeadline wrappedDeadline =
                MonotonicDeadline.start(Duration.ofNanos(100), wrapped::nanoTime);
        wrapped.advance(21L);

        assertEquals(Duration.ofNanos(79), wrappedDeadline.remaining());
    }

    @Test
    void expiresAtTheExactBoundaryAndFailsClosedWhenTheClockMovesBackwards() {
        MutableNanoClock exact = new MutableNanoClock(100L);
        MonotonicDeadline exactDeadline =
                MonotonicDeadline.start(Duration.ofNanos(10), exact::nanoTime);
        exact.advance(9L);
        assertEquals(Duration.ofNanos(1), exactDeadline.remaining());
        exact.advance(1L);
        assertEquals(Duration.ZERO, exactDeadline.remaining());

        MutableNanoClock backwards = new MutableNanoClock(100L);
        MonotonicDeadline backwardsDeadline =
                MonotonicDeadline.start(Duration.ofNanos(10), backwards::nanoTime);
        backwards.advance(-1L);
        assertEquals(Duration.ZERO, backwardsDeadline.remaining());
    }

    @Test
    void throwsTheCallersExceptionOnlyOnceExpired() {
        MutableNanoClock clock = new MutableNanoClock(0L);
        MonotonicDeadline deadline = MonotonicDeadline.start(Duration.ofNanos(10), clock::nanoTime);
        IllegalStateException expired = new IllegalStateException("expired");

        assertEquals(Duration.ofNanos(10), deadline.remainingOrThrow(() -> expired));
        clock.advance(10L);
        assertSame(expired, assertThrows(
                IllegalStateException.class, () -> deadline.remainingOrThrow(() -> expired)));
    }

    @Test
    void rejectsNonPositiveTimeoutsAndClampsOnesTooLongForNanoseconds() {
        assertThrows(IllegalArgumentException.class,
                () -> MonotonicDeadline.start(Duration.ZERO, System::nanoTime));
        assertThrows(IllegalArgumentException.class,
                () -> MonotonicDeadline.start(Duration.ofNanos(-1), System::nanoTime));

        MutableNanoClock clock = new MutableNanoClock(0L);
        Duration tooLong = Duration.ofSeconds(Long.MAX_VALUE);
        MonotonicDeadline clamped = MonotonicDeadline.start(tooLong, clock::nanoTime);

        assertEquals(tooLong, clamped.timeout());
        assertEquals(Duration.ofNanos(Long.MAX_VALUE), clamped.remaining());
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
