package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AttemptCleanupBoundaryTest {

    @Test
    void delayedWorkerStartHonorsDeadlineWithoutCancellingTheQueuedCleanup()
            throws Exception {
        ExecutorService cleanupCalls = Executors.newSingleThreadExecutor();
        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        CountDownLatch cleanupInvoked = new CountDownLatch(1);
        AtomicInteger cleanupInvocations = new AtomicInteger();
        cleanupCalls.submit(() -> {
            blockerEntered.countDown();
            awaitUninterruptibly(releaseBlocker);
        });
        assertTrue(blockerEntered.await(1, TimeUnit.SECONDS));
        AttemptCleanupBoundary boundary = new AttemptCleanupBoundary(
                Duration.ofMillis(75), cleanupCalls);

        try {
            Thread.currentThread().interrupt();
            long startedAt = System.nanoTime();
            RuntimeException failure = boundary.await(() -> {
                cleanupInvocations.incrementAndGet();
                cleanupInvoked.countDown();
                return null;
            });
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startedAt);

            assertInstanceOf(AttemptCleanupTimeoutException.class, failure);
            assertTrue(elapsedMillis < 2_000L);
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0, cleanupInvocations.get());

            Thread.interrupted();
            releaseBlocker.countDown();
            assertTrue(cleanupInvoked.await(1, TimeUnit.SECONDS));
            assertEquals(1, cleanupInvocations.get());
        } finally {
            Thread.interrupted();
            releaseBlocker.countDown();
            cleanupCalls.shutdownNow();
            assertTrue(cleanupCalls.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptionWhileQueuedUsesRemainingDeadlineAndStillRunsCleanupExactlyOnce()
            throws Exception {
        CountDownLatch cleanupSubmitted = new CountDownLatch(1);
        AtomicInteger submissions = new AtomicInteger();
        ThreadPoolExecutor cleanupCalls = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                super.execute(command);
                if (submissions.incrementAndGet() == 2) {
                    cleanupSubmitted.countDown();
                }
            }
        };
        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        CountDownLatch cleanupInvoked = new CountDownLatch(1);
        AtomicInteger cleanupInvocations = new AtomicInteger();
        cleanupCalls.submit(() -> {
            blockerEntered.countDown();
            awaitUninterruptibly(releaseBlocker);
        });
        assertTrue(blockerEntered.await(1, TimeUnit.SECONDS));
        AttemptCleanupBoundary boundary = new AttemptCleanupBoundary(
                Duration.ofMillis(100), cleanupCalls);
        AtomicReference<RuntimeException> result = new AtomicReference<>();
        AtomicBoolean interruptedOnReturn = new AtomicBoolean();
        Thread caller = Thread.ofPlatform().daemon(true).start(() -> {
            result.set(boundary.await(() -> {
                cleanupInvocations.incrementAndGet();
                cleanupInvoked.countDown();
                return null;
            }));
            interruptedOnReturn.set(Thread.currentThread().isInterrupted());
        });

        try {
            assertTrue(cleanupSubmitted.await(1, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(Duration.ofSeconds(2));

            assertFalse(caller.isAlive());
            assertInstanceOf(AttemptCleanupTimeoutException.class, result.get());
            assertTrue(interruptedOnReturn.get());
            assertEquals(0, cleanupInvocations.get());

            releaseBlocker.countDown();
            assertTrue(cleanupInvoked.await(1, TimeUnit.SECONDS));
            assertEquals(1, cleanupInvocations.get());
        } finally {
            releaseBlocker.countDown();
            caller.join(Duration.ofSeconds(1));
            cleanupCalls.shutdownNow();
            assertTrue(cleanupCalls.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
