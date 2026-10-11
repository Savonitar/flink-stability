package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AsyncSnapshotDelayTest {
    @Test void listedCheckpointFailsAfterFailureDelayIndependentlyOfOrdinaryDelay() throws Exception {
        AtomicReference<Thread> cancellationThread = new AtomicReference<>();
        AtomicBoolean executed = new AtomicBoolean();
        var original = new FutureTask<>(() -> {
            executed.set(true);
            return SnapshotResult.<OperatorStateHandle>empty();
        }) {
            @Override public boolean cancel(boolean interrupt) {
                cancellationThread.set(Thread.currentThread());
                return super.cancel(interrupt);
            }
        };
        var operator = new AsyncSnapshotDelay(value -> value, 20000, Set.of(2L, 3L), 100, null, 2000);
        var failing = operator.wrapSnapshot(original, 2);
        assertFalse(failing.isDone());
        assertFalse(original.isCancelled());
        Thread worker = new Thread(failing, "checkpoint-async-test");
        long start = System.nanoTime();
        long startTimeMs = System.currentTimeMillis();
        worker.start();
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> failing.get(3, TimeUnit.SECONDS));
        worker.join();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 100);
        assertInstanceOf(RuntimeException.class, failure.getCause());
        String markerPrefix = "POOL_REUSE_SNAPSHOT_ASYNC_FAIL checkpoint=2 timeMs=";
        assertTrue(failure.getCause().getMessage().startsWith(markerPrefix));
        long failureTimeMs = Long.parseLong(failure.getCause().getMessage().substring(markerPrefix.length()));
        assertTrue(failureTimeMs >= startTimeMs + 100);
        assertTrue(failureTimeMs <= System.currentTimeMillis());
        assertSame(worker, cancellationThread.get());
        assertTrue(original.isCancelled());
        assertFalse(executed.get());
    }

    @Test void unlistedCheckpointStillDelaysAndReturnsOriginalResult() throws Exception {
        AtomicReference<Thread> executionThread = new AtomicReference<>();
        SnapshotResult<OperatorStateHandle> snapshot = SnapshotResult.empty();
        var original = new FutureTask<>(() -> { executionThread.set(Thread.currentThread()); return snapshot; });
        var operator = new AsyncSnapshotDelay(value -> value, 100, Set.of(2L, 3L), 20000, null, 2000);
        var delayed = operator.wrapSnapshot(original, 4);
        assertNull(executionThread.get());
        Thread worker = new Thread(delayed);
        long start = System.nanoTime();
        worker.start();
        assertSame(snapshot, delayed.get(3, TimeUnit.SECONDS));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 100);
        worker.join();
        assertSame(worker, executionThread.get());
        assertFalse(original.isCancelled());
    }

    @Test void checkpointFailuresAlsoWorkWithoutAsyncDelay() {
        var operator = new AsyncSnapshotDelay(value -> value, 0, Set.of(2L), 0, null, 2000);
        var original = new FutureTask<>(() -> SnapshotResult.<OperatorStateHandle>empty());
        assertSame(original, operator.wrapSnapshot(original, 1));
        var failing = operator.wrapSnapshot(original, 2);
        assertDoesNotThrow(failing::run); // Future.run captures the error; it never fails the task thread.
        ExecutionException failure = assertThrows(ExecutionException.class, failing::get);
        assertTrue(failure.getCause().getMessage().matches("POOL_REUSE_SNAPSHOT_ASYNC_FAIL checkpoint=2 timeMs=[0-9]+"));
        assertTrue(original.isCancelled());
    }

    @Test void cancellingAnUnstartedFailureFutureAlsoCancelsOriginal() {
        var operator = new AsyncSnapshotDelay(value -> value, 0, Set.of(2L), 500, null, 2000);
        var original = new FutureTask<>(() -> SnapshotResult.<OperatorStateHandle>empty());
        var failing = operator.wrapSnapshot(original, 2);
        assertTrue(failing.cancel(true));
        assertTrue(original.isCancelled());
        failing.run();
        assertTrue(failing.isCancelled());
    }

    @Test void plannedFailureFiresOnlyOnFirstElementAtOrAfterDelayAndCannotRearm() {
        var trigger = new AsyncSnapshotDelay.PlannedFailure(3, 2000);
        trigger.beforeElement(50000); // No snapshot means no trigger.
        trigger.afterSnapshot(3, 100000);
        trigger.beforeElement(101999);
        trigger.afterSnapshot(3, 101999); // Repeated calls cannot move the deadline.
        RuntimeException failure = assertThrows(RuntimeException.class, () -> trigger.beforeElement(102000));
        assertEquals("POOL_REUSE_PLANNED_FAILURE after checkpoint=3 timeMs=102000", failure.getMessage());
        assertDoesNotThrow(() -> trigger.beforeElement(102001));
        trigger.afterSnapshot(3, 103000);
        assertDoesNotThrow(() -> trigger.beforeElement(105000));
    }

    @Test void plannedFailureNeverArmsOnOtherCheckpointIdsIncludingAfterRestore() {
        var trigger = new AsyncSnapshotDelay.PlannedFailure(3, 0);
        trigger.afterSnapshot(2, 1000);
        trigger.afterSnapshot(4, 2000);
        assertDoesNotThrow(() -> trigger.beforeElement(100000));
    }

    @Test void zeroDelayStillWaitsForAnElementAfterTheExactSnapshot() {
        var trigger = new AsyncSnapshotDelay.PlannedFailure(3, 0);
        assertDoesNotThrow(() -> trigger.afterSnapshot(3, 1000));
        assertThrows(RuntimeException.class, () -> trigger.beforeElement(1000));
        assertDoesNotThrow(() -> trigger.beforeElement(1000));
    }

    @Test void delayRunsOnlyOnAsyncThreadAndPropagatesTheOriginalResult() throws Exception {
        AtomicBoolean executed = new AtomicBoolean();
        SnapshotResult<OperatorStateHandle> snapshot = SnapshotResult.empty();
        var original = new FutureTask<>(() -> { executed.set(true); return snapshot; });
        var delayed = AsyncSnapshotDelay.delayed(original, 100, 7);
        assertFalse(executed.get());
        assertFalse(delayed.isDone());
        Thread worker = new Thread(delayed);
        long start = System.nanoTime();
        worker.start();
        assertSame(snapshot, delayed.get(3, TimeUnit.SECONDS));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 100);
        worker.join();
    }

    @Test void cancellationReachesOriginalSnapshot() {
        var original = new FutureTask<>(() -> SnapshotResult.<OperatorStateHandle>empty());
        var delayed = AsyncSnapshotDelay.delayed(original, 20000, 8);
        assertTrue(delayed.cancel(true));
        assertTrue(original.isCancelled());
        delayed.run();
        assertTrue(delayed.isCancelled());
    }

    @Test void asyncDelayIsOptionalAndStrictlyParsed() {
        assertEquals(0, FlinkKafkaEosJobArguments.from(new String[0]).snapshotAsyncDelayMs());
        assertEquals(20000, FlinkKafkaEosJobArguments.from(new String[]{"--snapshotAsyncDelayMs", "20000"}).snapshotAsyncDelayMs());
        assertEquals(0, FlinkKafkaEosJobArguments.from(new String[]{"--snapshotAsyncDelayMs", "0"}).snapshotAsyncDelayMs());
        for (String bad : new String[]{"-1", "01", "nan", "2147483648"})
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(new String[]{"--snapshotAsyncDelayMs", bad}));
    }
}
