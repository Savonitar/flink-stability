package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.junit.jupiter.api.Test;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class AsyncSnapshotDelayTest {
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
