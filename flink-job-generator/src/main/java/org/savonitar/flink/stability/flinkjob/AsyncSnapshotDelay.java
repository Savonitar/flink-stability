package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/** Delays source-chain acknowledgement on the async checkpoint thread, never the mailbox. */
final class AsyncSnapshotDelay extends StreamMap<String, String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AsyncSnapshotDelay.class);
    private final int delayMs;

    AsyncSnapshotDelay(MapFunction<String, String> function, int delayMs) {
        super(function);
        if (delayMs < 0) throw new IllegalArgumentException("Negative async delay");
        this.delayMs = delayMs;
    }

    @Override
    public OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp,
            CheckpointOptions options, CheckpointStreamFactory storage) throws Exception {
        OperatorSnapshotFutures futures = super.snapshotState(checkpointId, timestamp, options, storage);
        if (delayMs > 0) {
            futures.setOperatorStateRawFuture(delayed(futures.getOperatorStateRawFuture(), delayMs, checkpointId));
            LOG.info("POOL_REUSE_SNAPSHOT_SYNC checkpoint={} delayMs={} timeMs={}",
                    checkpointId, delayMs, System.currentTimeMillis());
        }
        return futures;
    }

    static <T extends StateObject> RunnableFuture<SnapshotResult<T>> delayed(
            RunnableFuture<SnapshotResult<T>> original, int delayMs, long checkpointId) {
        return new FutureTask<>(() -> {
            long start = System.currentTimeMillis();
            LOG.info("POOL_REUSE_SNAPSHOT_ASYNC_START checkpoint={} delayMs={} timeMs={}",
                    checkpointId, delayMs, start);
            Thread.sleep(delayMs);
            original.run();
            SnapshotResult<T> result = original.get();
            LOG.info("POOL_REUSE_SNAPSHOT_ASYNC_DONE checkpoint={} timeMs={}",
                    checkpointId, System.currentTimeMillis());
            return result;
        }) {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean cancelled = super.cancel(mayInterruptIfRunning);
                original.cancel(mayInterruptIfRunning);
                return cancelled;
            }
        };
    }
}
