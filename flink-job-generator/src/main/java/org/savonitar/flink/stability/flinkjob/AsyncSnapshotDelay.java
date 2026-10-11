package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/** Injects checkpoint failures/delays on the async thread and a separate task-thread failure. */
final class AsyncSnapshotDelay extends StreamMap<String, String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AsyncSnapshotDelay.class);
    private final int delayMs;
    private final Set<Long> failedCheckpoints;
    private final PlannedFailure plannedFailure;

    AsyncSnapshotDelay(MapFunction<String, String> function, int delayMs) {
        this(function, delayMs, Set.of(), null, 2000);
    }

    AsyncSnapshotDelay(MapFunction<String, String> function, int delayMs,
            Set<Long> failedCheckpoints, Long failAfterCheckpoint, int failAfterDelayMs) {
        super(function);
        if (delayMs < 0) throw new IllegalArgumentException("Negative async delay");
        this.delayMs = delayMs;
        this.failedCheckpoints = Set.copyOf(failedCheckpoints);
        this.plannedFailure = failAfterCheckpoint == null ? null : new PlannedFailure(failAfterCheckpoint, failAfterDelayMs);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp,
            CheckpointOptions options, CheckpointStreamFactory storage) throws Exception {
        OperatorSnapshotFutures futures = super.snapshotState(checkpointId, timestamp, options, storage);
        futures.setOperatorStateRawFuture(wrapSnapshot(futures.getOperatorStateRawFuture(), checkpointId));
        long now = System.currentTimeMillis();
        if (delayMs > 0 || !failedCheckpoints.isEmpty() || plannedFailure != null) {
            LOG.info("POOL_REUSE_SNAPSHOT_SYNC checkpoint={} delayMs={} timeMs={}",
                    checkpointId, delayMs, now);
        }
        // The underlying synchronous snapshot has returned. Only the exact barrier arms
        // this task-thread trigger; checkpoint ids remain monotonic across failover.
        if (plannedFailure != null) plannedFailure.afterSnapshot(checkpointId, now);
        return futures;
    }

    @Override
    public void processElement(StreamRecord<String> element) throws Exception {
        if (plannedFailure != null) plannedFailure.beforeElement(System.currentTimeMillis());
        super.processElement(element);
    }

    <T extends StateObject> RunnableFuture<SnapshotResult<T>> wrapSnapshot(
            RunnableFuture<SnapshotResult<T>> original, long checkpointId) {
        boolean fail = failedCheckpoints.contains(checkpointId);
        return delayMs > 0 || fail ? snapshotFuture(original, delayMs, checkpointId, fail) : original;
    }

    static <T extends StateObject> RunnableFuture<SnapshotResult<T>> delayed(
            RunnableFuture<SnapshotResult<T>> original, int delayMs, long checkpointId) {
        return snapshotFuture(original, delayMs, checkpointId, false);
    }

    private static <T extends StateObject> RunnableFuture<SnapshotResult<T>> snapshotFuture(
            RunnableFuture<SnapshotResult<T>> original, int delayMs, long checkpointId, boolean fail) {
        return new FutureTask<>(() -> {
            if (fail) {
                String message = "POOL_REUSE_SNAPSHOT_ASYNC_FAIL checkpoint=" + checkpointId;
                LOG.info(message);
                original.cancel(true);
                throw new RuntimeException(message);
            }
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

    /** Deliberately not checkpointed: a restarted task only sees later checkpoint ids. */
    static final class PlannedFailure implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long checkpointId;
        private final int delayMs;
        private transient boolean armed;
        private transient boolean fired;
        private transient long armedAtMs;

        PlannedFailure(long checkpointId, int delayMs) {
            if (checkpointId <= 0) throw new IllegalArgumentException("Checkpoint id must be positive");
            if (delayMs < 0) throw new IllegalArgumentException("Negative planned-failure delay");
            this.checkpointId = checkpointId;
            this.delayMs = delayMs;
        }

        void afterSnapshot(long snapshotId, long nowMs) {
            if (snapshotId == checkpointId && !armed && !fired) {
                armed = true;
                armedAtMs = nowMs;
            }
        }

        void beforeElement(long nowMs) {
            if (armed && !fired && nowMs - armedAtMs >= delayMs) {
                fired = true;
                String message = "POOL_REUSE_PLANNED_FAILURE after checkpoint=" + checkpointId + " timeMs=" + nowMs;
                LOG.info(message);
                throw new RuntimeException(message);
            }
        }
    }
}
