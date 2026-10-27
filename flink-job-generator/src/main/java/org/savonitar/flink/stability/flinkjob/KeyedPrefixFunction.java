package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;

/**
 * An opt-in accumulator whose output depends on the actual restored keyed state.
 * The terminal oracle lives in core and must not reuse this implementation.
 */
final class KeyedPrefixFunction extends RichMapFunction<String, String>
        implements CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KeyedPrefixFunction.class);
    static final String STATE_NAME = "flink-stability-keyed-prefix-count-sum-v1";

    private final int keyCount;
    private final StateRecoveryBuild.Mode buildMode;
    private final ReceiptSink receipts;
    private transient ValueState<Tuple2<Long, Long>> aggregate;
    // These bounded, incarnation-local counters are diagnostic only. They are not recovery state.
    private transient long[] observedCounts;
    private transient long processed;
    private transient long maximumInputId;
    private transient long restoredCheckpointId;
    private transient int observedKeyCount;

    KeyedPrefixFunction(int keyCount) {
        this(keyCount, StateRecoveryBuild.mode(), LogReceiptSink.INSTANCE);
    }

    KeyedPrefixFunction(int keyCount, StateRecoveryBuild.Mode buildMode, ReceiptSink receipts) {
        this.keyCount = requireKeyCount(keyCount);
        this.buildMode = Objects.requireNonNull(buildMode, "buildMode");
        this.receipts = Objects.requireNonNull(receipts, "receipts");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        restoredCheckpointId = context.isRestored()
                ? context.getRestoredCheckpointId().orElseThrow(() ->
                        new IllegalStateException("Restoration lacks a checkpoint identity")) : -1;
        if (context.isRestored() && restoredCheckpointId < 0)
            throw new IllegalStateException("Restoration has an invalid checkpoint identity");
    }

    @Override
    public void open(OpenContext context) throws Exception {
        if (getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks() != 1) {
            throw new IllegalStateException("keyed-prefix-v1 requires one operator subtask");
        }
        aggregate = getRuntimeContext().getState(
                new ValueStateDescriptor<>(STATE_NAME, stateSerializer()));
        observedCounts = new long[keyCount];
        observedKeyCount = 0;
        processed = 0;
        maximumInputId = -1;
        receipts.emit(identityMarker("START", restoredCheckpointId) + " buildMode=" + buildMode.wireValue);
    }

    @Override
    public String map(String value) throws Exception {
        long id = parseId(value);
        int key = (int) (id % keyCount);
        Tuple2<Long, Long> previous = aggregate.value();
        if (previous != null && (previous.f0 == null || previous.f1 == null
                || previous.f0 < 1 || previous.f1 < 1)) {
            throw new IllegalStateException("Invalid restored keyed-prefix aggregate");
        }
        boolean firstForKey = observedCounts[key] == 0;
        boolean restoredFirstKey = restoredCheckpointId >= 0 && firstForKey;
        boolean reset = restoredFirstKey && previous != null
                && buildMode == StateRecoveryBuild.Mode.RESET_STATE_ON_RESTORE;
        long previousCount = previous == null ? 0 : previous.f0;
        long previousSum = previous == null ? 0 : previous.f1;
        long count = Math.addExact(reset ? 0 : previousCount, 1);
        long sum = Math.addExact(reset ? 0 : previousSum, Math.addExact(id, 1));
        // Replace, never mutate the object returned by a state backend before update succeeds.
        aggregate.update(Tuple2.of(count, sum));
        observedCounts[key] = count;
        processed = Math.addExact(processed, 1);
        maximumInputId = Math.max(maximumInputId, id);
        if (restoredFirstKey) {
            receipts.emit(keyMarker("RESTORED_KEY", key, id, previousCount, previousSum, count, sum));
            if (reset) receipts.emit(keyMarker("MUTANT_RESET", key, id, previousCount, previousSum, count, sum));
        }
        if (firstForKey && ++observedKeyCount == keyCount)
            receipts.emit(progressMarker("PROGRESS", restoredCheckpointId));
        return "{\"version\":1,\"id\":" + id + ",\"key\":" + key
                + ",\"count\":" + count + ",\"prefixSum\":" + sum + "}";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        // This is the synchronous snapshot boundary, not confirmation that the checkpoint completed.
        receipts.emit(snapshotMarker(context.getCheckpointId()));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        receipts.emit(identityMarker("COMPLETED", checkpointId));
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        receipts.emit(identityMarker("ABORTED", checkpointId));
    }

    String snapshotMarker(long checkpointId) {
        return progressMarker("SNAPSHOT", checkpointId);
    }

    private String progressMarker(String event, long checkpointId) {
        int seen = 0;
        long minimumCount = Long.MAX_VALUE;
        for (long count : observedCounts) {
            if (count > 0) {
                seen++;
                minimumCount = Math.min(minimumCount, count);
            }
        }
        return identityMarker(event, checkpointId)
                + " keyCount=" + keyCount + " observedKeys=" + seen
                + " minimumObservedCount=" + (seen == 0 ? 0 : minimumCount)
                + " processedThisAttempt=" + processed + " maximumInputId=" + maximumInputId;
    }

    private String keyMarker(String event, int key, long id, long previousCount, long previousSum,
            long count, long sum) {
        return identityMarker(event, restoredCheckpointId) + " keyCount=" + keyCount
                + " key=" + key + " inputId=" + id
                + " previousCount=" + previousCount + " previousSum=" + previousSum
                + " newCount=" + count + " newSum=" + sum;
    }

    private String identityMarker(String event, long checkpointId) {
        return "STATE_PREFIX_" + event + " version=1 jobId="
                + getRuntimeContext().getJobInfo().getJobId()
                + " operatorUid=" + FlinkKafkaEosJob.KEYED_PREFIX_OPERATOR_UID
                + " attempt=" + getRuntimeContext().getTaskInfo().getAttemptNumber()
                + " subtask=" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                + " checkpoint=" + checkpointId + " restoredCheckpoint=" + restoredCheckpointId;
    }

    @FunctionalInterface
    interface ReceiptSink extends Serializable {
        void emit(String marker);
    }

    private enum LogReceiptSink implements ReceiptSink {
        INSTANCE;
        @Override public void emit(String marker) { LOG.info("{}", marker); }
    }

    @SuppressWarnings("unchecked")
    static TypeSerializer<Tuple2<Long, Long>> stateSerializer() {
        Class<Tuple2<Long, Long>> tupleType = (Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleType,
                new TypeSerializer<?>[]{LongSerializer.INSTANCE, LongSerializer.INSTANCE});
    }

    private static int requireKeyCount(int count) {
        if (count < 1 || count > 1024) throw new IllegalArgumentException("Key count must be in 1..1024");
        return count;
    }

    static long parseId(String value) {
        if (value == null || value.length() > 19 || !value.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException("keyed-prefix-v1 input must be a canonical nonnegative long ID");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("keyed-prefix-v1 input ID exceeds the signed long range", failure);
        }
    }

    static final class PrefixKeySelector implements KeySelector<String, Integer> {
        private static final long serialVersionUID = 1L;
        private final int keyCount;

        PrefixKeySelector(int keyCount) {
            this.keyCount = requireKeyCount(keyCount);
        }

        @Override
        public Integer getKey(String value) {
            return (int) (parseId(value) % keyCount);
        }
    }
}
