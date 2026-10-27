package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

class KeyedPrefixFunctionTest {
    @Test
    void mutantInitialStartMatchesCorrectEvenWithANonzeroAttemptNumber() throws Exception {
        Backend correctBackend = new Backend();
        Backend mutantBackend = new Backend();
        List<String> receipts = new ArrayList<>();
        KeyedPrefixFunction correct = open(correctBackend, OptionalLong.empty(), 1);
        KeyedPrefixFunction mutant = open(mutantBackend, OptionalLong.empty(), 1,
                StateRecoveryBuild.Mode.RESET_STATE_ON_RESTORE, receipts);
        for (int id = 0; id < 8; id++)
            assertEquals(map(correct, correctBackend, id), map(mutant, mutantBackend, id));
        assertEquals(correctBackend.values, mutantBackend.values);
        assertEquals(2, receipts.size()); // START plus first all-key PROGRESS; no restore/reset receipt.
        assertTrue(receipts.get(0).endsWith("buildMode=reset-state-on-restore-v1"));
        assertTrue(receipts.get(1).startsWith("STATE_PREFIX_PROGRESS "));
        assertTrue(receipts.get(1).contains("checkpoint=-1 restoredCheckpoint=-1"));
    }

    @Test
    void mutantResetsRealRestoredStateExactlyOncePerKeyAndEmitsBoundedReceipts() throws Exception {
        Backend initial = new Backend();
        KeyedPrefixFunction original = open(initial, OptionalLong.empty(), 0);
        for (int id = 0; id < 4; id++) map(original, initial, id);
        Backend restored = Backend.restore(initial.snapshot());
        List<String> receipts = new ArrayList<>();
        KeyedPrefixFunction mutant = open(restored, OptionalLong.of(7), 1,
                StateRecoveryBuild.Mode.RESET_STATE_ON_RESTORE, receipts);
        assertEquals("{\"version\":1,\"id\":4,\"key\":0,\"count\":1,\"prefixSum\":5}",
                map(mutant, restored, 4));
        assertEquals("{\"version\":1,\"id\":6,\"key\":0,\"count\":2,\"prefixSum\":12}",
                map(mutant, restored, 6));
        assertFalse(receipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_PROGRESS ")));
        assertEquals("{\"version\":1,\"id\":5,\"key\":1,\"count\":1,\"prefixSum\":6}",
                map(mutant, restored, 5));
        assertEquals("{\"version\":1,\"id\":7,\"key\":1,\"count\":2,\"prefixSum\":14}",
                map(mutant, restored, 7));
        List<String> resets = receipts.stream().filter(value -> value.startsWith("STATE_PREFIX_MUTANT_RESET "))
                .collect(java.util.stream.Collectors.toList());
        assertEquals(2, resets.size());
        assertTrue(resets.get(0).endsWith("keyCount=2 key=0 inputId=4 previousCount=2 previousSum=4 newCount=1 newSum=5"));
        assertTrue(resets.get(1).endsWith("keyCount=2 key=1 inputId=5 previousCount=2 previousSum=6 newCount=1 newSum=6"));
        assertEquals(2, receipts.stream().filter(value -> value.startsWith("STATE_PREFIX_RESTORED_KEY ")).count());
        assertEquals(1, receipts.stream().filter(value -> value.startsWith("STATE_PREFIX_PROGRESS ")).count());
        assertTrue(receipts.stream().allMatch(value -> value.length() < 512
                && value.contains("jobId=" + restored.jobId)
                && value.contains("operatorUid=" + FlinkKafkaEosJob.KEYED_PREFIX_OPERATOR_UID)
                && value.contains("attempt=1 subtask=0 checkpoint=7 restoredCheckpoint=7")));

        // A subsequent genuine restoration gets its own once-per-key mutation, not a persisted flag.
        Backend second = Backend.restore(restored.snapshot());
        List<String> secondReceipts = new ArrayList<>();
        KeyedPrefixFunction again = open(second, OptionalLong.of(8), 2,
                StateRecoveryBuild.Mode.RESET_STATE_ON_RESTORE, secondReceipts);
        assertEquals("{\"version\":1,\"id\":8,\"key\":0,\"count\":1,\"prefixSum\":9}", map(again, second, 8));
        assertTrue(secondReceipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_MUTANT_RESET ")
                && value.contains("previousCount=2 previousSum=12")));
    }

    @Test
    void correctArmReportsRealRestoredStateAndMissingStateCannotClaimAnActivation() throws Exception {
        Backend restored = new Backend();
        restored.values.put(0, Tuple2.of(7L, 100L));
        List<String> correctReceipts = new ArrayList<>();
        KeyedPrefixFunction correct = open(restored, OptionalLong.of(7), 1,
                StateRecoveryBuild.Mode.CORRECT, correctReceipts);
        map(correct, restored, 4);
        assertTrue(correctReceipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_RESTORED_KEY ")
                && value.endsWith("keyCount=2 key=0 inputId=4 previousCount=7 previousSum=100 newCount=8 newSum=105")));
        assertFalse(correctReceipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_MUTANT_RESET ")));

        Backend missing = new Backend();
        List<String> missingReceipts = new ArrayList<>();
        KeyedPrefixFunction mutant = open(missing, OptionalLong.of(7), 1,
                StateRecoveryBuild.Mode.RESET_STATE_ON_RESTORE, missingReceipts);
        map(mutant, missing, 4);
        assertTrue(missingReceipts.stream().anyMatch(value -> value.contains("previousCount=0 previousSum=0")));
        assertFalse(missingReceipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_MUTANT_RESET ")));
    }

    @Test
    void failedStateUpdateCannotEmitActivationOrConsumeFirstKeyMutation() throws Exception {
        Backend backend = new Backend();
        backend.values.put(0, Tuple2.of(2L, 4L));
        backend.failNextUpdate = true;
        List<String> receipts = new ArrayList<>();
        KeyedPrefixFunction mutant = open(backend, OptionalLong.of(7), 1,
                StateRecoveryBuild.Mode.RESET_STATE_ON_RESTORE, receipts);
        assertThrows(IllegalStateException.class, () -> map(mutant, backend, 4));
        assertEquals(1, receipts.size());
        assertEquals(Tuple2.of(2L, 4L), backend.values.get(0));
        assertEquals("{\"version\":1,\"id\":4,\"key\":0,\"count\":1,\"prefixSum\":5}", map(mutant, backend, 4));
        assertEquals(1, receipts.stream().filter(value -> value.startsWith("STATE_PREFIX_MUTANT_RESET ")).count());
    }

    @Test
    void emitsHandwrittenPrefixesForInterleavedKeys() throws Exception {
        Backend backend = new Backend();
        KeyedPrefixFunction function = open(backend, OptionalLong.empty(), 0);
        String[] expected = {
                "{\"version\":1,\"id\":0,\"key\":0,\"count\":1,\"prefixSum\":1}",
                "{\"version\":1,\"id\":1,\"key\":1,\"count\":1,\"prefixSum\":2}",
                "{\"version\":1,\"id\":2,\"key\":0,\"count\":2,\"prefixSum\":4}",
                "{\"version\":1,\"id\":3,\"key\":1,\"count\":2,\"prefixSum\":6}",
                "{\"version\":1,\"id\":4,\"key\":0,\"count\":3,\"prefixSum\":9}",
                "{\"version\":1,\"id\":5,\"key\":1,\"count\":3,\"prefixSum\":12}"
        };
        for (int id = 0; id < expected.length; id++) {
            assertEquals(expected[id], map(function, backend, id));
        }
    }

    @Test
    void restoresSerializedKeyedStateInsteadOfRecomputingFromInputId() throws Exception {
        Backend backend = new Backend();
        KeyedPrefixFunction original = open(backend, OptionalLong.empty(), 0);
        for (int id = 0; id < 4; id++) map(original, backend, id);
        Map<Integer, byte[]> checkpoint = backend.snapshot();
        map(original, backend, 4);
        map(original, backend, 5);

        Backend restored = Backend.restore(checkpoint);
        KeyedPrefixFunction replacement = open(restored, OptionalLong.of(7), 1);
        assertEquals("{\"version\":1,\"id\":4,\"key\":0,\"count\":3,\"prefixSum\":9}",
                map(replacement, restored, 4));
        assertEquals("{\"version\":1,\"id\":5,\"key\":1,\"count\":3,\"prefixSum\":12}",
                map(replacement, restored, 5));
        assertEquals("{\"version\":1,\"id\":6,\"key\":0,\"count\":4,\"prefixSum\":16}",
                map(replacement, restored, 6));
        assertTrue(replacement.snapshotMarker(8).contains("attempt=1 subtask=0 checkpoint=8 restoredCheckpoint=7"));

        // Deliberately different state must change the result for the same input ID.
        Backend altered = new Backend();
        altered.values.put(0, Tuple2.of(7L, 100L));
        KeyedPrefixFunction other = open(altered, OptionalLong.of(7), 1);
        assertEquals("{\"version\":1,\"id\":4,\"key\":0,\"count\":8,\"prefixSum\":105}",
                map(other, altered, 4));
        Backend lost = new Backend();
        assertEquals("{\"version\":1,\"id\":4,\"key\":0,\"count\":1,\"prefixSum\":5}",
                map(open(lost, OptionalLong.of(7), 1), lost, 4));
    }

    @Test
    void snapshotMarkersAreBoundedAndDoNotInventUnobservedRestoredKeys() throws Exception {
        Backend backend = new Backend();
        backend.values.put(0, Tuple2.of(2L, 4L));
        backend.values.put(1, Tuple2.of(2L, 6L));
        KeyedPrefixFunction function = open(backend, OptionalLong.of(3), 1);
        assertTrue(function.snapshotMarker(4).contains("keyCount=2 observedKeys=0 minimumObservedCount=0"));
        map(function, backend, 4);
        String marker = function.snapshotMarker(5);
        assertTrue(marker.contains("keyCount=2 observedKeys=1 minimumObservedCount=3"));
        assertTrue(marker.contains("processedThisAttempt=1 maximumInputId=4"));
        assertTrue(marker.contains("jobId=" + backend.jobId));
        assertTrue(marker.contains("operatorUid=" + FlinkKafkaEosJob.KEYED_PREFIX_OPERATOR_UID));
        assertTrue(marker.length() < 512);
    }

    @Test
    void rejectsMalformedIdsAndCheckedOverflowWithoutUpdatingState() throws Exception {
        Backend backend = new Backend();
        KeyedPrefixFunction function = open(backend, OptionalLong.empty(), 0);
        for (String invalid : new String[]{"", "-1", "+1", "01", " 1", "1.0", "١", "9223372036854775808"}) {
            assertThrows(IllegalArgumentException.class, () -> function.map(invalid), invalid);
            assertThrows(IllegalArgumentException.class,
                    () -> new KeyedPrefixFunction.PrefixKeySelector(2).getKey(invalid), invalid);
        }
        assertThrows(IllegalArgumentException.class, () -> function.map(null));
        assertTrue(backend.values.isEmpty());
        backend.currentKey = 0;
        backend.values.put(0, Tuple2.of(Long.MAX_VALUE, 1L));
        assertThrows(ArithmeticException.class, () -> function.map("0"));
        assertEquals(Tuple2.of(Long.MAX_VALUE, 1L), backend.values.get(0));
        backend.values.put(0, Tuple2.of(1L, Long.MAX_VALUE));
        assertThrows(ArithmeticException.class, () -> function.map("0"));
        assertEquals(Tuple2.of(1L, Long.MAX_VALUE), backend.values.get(0));
    }

    private static String map(KeyedPrefixFunction function, Backend backend, long id) throws Exception {
        String value = Long.toString(id);
        backend.currentKey = new KeyedPrefixFunction.PrefixKeySelector(2).getKey(value);
        return function.map(value);
    }

    private static KeyedPrefixFunction open(Backend backend, OptionalLong restored, int attempt) throws Exception {
        return open(backend, restored, attempt, StateRecoveryBuild.Mode.CORRECT, new ArrayList<>());
    }

    private static KeyedPrefixFunction open(Backend backend, OptionalLong restored, int attempt,
            StateRecoveryBuild.Mode mode, List<String> receipts) throws Exception {
        KeyedPrefixFunction function = new KeyedPrefixFunction(2, mode, receipts::add);
        function.setRuntimeContext(backend.runtime(attempt));
        function.initializeState(new FunctionInitializationContext() {
            @Override public OptionalLong getRestoredCheckpointId() { return restored; }
            @Override public OperatorStateStore getOperatorStateStore() { throw new UnsupportedOperationException(); }
            @Override public KeyedStateStore getKeyedStateStore() { throw new UnsupportedOperationException(); }
        });
        function.open(null);
        return function;
    }

    /** In-memory keyed ValueState seam; snapshots use the operator's actual explicit serializer. */
    private static final class Backend implements ValueState<Tuple2<Long, Long>> {
        final JobID jobId = new JobID();
        final Map<Integer, Tuple2<Long, Long>> values = new HashMap<>();
        int currentKey;
        boolean failNextUpdate;
        TypeSerializer<Tuple2<Long, Long>> serializer;

        @Override public Tuple2<Long, Long> value() { return values.get(currentKey); }
        @Override public void update(Tuple2<Long, Long> value) {
            if (failNextUpdate) {
                failNextUpdate = false;
                throw new IllegalStateException("Deliberate state-update failure");
            }
            values.put(currentKey, value);
        }
        @Override public void clear() { values.remove(currentKey); }

        @SuppressWarnings("unchecked")
        RuntimeContext runtime(int attempt) {
            JobInfo job = new JobInfo() {
                @Override public JobID getJobId() { return jobId; }
                @Override public String getJobName() { return "state-test"; }
            };
            TaskInfo task = (TaskInfo) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{TaskInfo.class}, (proxy, method, arguments) -> {
                        switch (method.getName()) {
                            case "getAttemptNumber": return attempt;
                            case "getNumberOfParallelSubtasks": return 1;
                            case "getIndexOfThisSubtask": return 0;
                            default: throw new UnsupportedOperationException(method.getName());
                        }
                    });
            return (RuntimeContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{RuntimeContext.class}, (proxy, method, arguments) -> {
                        switch (method.getName()) {
                            case "getState":
                                ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                                        (ValueStateDescriptor<Tuple2<Long, Long>>) arguments[0];
                                assertEquals(KeyedPrefixFunction.STATE_NAME, descriptor.getName());
                                assertFalse(descriptor.getTtlConfig().isEnabled());
                                serializer = descriptor.getSerializer();
                                assertEquals(KeyedPrefixFunction.stateSerializer(), serializer);
                                return this;
                            case "getJobInfo": return job;
                            case "getTaskInfo": return task;
                            default: throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }

        Map<Integer, byte[]> snapshot() throws Exception {
            Map<Integer, byte[]> result = new HashMap<>();
            for (Map.Entry<Integer, Tuple2<Long, Long>> entry : values.entrySet()) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                serializer.serialize(entry.getValue(), new DataOutputViewStreamWrapper(bytes));
                result.put(entry.getKey(), bytes.toByteArray());
            }
            return result;
        }

        static Backend restore(Map<Integer, byte[]> snapshot) throws Exception {
            Backend result = new Backend();
            TypeSerializer<Tuple2<Long, Long>> serializer = KeyedPrefixFunction.stateSerializer();
            for (Map.Entry<Integer, byte[]> entry : snapshot.entrySet()) {
                result.values.put(entry.getKey(), serializer.deserialize(
                        new DataInputViewStreamWrapper(new ByteArrayInputStream(entry.getValue()))));
            }
            return result;
        }
    }
}
