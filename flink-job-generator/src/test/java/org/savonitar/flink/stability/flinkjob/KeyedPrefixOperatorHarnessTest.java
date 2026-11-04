package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/** Uses Flink's state backend and snapshot handles; no substitute in-memory state store. */
class KeyedPrefixOperatorHarnessTest {
    @Test
    void correctAndMutantRestoreActualKeyedSnapshotsInFreshOperators() throws Exception {
        for (StateRecoveryBuild.Mode mode : StateRecoveryBuild.Mode.values()) {
            List<String> originalReceipts = new ArrayList<>();
            OperatorSubtaskState snapshot;
            try (var original = harness(mode, originalReceipts)) {
                original.setup();
                original.open();
                assertEquals(output(0, 0, 1, 1), process(original, 0));
                assertEquals(output(1, 1, 1, 2), process(original, 1));
                assertEquals(output(2, 0, 2, 4), process(original, 2));
                assertEquals(output(3, 1, 2, 6), process(original, 3));
                snapshot = original.snapshot(7, 100);
                original.notifyOfCompletedCheckpoint(7);
            }
            assertTrue(originalReceipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_SNAPSHOT ")
                    && value.contains("checkpoint=7 restoredCheckpoint=-1")));
            assertTrue(originalReceipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_COMPLETED ")));
            assertFalse(originalReceipts.stream().anyMatch(value -> value.startsWith("STATE_PREFIX_RESTORED_KEY ")
                    || value.startsWith("STATE_PREFIX_MUTANT_RESET ")));

            List<String> restoredReceipts = new ArrayList<>();
            boolean mutant = mode == StateRecoveryBuild.Mode.RESET_STATE_ON_RESTORE;
            try (var restored = harness(mode, restoredReceipts)) {
                restored.setup();
                restored.initializeState(snapshot);
                restored.open();
                assertEquals(output(4, 0, mutant ? 1 : 3, mutant ? 5 : 9), process(restored, 4));
                assertEquals(output(6, 0, mutant ? 2 : 4, mutant ? 12 : 16), process(restored, 6));
                assertEquals(output(5, 1, mutant ? 1 : 3, mutant ? 6 : 12), process(restored, 5));
                assertEquals(output(7, 1, mutant ? 2 : 4, mutant ? 14 : 20), process(restored, 7));
            }
            var start = restoredReceipts.get(0);
            assertTrue(start.startsWith("STATE_PREFIX_START "));
            assertTrue(start.endsWith("buildMode=" + mode.wireValue));
            var checkpoint = Pattern.compile(" restoredCheckpoint=([0-9]+)(?: |$)").matcher(start);
            assertTrue(checkpoint.find(), "Flink initialization must identify a genuine restored snapshot");
            String restoredId = checkpoint.group(1);
            List<String> keys = events(restoredReceipts, "RESTORED_KEY");
            assertEquals(2, keys.size());
            assertTrue(keys.get(0).contains("checkpoint=" + restoredId + " restoredCheckpoint=" + restoredId));
            assertTrue(keys.get(0).endsWith("keyCount=2 key=0 inputId=4 previousCount=2 previousSum=4 newCount="
                    + (mutant ? 1 : 3) + " newSum=" + (mutant ? 5 : 9)));
            assertTrue(keys.get(1).endsWith("keyCount=2 key=1 inputId=5 previousCount=2 previousSum=6 newCount="
                    + (mutant ? 1 : 3) + " newSum=" + (mutant ? 6 : 12)));
            assertEquals(mutant ? 2 : 0, events(restoredReceipts, "MUTANT_RESET").size());
            assertEquals(1, events(restoredReceipts, "PROGRESS").size());
            if (mutant) {
                List<String> resets = events(restoredReceipts, "MUTANT_RESET");
                for (int index = 0; index < keys.size(); index++)
                    assertEquals(keys.get(index).replace("STATE_PREFIX_RESTORED_KEY ", "STATE_PREFIX_MUTANT_RESET "), resets.get(index));
            }
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<Integer, String, String> harness(
            StateRecoveryBuild.Mode mode, List<String> receipts) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new StreamMap<>(new KeyedPrefixFunction(2, mode, receipts::add)),
                value -> Integer.parseInt(value) % 2, Types.INT);
    }

    private static String process(KeyedOneInputStreamOperatorTestHarness<Integer, String, String> harness, int id)
            throws Exception {
        harness.processElement(new StreamRecord<>(Integer.toString(id)));
        Object next = harness.getOutput().poll();
        assertTrue(next instanceof StreamRecord<?>);
        assertTrue(harness.getOutput().isEmpty());
        return (String) ((StreamRecord<?>) next).getValue();
    }

    private static String output(int id, int key, int count, int sum) {
        return "{\"version\":1,\"id\":" + id + ",\"key\":" + key + ",\"count\":" + count + ",\"prefixSum\":" + sum + "}";
    }

    private static List<String> events(List<String> receipts, String name) {
        var result = new ArrayList<String>();
        for (String receipt : receipts) if (receipt.startsWith("STATE_PREFIX_" + name + " ")) result.add(receipt);
        return result;
    }
}
