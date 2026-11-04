package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StorageFaultIoCounterTest {
    @Test void supportsOnlyAbsentHistoricalSixOrCurrentEightCounterInventories() {
        assertTrue(status(Map.of()).counters().isEmpty());
        for (boolean syncCounters : new boolean[]{false, true}) {
            var counters = inventory(syncCounters);
            var receipt = status(counters);
            assertEquals(syncCounters ? 8 : 6, receipt.counters().size());
            assertThrows(UnsupportedOperationException.class, () -> receipt.counters().clear());
            counters.remove("release");
            assertThrows(IllegalArgumentException.class, () -> status(counters));
            assertEquals(syncCounters ? 8 : 6, receipt.counters().size(), "Receipt is an immutable snapshot");
        }
        var renamed = inventory(true);
        renamed.put("unknown", renamed.remove("fdatasync"));
        assertThrows(IllegalArgumentException.class, () -> status(renamed));
        var partial = inventory(false);
        partial.put("fsync", new StorageFaultControl.IoCounter(1, 0));
        assertThrows(IllegalArgumentException.class, () -> status(partial));
    }

    @Test void syncCountsAcceptLongMaximumButCannotCarryBytes() {
        var counters = inventory(true);
        for (String name : new String[]{"fsync", "fdatasync"}) {
            counters.put(name, new StorageFaultControl.IoCounter(Long.MAX_VALUE, 0));
            assertEquals(Long.MAX_VALUE, status(counters).counters().get(name).count());
            counters.put(name, new StorageFaultControl.IoCounter(1, 1));
            assertThrows(IllegalArgumentException.class, () -> status(counters));
            counters.put(name, new StorageFaultControl.IoCounter(0, 0));
        }
        assertThrows(IllegalArgumentException.class, () -> new StorageFaultControl.IoCounter(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> new StorageFaultControl.IoCounter(1, -1));
        assertThrows(IllegalArgumentException.class, () -> new StorageFaultControl.IoCounter(0, 1));
    }

    private static HashMap<String, StorageFaultControl.IoCounter> inventory(boolean syncCounters) {
        var counters = new HashMap<String, StorageFaultControl.IoCounter>();
        for (String name : new String[]{"read", "getattr", "readdir", "access", "flush", "release"})
            counters.put(name, new StorageFaultControl.IoCounter(0, 0));
        if (syncCounters) {
            counters.put("fsync", new StorageFaultControl.IoCounter(0, 0));
            counters.put("fdatasync", new StorageFaultControl.IoCounter(0, 0));
        }
        return counters;
    }

    private static StorageFaultControl.Status status(Map<String, StorageFaultControl.IoCounter> counters) {
        return new StorageFaultControl.Status(StorageFaultControl.State.HEALTHY, null, 0, 1, 0, 0, true, false, counters);
    }
}
