package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskManagerControlTest {
    @Test
    void legacyControlCannotPretendToRestartAnotherNamedSlotOrSupplyIdentity() throws Exception {
        List<Duration> restarts = new ArrayList<>();
        TaskManagerControl control = new TaskManagerControl() {
            @Override
            public void killTaskManager(String targetName, Duration timeout) {}

            @Override
            public void restartTaskManager(Duration timeout) {
                restarts.add(timeout);
            }
        };
        Duration timeout = Duration.ofSeconds(1);
        control.restartTaskManager("taskmanager-1", timeout);
        assertThrows(UnsupportedOperationException.class,
                () -> control.restartTaskManager("taskmanager-2", timeout));
        assertEquals(List.of(timeout), restarts);
        assertTrue(control.taskManagerIdentity("taskmanager-1").isEmpty());
        assertTrue(control.taskManagerIdentity("taskmanager-2").isEmpty());
        assertTrue(control.taskManagerIdentity("taskmanager-1", timeout).isEmpty());
        for (Duration invalid : List.of(Duration.ZERO, Duration.ofMillis(-1))) {
            assertThrows(IllegalArgumentException.class,
                    () -> control.taskManagerIdentity("taskmanager-1", invalid));
        }
    }

    @Test
    void identityRequiresAllThreeNames() {
        assertThrows(IllegalArgumentException.class,
                () -> new TaskManagerControl.Identity("", "physical", "resource"));
        assertThrows(IllegalArgumentException.class,
                () -> new TaskManagerControl.Identity("taskmanager-1", " ", "resource"));
        assertThrows(IllegalArgumentException.class,
                () -> new TaskManagerControl.Identity("taskmanager-1", "physical", ""));
    }
}
