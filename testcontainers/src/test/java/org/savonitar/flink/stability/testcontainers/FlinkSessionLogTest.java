package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkSessionLogTest {
    private static final String MESSAGE = "INFO org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.ClientCnxn - "
            + "Session establishment complete on server localhost/127.0.0.1:2181, session id = 0x123, negotiated timeout = 6000\n";

    @Test
    void fragmentedOwnProcessLogsRetainRequestedAndNegotiatedValuesSeparately() {
        var log = log("jobmanager-1#1");
        assertTrue(log.snapshot().isEmpty());
        log.created("container-old");
        log.accept(MESSAGE.substring(0, 100));
        assertTrue(log.snapshot().orElseThrow().negotiated().isEmpty());
        log.accept(MESSAGE.substring(100));
        var evidence = log.snapshot().orElseThrow();
        assertEquals("container-old", evidence.runtimeId());
        assertEquals("jobmanager-1#1", evidence.classLoadProcess());
        assertEquals(6_000, evidence.requestedTimeoutMillis());
        assertEquals(6_000, evidence.negotiated().getFirst().timeoutMillis());
        log.accept(MESSAGE.replace("6000", "10000"));
        assertEquals(10_000, log.snapshot().orElseThrow().negotiated().getLast().timeoutMillis());
        assertFalse(log.snapshot().orElseThrow().overflow());
    }

    @Test
    void harnessObserverLogsAndOtherLoggersCannotSupplyFlinkSessionEvidence() {
        var log = log("jobmanager-1#1");
        log.created("container-old");
        log.accept(MESSAGE.replace("org.apache.flink.shaded.zookeeper3.", ""));
        log.accept(MESSAGE.replace("ClientCnxn", "UnrelatedLogger"));
        log.accept(null);
        assertTrue(log.snapshot().orElseThrow().negotiated().isEmpty());
    }

    @Test
    void replacementCollectorHasItsOwnIdentityAndRetryAmbiguityFailsClosed() {
        var old = log("jobmanager-1#1");
        old.created("container-old");
        old.accept(MESSAGE);
        var replacement = log("jobmanager-1#2");
        replacement.created("container-new");
        assertTrue(replacement.snapshot().orElseThrow().negotiated().isEmpty());
        replacement.accept(MESSAGE.replace("0x123", "0x456"));
        assertEquals("0x456", replacement.snapshot().orElseThrow().negotiated().getFirst().sessionId());
        old.created("ambiguous-retry");
        old.accept(MESSAGE.replace("0x123", "0x999"));
        assertTrue(old.snapshot().orElseThrow().overflow());
        assertEquals("container-old", old.snapshot().orElseThrow().runtimeId());
        assertEquals(1, old.snapshot().orElseThrow().negotiated().size());
    }

    @Test
    void sessionAndLineLimitsRemainExplicitRatherThanDroppingEvidenceSilently() {
        var log = log("jobmanager-1#1");
        log.created("container-old");
        for (int i = 0; i <= FlinkSessionLog.MAX_SESSIONS; i++) log.accept(MESSAGE);
        assertEquals(FlinkSessionLog.MAX_SESSIONS, log.snapshot().orElseThrow().negotiated().size());
        assertTrue(log.snapshot().orElseThrow().overflow());
        var oversized = log("jobmanager-1#1");
        oversized.created("container-old");
        oversized.accept("x".repeat(FlinkSessionLog.MAX_LINE_CHARS + 1) + "\n" + MESSAGE);
        assertTrue(oversized.snapshot().orElseThrow().overflow());
        assertEquals(1, oversized.snapshot().orElseThrow().negotiated().size());
    }

    private static FlinkSessionLog log(String process) {
        return new FlinkSessionLog("jobmanager-1", FlinkComponentRole.JOB_MANAGER, process, 6_000);
    }
}
