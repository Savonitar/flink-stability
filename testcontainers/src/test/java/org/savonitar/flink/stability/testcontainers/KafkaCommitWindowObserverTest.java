package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaCommitWindowObserverTest {
    static final KafkaBrokerControl.Target TARGET = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR, null, null, -1, "sink-");
    @Test void provesBothModesAndTransactionVersionsBeforeHealing() {
        for (var mode : List.of(KafkaBrokerControl.Action.KILL, KafkaBrokerControl.Action.PAUSE)) for (int version : List.of(1,2)) {
            var fake = new Fake(); fake.version = version;
            var evidence = execute(fake, mode);
            assertEquals(2, evidence.size());
            assertTrue(evidence.stream().allMatch(KafkaBrokerControl.Evidence::confirmed), evidence.toString());
            var window = evidence.getFirst().commitWindow();
            assertEquals("ONGOING", window.ongoing().state()); assertEquals("COMPLETE_COMMIT", window.committed().state());
            assertEquals(version == 2 ? 5 : 4, window.committed().producerEpoch());
            assertEquals(2, window.brokerSessionTimeoutMillis());
            assertTrue(window.committed().elapsedAfterFaultNanos() < 10_000_000);
            assertTrue(fake.running); assertFalse(fake.paused);
            assertEquals(mode == KafkaBrokerControl.Action.KILL ? KafkaBrokerControl.Action.RESTART : KafkaBrokerControl.Action.RESUME, fake.actions.getLast());
        }
    }
    @Test void completedBeforeOpenWitnessAbortedReplacedAndLateTransactionsCannotConfirm() {
        for (String failure : List.of("already-complete", "aborted", "producer-changed", "epoch-changed", "never-commits", "late", "wrong-coordinator", "healed-early")) {
            var fake = new Fake(); fake.failure = failure;
            var evidence = execute(fake, KafkaBrokerControl.Action.PAUSE);
            assertFalse(evidence.getFirst().confirmed(), failure);
            assertNotNull(evidence.getFirst().commitWindow());
            assertTrue(fake.actions.contains(KafkaBrokerControl.Action.RESUME), failure);
            assertFalse(fake.paused);
        }
    }
    @Test void missingOrTooLongBrokerSessionTimeoutPreventsMutation() {
        for (long session : List.of(0L, 10L, 5000L)) {
            var fake = new Fake(); fake.session = session;
            var evidence = execute(fake, KafkaBrokerControl.Action.KILL);
            assertFalse(evidence.getFirst().confirmed()); assertTrue(fake.actions.isEmpty());
        }
    }
    @Test void confirmedWindowCannotUseDifferentContainerIdentity() {
        var evidence = execute(new Fake(), KafkaBrokerControl.Action.PAUSE).getFirst();
        var original = evidence.commitWindow(); var c = original.committed();
        var forged = new KafkaCommitWindow.Observation(c.transactionalId(), c.producerId(), c.producerEpoch(), c.state(), c.coordinatorId(), c.observedAtMillis(), c.elapsedAfterFaultNanos(),
            new KafkaBrokerControl.Snapshot("different-container", "image", "network", 2, true, true), c.partition());
        assertFalse(evidence.withCommitWindow(new KafkaCommitWindow(1,2,original.ongoing(),forged,null)).confirmed());
    }
    static List<KafkaBrokerControl.Evidence> execute(Fake fake, KafkaBrokerControl.Action mode) {
        return KafkaBrokerFault.execute(new KafkaBrokerControl.Request(TARGET, mode, Duration.ofMillis(10), Duration.ofMillis(30), fake.version), List.of(new KafkaLogCapture.Partition("output", 0)), fake);
    }
    static class Fake extends KafkaSelectedBrokerFaultTest.Fake {
        int version=1; long session=2; String failure="";
        @Override public long clockMillis() { return time/1_000_000; }
        @Override public long sessionTimeoutMillis(int brokerId, MonotonicDeadline deadline) { assertEquals(2,brokerId); return session; }
        @Override public KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline deadline) {
            return new KafkaBrokerControl.Selection(target,"broker-2","__transaction_state",0,1,"sink-1",77L,4,2);
        }
        @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> ps, MonotonicDeadline deadline) {
            boolean held = !running || paused;
            return ps.stream().map(p -> new KafkaBrokerControl.Leadership(p.topic(),p.partition(),held ? 3 : 2,List.of(1,2,3),held ? List.of(1,3) : List.of(1,2,3))).toList();
        }
        @Override public KafkaCommitWindow.Transaction transaction(String id, MonotonicDeadline deadline) {
            assertFalse(actions.isEmpty());
            if (failure.equals("late")) time=11_000_000;
            if (failure.equals("healed-early")) { running=true; paused=false; }
            boolean complete=time>=4_000_000 || failure.equals("already-complete");
            String state = failure.equals("aborted") ? "COMPLETE_ABORT" : failure.equals("never-commits") ? "ONGOING" : complete ? "COMPLETE_COMMIT" : "ONGOING";
            return new KafkaCommitWindow.Transaction(id,failure.equals("producer-changed") ? 88 : 77,
                failure.equals("epoch-changed") ? 8 : 4+(version==2 && complete ? 1:0),state,failure.equals("wrong-coordinator") ? 1:3);
        }
    }
}
