package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaSelectedBrokerFaultTest {
    static final KafkaBrokerControl.Target TARGET = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.PARTITION_LEADER,
            null, "output", 0, null);
    static final List<KafkaLogCapture.Partition> PARTITIONS = List.of(new KafkaLogCapture.Partition("output", 0));
    @Test void pauseObservesPhysicalStateLeaderAndIsrThenResumesTheSelectedBroker() {
        var fake = new Fake(); var evidence = execute(fake, KafkaBrokerControl.Action.PAUSE);
        assertEquals(2, evidence.size()); assertTrue(evidence.stream().allMatch(KafkaBrokerControl.Evidence::confirmed), evidence.toString());
        assertTrue(evidence.getFirst().after().running()); assertTrue(evidence.getFirst().after().paused());
        assertFalse(evidence.getLast().after().paused()); assertEquals(1, fake.selections);
        assertEquals("broker-2", evidence.getFirst().selection().broker());
        assertEquals(List.of(KafkaBrokerControl.Action.PAUSE, KafkaBrokerControl.Action.RESUME), fake.actions);
        assertTrue(fake.time >= Duration.ofMillis(10).toNanos());
    }
    @Test void missingLeaderTransferIsUnconfirmedButStillHealedWithinTheHoldBound() {
        var fake = new Fake(); fake.transfer = false;
        var evidence = execute(fake, KafkaBrokerControl.Action.PAUSE);
        assertFalse(evidence.getFirst().confirmed()); assertFalse(fake.paused);
        assertEquals(KafkaBrokerControl.Action.RESUME, fake.actions.getLast());
        assertTrue(fake.time <= Duration.ofMillis(11).toNanos());
    }
    @Test void changedSelectionAndFailedSelectionDoNotInjectOrInventBrokerEvidence() {
        var fake = new Fake(); fake.changed = true;
        var evidence = execute(fake, KafkaBrokerControl.Action.KILL);
        assertFalse(evidence.getFirst().confirmed()); assertTrue(fake.actions.isEmpty());
        fake = new Fake(); fake.missing = true;
        evidence = execute(fake, KafkaBrokerControl.Action.KILL);
        assertNull(evidence.getFirst().selection()); assertTrue(fake.actions.isEmpty());
    }
    @Test void ambiguousMutationFailureStillHealsTheSameContainer() {
        var fake = new Fake(); fake.failMutation = true;
        var evidence = execute(fake, KafkaBrokerControl.Action.KILL);
        assertFalse(evidence.getFirst().confirmed()); assertTrue(fake.running);
        assertEquals(List.of(KafkaBrokerControl.Action.KILL, KafkaBrokerControl.Action.RESTART), fake.actions);
    }
    @Test void missingPreHealMetadataCannotSuppressResume() {
        var fake = new Fake(); fake.failHealMetadata = true;
        var evidence = execute(fake, KafkaBrokerControl.Action.PAUSE);
        assertFalse(fake.paused); assertEquals(KafkaBrokerControl.Action.RESUME, fake.actions.getLast());
        assertFalse(evidence.getLast().confirmed()); assertTrue(evidence.getLast().error().contains("safety heal"));
    }
    @Test void lateMetadataCannotConfirmAndDoesNotPreventSafetyHealing() {
        var fake = new Fake(); fake.lateMetadata = true;
        var evidence = execute(fake, KafkaBrokerControl.Action.PAUSE);
        assertFalse(evidence.getFirst().confirmed()); assertFalse(fake.paused);
        assertEquals(KafkaBrokerControl.Action.RESUME, fake.actions.getLast());
    }
    static List<KafkaBrokerControl.Evidence> execute(Fake fake, KafkaBrokerControl.Action action) {
        return KafkaBrokerFault.execute(new KafkaBrokerControl.Request(TARGET, action,
                Duration.ofMillis(10), Duration.ofMillis(30)), PARTITIONS, fake);
    }
    static class Fake implements KafkaBrokerFault.ClusterDriver, KafkaBrokerFault.Driver {
        long time; int selections; boolean running = true, paused, transfer = true, changed, missing, failMutation, failHealMetadata, lateMetadata;
        List<KafkaBrokerControl.Action> actions = new ArrayList<>();
        @Override public long nanoTime() { return time; }
        @Override public KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline deadline) {
            selections++; if (missing) throw new IllegalStateException("No open transaction");
            return new KafkaBrokerControl.Selection(target, "broker-2", "output", 0, 0, null, null, null, 2);
        }
        @Override public KafkaBrokerFault.Driver broker(String name) { assertEquals("broker-2", name); return this; }
        @Override public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline deadline) {
            return new KafkaBrokerControl.Snapshot("container-2", "image", "network", 2, running, paused);
        }
        @Override public void mutate(boolean restart, MonotonicDeadline deadline) { throw new AssertionError(); }
        @Override public void mutate(KafkaBrokerControl.Action action, MonotonicDeadline deadline) {
            actions.add(action);
            switch (action) { case KILL -> running = false; case RESTART -> running = true; case PAUSE -> paused = true; case RESUME -> paused = false; }
            if (failMutation && !action.heals()) throw new IllegalStateException("Ambiguous command response");
        }
        @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> ps, MonotonicDeadline deadline) {
            if (failHealMetadata && time >= 10_000_000) throw new IllegalStateException("Metadata unavailable while healing");
            if (lateMetadata && !actions.isEmpty() && !actions.getLast().heals()) time = 31_000_000;
            boolean absent = (!running || paused) && transfer;
            return List.of(new KafkaBrokerControl.Leadership("output", 0, absent || changed ? 3 : 2,
                    List.of(1,2,3), absent ? List.of(1,3) : List.of(1,2,3)));
        }
        @Override public void pause(MonotonicDeadline deadline) { time += 1_000_000; }
    }
}
