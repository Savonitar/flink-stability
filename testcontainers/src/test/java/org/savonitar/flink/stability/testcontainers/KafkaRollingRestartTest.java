package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaRollingRestartTest {
    static final KafkaBrokerControl.Target TARGET = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR,
            null, null, -1, "sink-");
    static List<KafkaBrokerControl.Evidence> execute(Fake driver, KafkaBrokerControl.RollingOrder order, boolean elect) {
        return KafkaBrokerFault.execute(new KafkaBrokerControl.Request(TARGET, KafkaBrokerControl.Action.ROLLING_RESTART,
                Duration.ZERO, Duration.ofMillis(50), null, order, elect), List.of(new KafkaLogCapture.Partition("output", 0)), driver);
    }
    @Test void fixedNontransactionalRollingRequiresFullIsrWithoutInventingTransactionMetadata() {
        var target = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.PARTITION_LEADER, null, "output", 0, null);
        for (boolean loseIsr : List.of(false, true)) {
            var driver = new Fake() {
                public List<KafkaLogCapture.Partition> transactionPartitions(MonotonicDeadline d) { throw new AssertionError("No transactional workload"); }
                public KafkaBrokerControl.Selection select(KafkaBrokerControl.Target requested, MonotonicDeadline d) {
                    return new KafkaBrokerControl.Selection(requested, "broker-1", "output", 0, 0, null, null, null, 1);
                }
            };
            driver.neverFull = loseIsr;
            var result = KafkaBrokerFault.execute(new KafkaBrokerControl.Request(target, KafkaBrokerControl.Action.ROLLING_RESTART,
                    Duration.ZERO, Duration.ofMillis(50), null, KafkaBrokerControl.RollingOrder.FIXED, false),
                    List.of(new KafkaLogCapture.Partition("output", 0)), driver);
            assertEquals(loseIsr ? 1 : 3, driver.stops.size());
            assertEquals(!loseIsr, result.stream().allMatch(KafkaBrokerControl.Evidence::confirmed));
            assertNull(result.getFirst().rolling().reference().transactionalId());
            assertTrue(driver.running.values().stream().allMatch(Boolean::booleanValue));
        }
        assertThrows(IllegalArgumentException.class, () -> new KafkaBrokerControl.Request(target, KafkaBrokerControl.Action.ROLLING_RESTART,
                Duration.ZERO, Duration.ofSeconds(30), null, KafkaBrokerControl.RollingOrder.COORDINATOR_FIRST, false));
    }

    @Test void allOrdersStopOnlyOneBrokerAndWaitForEveryTransactionPartitionToRejoin() {
        for (var order : KafkaBrokerControl.RollingOrder.values()) {
            var driver = new Fake(); var result = execute(driver, order, false);
            var expected = switch (order) {
                case FIXED -> List.of(1,2,3); case COORDINATOR_FIRST -> List.of(2,1,3); case COORDINATOR_LAST -> List.of(1,3,2);
            };
            assertEquals(expected, driver.stops); assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(KafkaBrokerControl.Evidence::confirmed), result.toString());
            for (int i = 0; i < 3; i++) {
                var progress = result.get(i * 2).rolling(); assertEquals(i + 1, progress.ordinal());
                assertEquals(3, progress.fullyReplicated().size());
                assertEquals("sink-open", progress.reference().transactionalId());
                assertTrue(progress.recoveredAtMillis() >= progress.startedAtMillis() + 3);
                if (i > 0) assertTrue(progress.startedAtMillis() >= result.get(i * 2 - 1).rolling().recoveredAtMillis());
            }
        }
    }
    @Test void noNextBrokerWhenFullIsrCannotBeRestoredEvenIfTwoReplicasAreHealthy() {
        var driver = new Fake(); driver.neverFull = true;
        var result = execute(driver, KafkaBrokerControl.RollingOrder.FIXED, false);
        assertEquals(List.of(1), driver.stops); assertTrue(driver.running.values().stream().allMatch(Boolean::booleanValue));
        assertFalse(result.getLast().confirmed()); assertTrue(driver.time <= 50_000_000L);
    }
    @Test void ambiguousTermStillRestartsSameBrokerAndStopsTheSequence() {
        var driver = new Fake(); driver.failStop = true;
        var result = execute(driver, KafkaBrokerControl.RollingOrder.FIXED, false);
        assertEquals(List.of(1), driver.stops); assertEquals(List.of(1), driver.starts);
        assertFalse(result.getFirst().confirmed()); assertTrue(driver.running.get(1));
    }
    @Test void preferredElectionMustBeObservedWithFullIsrAfterAllThreeRestarts() {
        var driver = new Fake(); var result = execute(driver, KafkaBrokerControl.RollingOrder.FIXED, true);
        assertEquals(1, driver.elections); assertEquals(3, driver.stops.size());
        assertTrue(result.getLast().rolling().preferredElectionConfirmed()); assertTrue(result.getLast().confirmed());
        driver = new Fake(); driver.ignoreElection = true;
        result = execute(driver, KafkaBrokerControl.RollingOrder.FIXED, true);
        assertFalse(result.getLast().confirmed());
    }
    @Test void missingTransactionMetadataAndLostInitialIsrDoNotStopAnything() {
        for (boolean missing : List.of(true, false)) {
            var driver = new Fake(); driver.missingMetadata = missing; driver.initialPartial = !missing;
            var result = execute(driver, KafkaBrokerControl.RollingOrder.FIXED, false);
            assertTrue(driver.stops.isEmpty()); assertFalse(result.getFirst().confirmed());
        }
    }
    static class Fake implements KafkaBrokerFault.ClusterDriver {
        long time, recoverAt; int recovering, elections; boolean neverFull, failStop, ignoreElection, missingMetadata, initialPartial;
        Map<Integer,Boolean> running = new HashMap<>(Map.of(1,true,2,true,3,true));
        Map<String,Integer> leaders = new HashMap<>(Map.of("output/0", 1, "__transaction_state/0", 1, "__transaction_state/1", 2));
        List<Integer> stops = new ArrayList<>(), starts = new ArrayList<>();
        public long nanoTime() { return time; } public long clockMillis() { return time / 1_000_000; }
        public List<KafkaLogCapture.Partition> transactionPartitions(MonotonicDeadline d) {
            if (missingMetadata) throw new IllegalStateException("Missing transaction metadata");
            return List.of(new KafkaLogCapture.Partition("__transaction_state",0), new KafkaLogCapture.Partition("__transaction_state",1));
        }
        public KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline d) {
            return new KafkaBrokerControl.Selection(target, "broker-2", "__transaction_state", 1, 2, "sink-open", 42L, 0, 2);
        }
        public void electPreferred(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline d) {
            assertEquals(3, starts.size()); elections++;
            if (!ignoreElection) leaders.replaceAll((key,value) -> 1);
            else leaders.replaceAll((key,value) -> 2);
        }
        public KafkaBrokerFault.Driver broker(String name) {
            int id = Integer.parseInt(name.substring(7));
            return new KafkaBrokerFault.Driver() {
                public long nanoTime() { return time; }
                public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline d) {
                    return new KafkaBrokerControl.Snapshot("container-" + id,"image","network",id,running.get(id));
                }
                public void mutate(boolean restart, MonotonicDeadline d) { throw new AssertionError(); }
                public void mutate(KafkaBrokerControl.Action action, MonotonicDeadline d) {
                    assertTrue(action == KafkaBrokerControl.Action.STOP || action == KafkaBrokerControl.Action.RESTART);
                    if (action == KafkaBrokerControl.Action.STOP) {
                        assertTrue(running.values().stream().allMatch(Boolean::booleanValue));
                        assertTrue(time >= recoverAt); stops.add(id); running.put(id,false);
                        int successor = id == 3 ? 1 : id + 1; leaders.replaceAll((key,value) -> value == id ? successor : value);
                        if (failStop) throw new IllegalStateException("Ambiguous TERM response");
                    } else { starts.add(id); running.put(id,true); recovering=id; recoverAt=time+3_000_000; }
                }
                public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline d) {
                    var isr = new ArrayList<>(List.of(1,2,3)); isr.removeIf(node -> !running.get(node));
                    if (initialPartial || !starts.isEmpty() && (neverFull || time < recoverAt)) isr.remove(Integer.valueOf(recovering == 1 ? 2 : 1));
                    return partitions.stream().map(p -> new KafkaBrokerControl.Leadership(p.topic(),p.partition(),
                            leaders.get(p.topic()+"/"+p.partition()),List.of(1,2,3),isr)).toList();
                }
                public void pause(MonotonicDeadline d) { time += 1_000_000; }
            };
        }
    }
}
