package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class KafkaBrokerFaultTest {
    private static final List<KafkaLogCapture.Partition> PARTITIONS = List.of(new KafkaLogCapture.Partition("output", 0));
    @Test void environmentUsesOneQuorumAndDifferentNodesWithThreeReplicasAndTwoIsr() {
        var target = new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.threeBrokers(), 3);
        for (int node = 1; node <= 3; node++) {
            var env = ThreeBrokerKafkaRuntime.environment(target, node, "cluster");
            assertEquals(Integer.toString(node), env.get("KAFKA_NODE_ID"));
            assertEquals("1@kafka-main-broker-1:9094,2@kafka-main-broker-2:9094,3@kafka-main-broker-3:9094", env.get("KAFKA_CONTROLLER_QUORUM_VOTERS"));
            assertEquals("3", env.get("KAFKA_DEFAULT_REPLICATION_FACTOR"));
            assertEquals("3", env.get("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"));
            assertEquals("3", env.get("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"));
            assertEquals("2", env.get("KAFKA_MIN_INSYNC_REPLICAS"));
            assertEquals("2", env.get("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"));
        }
        assertEquals("kafka-main-broker-1:19092,kafka-main-broker-2:19092,kafka-main-broker-3:19092", target.internalBootstrapServers());
        // Constructor/configuration must remain Docker-free.
        new ThreeBrokerKafkaRuntime(org.testcontainers.containers.Network.SHARED, target);
    }
    @Test void startWaitsForAllThreeControllersAndCleanupVisitsEveryOwnerAfterFailure() {
        var target = new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.threeBrokers(), 3);
        var entered = new java.util.concurrent.CountDownLatch(3);
        var nodes = java.util.stream.IntStream.rangeClosed(1, 3).mapToObj(id -> new LifecycleNode(id, entered)).toList();
        nodes.get(1).failStart = true;
        var runtime = new ThreeBrokerKafkaRuntime(org.testcontainers.containers.Network.SHARED, target,
                new java.util.ArrayList<org.testcontainers.kafka.KafkaContainer>(nodes));
        assertThrows(java.util.concurrent.CompletionException.class, runtime::start);
        assertEquals(0, entered.getCount());
        nodes.getFirst().failStop = true;
        assertThrows(IllegalStateException.class, runtime::stop);
        nodes.forEach(node -> assertEquals(1, node.stops));
        nodes.getFirst().failStop = false; runtime.stop();
        nodes.forEach(node -> assertEquals(2, node.stops));
    }
    private static class LifecycleNode extends org.testcontainers.kafka.KafkaContainer {
        private final java.util.concurrent.CountDownLatch entered; private final int id;
        boolean failStart, failStop; int stops;
        LifecycleNode(int id, java.util.concurrent.CountDownLatch entered) { super("apache/kafka:4.0.0"); this.id = id; this.entered = entered; }
        @Override public void start() {
            entered.countDown();
            try { assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS), "All controllers must start together"); }
            catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IllegalStateException(failure); }
            if (failStart) throw new IllegalStateException("startup failed");
        }
        @Override public void stop() { stops++; if (failStop) throw new IllegalStateException("stop failed"); }
        @Override public String getBootstrapServers() { return "localhost:" + (39090 + id); }
    }

    @Test void killAndRestartNeedStoppedRunningLeaderTransferAndIsrRecovery() {
        var driver = new Fake();
        var killed = run(false, driver);
        assertTrue(killed.confirmed(), killed.toString()); assertFalse(killed.after().running());
        assertEquals(1, killed.leadersBefore().getFirst().leader()); assertEquals(2, killed.leadersAfter().getFirst().leader());
        var restarted = run(true, driver);
        assertTrue(restarted.confirmed(), restarted.toString()); assertTrue(restarted.after().running());
        assertEquals(killed.before().containerId(), restarted.after().containerId());
    }
    @Test void unchangedLeaderTimeoutAndMetadataFailureRetainPartialEvidence() {
        var driver = new Fake(); driver.transfer = false;
        var result = run(false, driver);
        assertFalse(result.confirmed()); assertNotNull(result.error()); assertFalse(result.after().running());
        driver = new Fake(); driver.metadataFailure = true;
        result = run(false, driver);
        assertFalse(result.confirmed()); assertTrue(driver.running); assertEquals(0, driver.mutations);
    }
    @Test void replacingContainerOrRestartWithoutIsrRecoveryCannotConfirm() {
        var driver = new Fake(); driver.replace = true;
        assertFalse(run(false, driver).confirmed());
        driver = new Fake(); driver.running = false; driver.rejoin = false;
        assertFalse(run(true, driver).confirmed());
    }
    private static KafkaBrokerControl.Evidence run(boolean restart, Fake driver) {
        var deadline = MonotonicDeadline.start(Duration.ofMillis(3), driver.time::get);
        return KafkaBrokerFault.execute("broker-1", restart, PARTITIONS, deadline, driver);
    }
    private static class Fake implements KafkaBrokerFault.Driver {
        AtomicLong time = new AtomicLong(); boolean running = true, transfer = true, rejoin = true, replace, metadataFailure; int mutations;
        @Override public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline deadline) {
            return new KafkaBrokerControl.Snapshot(replace && mutations > 0 ? "replacement" : "container", "image", "network", 1, running);
        }
        @Override public void mutate(boolean restart, MonotonicDeadline deadline) { mutations++; running = restart; }
        @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline) {
            if (metadataFailure) throw new IllegalStateException("metadata failure");
            return List.of(new KafkaBrokerControl.Leadership("output", 0, running || !transfer ? 1 : 2,
                    List.of(1, 2, 3), running && rejoin ? List.of(1, 2, 3) : List.of(2, 3)));
        }
        @Override public void pause(MonotonicDeadline deadline) { time.addAndGet(1_000_000); }
    }
}
