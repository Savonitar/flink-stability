package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.containers.Network;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterManagerKafkaRuntimeTest {

    @Test
    void startsOnTheManagersNetworkAndPublishesBothEndpoints() {
        AtomicReference<Network> observedNetwork = new AtomicReference<>();
        AtomicReference<KafkaRuntimeTarget> observedTarget = new AtomicReference<>();
        RecordingRuntime runtime = new RecordingRuntime(endpoints());
        ClusterManager manager = manager((network, target) -> {
            observedNetwork.set(network);
            observedTarget.set(target);
            return runtime;
        });

        KafkaRuntimeEndpoints published = manager.startKafka(target());

        assertSame(Network.SHARED, observedNetwork.get());
        assertEquals(target(), observedTarget.get());
        assertEquals(endpoints(), published);
        assertEquals(List.of("start", "endpoints"), runtime.events);

        assertThrows(IllegalStateException.class, () -> manager.startKafka(target()));
        manager.close();
        manager.close();
        assertEquals(List.of("start", "endpoints", "stop"), runtime.events);
    }

    @Test
    void endpointIdentityMismatchStopsCandidateAndPublishesNothing() {
        RecordingRuntime runtime = new RecordingRuntime(new KafkaRuntimeEndpoints(
                "other", "apache/kafka:4.0.0", "kafka-other:19092", "localhost:19093"));
        ClusterManager manager = manager((network, target) -> runtime);

        assertThrows(IllegalStateException.class, () -> manager.startKafka(target()));

        assertEquals(List.of("start", "endpoints", "stop"), runtime.events);
        manager.close();
        assertEquals(List.of("start", "endpoints", "stop"), runtime.events);
    }

    @Test
    void retainsFailedStartupCleanupForCloseRetry() {
        RecordingRuntime runtime = new RecordingRuntime(endpoints());
        runtime.failStart = true;
        runtime.failStopOnce = true;
        ClusterManager manager = manager((network, target) -> runtime);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class, () -> manager.startKafka(target()));

        assertEquals("start failed", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals(List.of("start", "stop"), runtime.events);
        manager.close();
        manager.close();
        assertEquals(List.of("start", "stop", "stop"), runtime.events);
    }

    private static ClusterManager manager(ClusterManager.KafkaRuntimeFactory runtimeFactory) {
        return new ClusterManager(
                Network.SHARED,
                false,
                (target, network, checkpointRoot) -> {
                    throw new AssertionError("Flink must not be started");
                },
                runtimeFactory,
                Path.of("target", "cluster-manager-kafka-test"));
    }

    private static KafkaRuntimeTarget target() {
        return new KafkaRuntimeTarget(
                "main",
                "apache/kafka:4.0.0",
                new KafkaBrokerPolicy(Duration.ofHours(2), 1, 1, 1, Duration.ZERO));
    }

    private static KafkaRuntimeEndpoints endpoints() {
        return new KafkaRuntimeEndpoints(
                "main", "apache/kafka:4.0.0", "kafka-main:19092", "localhost:19093");
    }

    private static final class RecordingRuntime implements KafkaRuntimeCluster {
        private final KafkaRuntimeEndpoints endpoints;
        private final List<String> events = new ArrayList<>();
        private boolean failStart;
        private boolean failStopOnce;

        private RecordingRuntime(KafkaRuntimeEndpoints endpoints) {
            this.endpoints = endpoints;
        }

        @Override
        public void start() {
            events.add("start");
            if (failStart) {
                throw new IllegalStateException("start failed");
            }
        }

        @Override
        public void stop() {
            events.add("stop");
            if (failStopOnce) {
                failStopOnce = false;
                throw new IllegalStateException("stop failed");
            }
        }

        @Override
        public KafkaRuntimeEndpoints endpoints() {
            events.add("endpoints");
            return endpoints;
        }
    }
}
