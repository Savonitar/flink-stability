package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class KafkaRuntimeStartupTest {
    @Test void endpointsStayUnavailableUntilTheLastBrokerCompletesItsReadinessWait() throws Exception {
        var target = new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.threeBrokers(), 3);
        var readyBrokers = new CountDownLatch(2);
        var laggingBrokerEntered = new CountDownLatch(1);
        var releaseLaggingBroker = new CountDownLatch(1);
        var brokers = List.<KafkaContainer>of(
                new StartupNode(1, readyBrokers, new CountDownLatch(0)),
                new StartupNode(2, readyBrokers, new CountDownLatch(0)),
                new StartupNode(3, laggingBrokerEntered, releaseLaggingBroker));
        var runtime = new ThreeBrokerKafkaRuntime(Network.SHARED, target, brokers);

        try (var starter = Executors.newSingleThreadExecutor()) {
            var startup = starter.submit(runtime::start);
            try {
                assertTrue(readyBrokers.await(2, TimeUnit.SECONDS), "The first two brokers must reach readiness");
                assertTrue(laggingBrokerEntered.await(2, TimeUnit.SECONDS), "The third broker must start concurrently");
                // The third broker is held at its readiness barrier; startup must remain pending.
                assertThrows(TimeoutException.class, () -> startup.get(100, TimeUnit.MILLISECONDS));
                assertThrows(IllegalStateException.class, runtime::endpoints);
            } finally {
                releaseLaggingBroker.countDown();
            }
            startup.get(2, TimeUnit.SECONDS);
            assertEquals("localhost:39091,localhost:39092,localhost:39093",
                    runtime.endpoints().hostBootstrapServers());
        } finally {
            runtime.stop();
        }
    }

    private static final class StartupNode extends KafkaContainer {
        private final int id;
        private final CountDownLatch entered;
        private final CountDownLatch readiness;

        StartupNode(int id, CountDownLatch entered, CountDownLatch readiness) {
            super("apache/kafka:4.0.0");
            this.id = id;
            this.entered = entered;
            this.readiness = readiness;
        }

        @Override public void start() {
            entered.countDown();
            try {
                if (!readiness.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Readiness barrier timed out");
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted before broker readiness", failure);
            }
        }

        @Override public void stop() { }
        @Override public String getBootstrapServers() { return "localhost:" + (39090 + id); }
    }
}
