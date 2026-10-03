package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Three combined KRaft controller/brokers; process faults preserve each container's local log. */
final class ThreeBrokerKafkaRuntime implements KafkaRuntimeCluster {
    private final KafkaRuntimeTarget target;
    private final Network network;
    private final List<KafkaContainer> brokers;
    private boolean attempted;
    private volatile boolean started;
    private final Map<String, KafkaBrokerControl.Snapshot> identities = new java.util.concurrent.ConcurrentHashMap<>();

    ThreeBrokerKafkaRuntime(Network network, KafkaRuntimeTarget target) {
        this(network, target, containers(network, target));
    }

    /** Lifecycle seam: fake containers exercise parallel start and cleanup without Docker. */
    ThreeBrokerKafkaRuntime(Network network, KafkaRuntimeTarget target, List<KafkaContainer> brokers) {
        if (target.brokers() != 3 || brokers.size() != 3 || new HashSet<>(brokers).size() != 3)
            throw new IllegalArgumentException("Three distinct broker containers required");
        this.target = target; this.network = Objects.requireNonNull(network); this.brokers = List.copyOf(brokers);
    }

    static List<KafkaContainer> containers(Network network, KafkaRuntimeTarget target) {
        if (target.brokers() != 3) throw new IllegalArgumentException("Three brokers required");
        String clusterId = org.apache.kafka.common.Uuid.randomUuid().toString();
        var nodes = new ArrayList<KafkaContainer>();
        var ports = hostPorts();
        for (int node = 1; node <= 3; node++) {
            String alias = target.brokerAlias(node);
            var container = new KafkaContainer(target.imageReference()).withNetwork(network)
                    .withNetworkAliases(alias).withListener(alias + ":19092")
                    .withEnv(environment(target, node, clusterId))
                    .withCreateContainerCmdModifier(command -> command.withHostName(alias))
                    .withStartupTimeout(Duration.ofMinutes(2))
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("KAFKA_BROKER_" + node + "_LOGS")));
            // Docker must retain this binding when start reuses Testcontainers' persisted starter script.
            // A competing bind fails startup; never fall back to an ephemeral advertised endpoint.
            container.setPortBindings(List.of(ports.get(node - 1) + ":9092"));
            nodes.add(container);
        }
        return List.copyOf(nodes);
    }

    private static List<Integer> hostPorts() {
        // Reserve all three together to prevent duplicate ephemeral selections.
        try (var first = new java.net.ServerSocket(0);
             var second = new java.net.ServerSocket(0);
             var third = new java.net.ServerSocket(0)) {
            return List.of(first.getLocalPort(), second.getLocalPort(), third.getLocalPort());
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Cannot allocate fixed Kafka host ports", failure);
        }
    }

    static Map<String, String> environment(KafkaRuntimeTarget target, int node, String clusterId) {
        var environment = new LinkedHashMap<String, String>();
        target.brokerPolicy().kafkaConfiguration().forEach((key, value) ->
                environment.put("KAFKA_" + key.toUpperCase(Locale.ROOT).replace('.', '_'), value));
        environment.put("CLUSTER_ID", clusterId);
        environment.put("KAFKA_NODE_ID", Integer.toString(node));
        environment.put("KAFKA_PROCESS_ROLES", "broker,controller");
        environment.put("KAFKA_CONTROLLER_QUORUM_VOTERS", java.util.stream.IntStream.rangeClosed(1, 3)
                .mapToObj(id -> id + "@" + target.brokerAlias(id) + ":9094").collect(Collectors.joining(",")));
        environment.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        environment.put("KAFKA_LOG_DIRS", "/tmp/kafka-logs");
        return Map.copyOf(environment);
    }

    @Override public void start() {
        if (attempted) throw new IllegalStateException("Kafka already attempted");
        attempted = true;
        // All controllers must start concurrently; allOf waits for every start, even when one fails.
        try (var starters = java.util.concurrent.Executors.newFixedThreadPool(3)) {
            CompletableFuture.allOf(brokers.stream().map(node -> CompletableFuture.runAsync(node::start, starters))
                    .toArray(CompletableFuture[]::new)).join();
        }
        started = true;
    }
    @Override public void stop() {
        started = false;
        if (!attempted) return;
        RuntimeException failure = null;
        for (var broker : brokers) {
            try { broker.stop(); } catch (RuntimeException error) {
                if (failure == null) failure = error; else failure.addSuppressed(error);
            }
        }
        if (failure != null) throw failure;
        attempted = false;
    }
    @Override public KafkaRuntimeEndpoints endpoints() {
        if (!started) throw new IllegalStateException("Kafka is not started");
        return new KafkaRuntimeEndpoints(target.clusterAlias(), target.imageReference(), target.internalBootstrapServers(),
                brokers.stream().map(KafkaContainer::getBootstrapServers).collect(Collectors.joining(",")));
    }

    @Override public KafkaBrokerControl.Evidence brokerOperation(String name, boolean restart,
            List<KafkaLogCapture.Partition> partitions, Duration timeout) {
        var deadline = MonotonicDeadline.start(timeout, System::nanoTime);
        try (var admin = new KafkaBrokerAdmin(endpoints().hostBootstrapServers(), timeout)) {
            return KafkaBrokerFault.execute(name, restart, partitions, deadline, driver(name, admin));
        }
    }
    private KafkaBrokerFault.Driver driver(String name, KafkaBrokerAdmin admin) {
        if (!started || name == null || !name.matches("broker-[1-3]")) throw new IllegalArgumentException("Unknown owned broker");
        int node = Integer.parseInt(name.substring(7));
        return new DockerKafkaBrokerDriver(brokers.get(node - 1), target, node, network.getId(), () -> started, admin, identities);
    }
    @Override public List<KafkaBrokerControl.Evidence> brokerFault(KafkaBrokerControl.Request request,
            List<KafkaLogCapture.Partition> partitions) {
        try (var admin = new KafkaBrokerAdmin(endpoints().hostBootstrapServers(), request.timeout())) {
            return KafkaSelectedBrokerFault.execute(request, partitions, new KafkaSelectedBrokerFault.Driver() {
                @Override public KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline deadline) throws Exception {
                    return admin.select(target, deadline);
                }
                @Override public KafkaBrokerFault.Driver broker(String name) { return driver(name, admin); }
            });
        }
    }
    @Override public KafkaLogCapture captureKafkaLogs(List<KafkaLogCapture.Partition> partitions, Path directory, MonotonicDeadline deadline) {
        if (!started) throw new IllegalStateException("Kafka owner unavailable");
        // One owned replica for every RF=3 partition; do not multiply the shared capture budgets.
        var broker = brokers.getFirst(); String id = broker.getContainerId(), networkId = network.getId();
        var binding = new OwnedKafkaArchiveCapture.Binding(id, networkId, target.brokerAlias(1), target.clusterAlias(), target.imageReference(), 1);
        return SelectiveKafkaLogCapture.collect(binding, ApacheKafkaRuntime.archiveDriver(broker.getDockerClient(), id, networkId),
                partitions, directory, deadline);
    }
}
