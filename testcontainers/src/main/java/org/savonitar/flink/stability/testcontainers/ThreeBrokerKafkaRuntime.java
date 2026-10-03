package org.savonitar.flink.stability.testcontainers;

import org.apache.kafka.clients.admin.Admin;
import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
        if (!started || name == null || !name.matches("broker-[1-3]")) throw new IllegalArgumentException("Unknown owned broker");
        int node = Integer.parseInt(name.substring(7));
        var broker = brokers.get(node - 1);
        String id = broker.getContainerId(), networkId = network.getId();
        int port = broker.getMappedPort(9092);
        var admin = Admin.create(Map.of("bootstrap.servers", endpoints().hostBootstrapServers(),
                "default.api.timeout.ms", Math.toIntExact(timeout.toMillis()), "request.timeout.ms", Math.toIntExact(timeout.toMillis())));
        try {
            return KafkaBrokerFault.execute(name, restart, partitions, timeout, new KafkaBrokerFault.Driver() {
                @Override public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline deadline) {
                    return ContainerDriverCallBoundary.call(ContainerOperationDeadline.shared("broker inspection", deadline),
                            "inspect " + name, () -> {
                        requireOwned(id, node, deadline);
                        try (var command = broker.getDockerClient().inspectContainerCmd(id)) {
                            var response = command.exec();
                            var attachments = response.getNetworkSettings().getNetworks().values();
                            if (!id.equals(response.getId()) || attachments.stream().noneMatch(attachment ->
                                    networkId.equals(attachment.getNetworkID()) && attachment.getAliases() != null
                                            && attachment.getAliases().contains(target.brokerAlias(node))))
                                throw new IllegalStateException("Broker ownership changed");
                            boolean running = Boolean.TRUE.equals(response.getState().getRunning());
                            if (running) {
                                var bindings = response.getNetworkSettings().getPorts().getBindings()
                                        .get(com.github.dockerjava.api.model.ExposedPort.tcp(9092));
                                if (bindings == null || Arrays.stream(bindings).noneMatch(binding -> Integer.toString(port).equals(binding.getHostPortSpec())))
                                    throw new IllegalStateException("Broker published port changed; advertised endpoint unconfirmed");
                            }
                            var snapshot = new KafkaBrokerControl.Snapshot(id, response.getImageId(), networkId, node, running);
                            var original = identities.putIfAbsent(name, snapshot);
                            if (original != null && (!original.containerId().equals(id) || !original.imageId().equals(snapshot.imageId())
                                    || !original.networkId().equals(networkId))) throw new IllegalStateException("Broker identity changed");
                            return snapshot;
                        }
                    });
                }
                @Override public void mutate(boolean restart, MonotonicDeadline deadline) {
                    ContainerDriverCallBoundary.run(ContainerOperationDeadline.shared("broker operation", deadline),
                            (restart ? "restart " : "kill ") + name, () -> {
                        requireOwned(id, node, deadline);
                        if (restart) { try (var command = broker.getDockerClient().startContainerCmd(id)) { command.exec(); } }
                        else { try (var command = broker.getDockerClient().killContainerCmd(id).withSignal("KILL")) { command.exec(); } }
                    });
                }
                @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> requested, MonotonicDeadline deadline) throws Exception {
                    requireOwned(id, node, deadline);
                    var metadata = admin.describeTopics(requested.stream().map(KafkaLogCapture.Partition::topic).distinct().toList())
                            .allTopicNames().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS);
                    var result = new ArrayList<KafkaBrokerControl.Leadership>();
                    for (var partition : requested) {
                        var observed = metadata.get(partition.topic()).partitions().stream()
                                .filter(item -> item.partition() == partition.partition()).findFirst().orElseThrow();
                        result.add(new KafkaBrokerControl.Leadership(partition.topic(), partition.partition(),
                                observed.leader() == null ? -1 : observed.leader().id(),
                                observed.replicas().stream().map(org.apache.kafka.common.Node::id).toList(),
                                observed.isr().stream().map(org.apache.kafka.common.Node::id).toList()));
                    }
                    return List.copyOf(result);
                }
            });
        } finally { admin.close(Duration.ZERO); }
    }
    private void requireOwned(String id, int node, MonotonicDeadline deadline) {
        if (!started || !id.equals(brokers.get(node - 1).getContainerId()) || deadline.remaining().isZero()
                || Thread.currentThread().isInterrupted()) throw new IllegalStateException("Broker owner/deadline unavailable");
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
