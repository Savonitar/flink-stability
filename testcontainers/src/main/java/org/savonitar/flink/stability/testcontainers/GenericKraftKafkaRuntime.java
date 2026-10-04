package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Explicit one- or three-node KRaft launcher using the declared /opt/kafka binary layout. */
final class GenericKraftKafkaRuntime implements KafkaRuntimeCluster {
    private final KafkaRuntimeTarget target;
    private final Network network;
    private final List<GenericKraftKafkaContainer> brokers;
    private boolean attempted;
    private volatile boolean started;
    private final Map<String, KafkaBrokerControl.Snapshot> identities = new java.util.concurrent.ConcurrentHashMap<>();

    GenericKraftKafkaRuntime(Network network, KafkaRuntimeTarget target) {
        this(network, target, containers(network, target));
    }

    /** Lifecycle seam: fake containers exercise parallel start and cleanup without Docker. */
    GenericKraftKafkaRuntime(Network network, KafkaRuntimeTarget target, List<GenericKraftKafkaContainer> brokers) {
        if (brokers.size() != target.brokers() || new HashSet<>(brokers).size() != target.brokers())
            throw new IllegalArgumentException("One distinct container is required for each declared broker");
        this.target = target; this.network = Objects.requireNonNull(network); this.brokers = List.copyOf(brokers);
    }

    static List<GenericKraftKafkaContainer> containers(Network network, KafkaRuntimeTarget target) {
        var nodes = new ArrayList<GenericKraftKafkaContainer>();
        List<Integer> ports = hostPorts(target.brokers());
        for (int node = 1; node <= target.brokers(); node++) {
            nodes.add(new GenericKraftKafkaContainer(network, target, node, ports.get(node - 1))
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("KAFKA_GENERIC_BROKER_" + node + "_LOGS"))));
        }
        return List.copyOf(nodes);
    }

    private static List<Integer> hostPorts(int count) {
        var sockets = new ArrayList<java.net.ServerSocket>();
        try {
            for (int node = 0; node < count; node++) sockets.add(new java.net.ServerSocket(0));
            return sockets.stream().map(java.net.ServerSocket::getLocalPort).toList();
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Cannot allocate fixed Kafka host ports", failure);
        } finally {
            for (var socket : sockets) {
                try { socket.close(); } catch (java.io.IOException ignored) { /* No broker is running yet. */ }
            }
        }
    }

    @Override public void start() {
        if (attempted) throw new IllegalStateException("Kafka already attempted");
        attempted = true;
        // All controllers must start concurrently; allOf waits for every start, even when one fails.
        try (var starters = java.util.concurrent.Executors.newFixedThreadPool(target.brokers())) {
            CompletableFuture.allOf(brokers.stream().map(node -> CompletableFuture.runAsync(() -> { node.start(); node.markReady(); }, starters))
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
                brokers.stream().map(GenericKraftKafkaContainer::bootstrapServers).collect(Collectors.joining(",")),
                runtimeEvidence());
    }

    @Override public Optional<KafkaRuntimeEvidence> runtimeEvidence() {
        return Optional.of(new KafkaRuntimeEvidence(target.clusterAlias(), target.imageReference(), target.imageId(),
                target.launchType(), target.brokerConfig(), brokers.stream()
                        .flatMap(broker -> broker.retainedEvidence().stream()).toList()));
    }

    @Override public KafkaBrokerControl.Evidence brokerOperation(String name, boolean restart,
            List<KafkaLogCapture.Partition> partitions, Duration timeout) {
        var deadline = MonotonicDeadline.start(timeout, System::nanoTime);
        try (var admin = new KafkaBrokerAdmin(endpoints().hostBootstrapServers(), timeout)) {
            return KafkaBrokerFault.execute(name, restart, partitions, deadline, driver(name, admin));
        }
    }
    private KafkaBrokerFault.Driver driver(String name, KafkaBrokerAdmin admin) {
        if (!started || name == null || !name.matches("broker-[1-" + target.brokers() + "]")) throw new IllegalArgumentException("Unknown owned broker");
        int node = Integer.parseInt(name.substring(7));
        return new DockerKafkaBrokerDriver(brokers.get(node - 1), target, node, network.getId(), () -> started, admin, identities);
    }
    @Override public List<KafkaBrokerControl.Evidence> brokerFault(KafkaBrokerControl.Request request,
            List<KafkaLogCapture.Partition> partitions) {
        try (var admin = new KafkaBrokerAdmin(endpoints().hostBootstrapServers(), request.timeout())) {
            return KafkaBrokerFault.execute(request, partitions, new KafkaBrokerFault.ClusterDriver() {
                @Override public List<KafkaLogCapture.Partition> transactionPartitions(MonotonicDeadline deadline) throws Exception {
                    return admin.transactionPartitions(deadline);
                }
                @Override public void electPreferred(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline) throws Exception {
                    admin.electPreferred(partitions, deadline);
                }
                @Override public KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline deadline) throws Exception {
                    return admin.select(target, deadline);
                }
                @Override public long sessionTimeoutMillis(int brokerId, MonotonicDeadline deadline) throws Exception {
                    return admin.sessionTimeoutMillis(brokerId, deadline);
                }
                @Override public KafkaCommitWindow.Transaction transaction(String id, MonotonicDeadline deadline) throws Exception {
                    return admin.transaction(id, deadline);
                }
                @Override public KafkaBrokerFault.Driver broker(String name) { return driver(name, admin); }
            });
        }
    }
    @Override public KafkaLogCapture captureKafkaLogs(List<KafkaLogCapture.Partition> partitions, Path directory, MonotonicDeadline deadline) {
        if (!started) throw new IllegalStateException("Kafka owner unavailable");
        // One owned replica per partition; the launcher explicitly configures the capture log root.
        var broker = brokers.getFirst(); String id = broker.getContainerId(), networkId = network.getId();
        var binding = new OwnedKafkaArchiveCapture.Binding(id, networkId, target.brokerAlias(1), target.clusterAlias(), target.imageReference(), 1);
        return logCaptureCapability(SelectiveKafkaLogCapture.collect(binding,
                ApacheKafkaRuntime.archiveDriver(broker.getDockerClient(), id, networkId), partitions, directory, deadline));
    }

    static KafkaLogCapture logCaptureCapability(KafkaLogCapture capture) {
        if (!capture.archives().isEmpty() && capture.inventories().stream().noneMatch(inventory ->
                inventory.status().equals("IO_FAILURE") || inventory.status().equals("INVALID"))) return capture;
        var diagnostics = new ArrayList<>(capture.diagnostics());
        diagnostics.add("kafka.log-capture.unsupported: no complete physical-log capture from the generic launcher's declared "
                + KafkaRuntimeTarget.LOG_DIRECTORY + "; requires /usr/bin/find, /bin/stat and readable .log files; inspect retained diagnostics/transcripts");
        return new KafkaLogCapture(capture.inventories(), capture.archives(), capture.retainedBytes(),
                capture.reservedCalls(), diagnostics);
    }
}
