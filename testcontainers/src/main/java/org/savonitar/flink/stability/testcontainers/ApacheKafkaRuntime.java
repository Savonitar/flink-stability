package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import com.github.dockerjava.api.DockerClient;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Official Apache Kafka 4.0 runtime used by the first executable v1 path. */
final class ApacheKafkaRuntime implements KafkaRuntimeCluster {

    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

    private final KafkaRuntimeTarget target;
    private final Map<String, String> configuredEnvironment;
    private final KafkaContainer container;
    private final Supplier<String> ownedNetworkId;
    private final BiFunction<String, String, OwnedKafkaArchiveCapture.Driver> archiveDriverFactory;
    private boolean startAttempted;
    private boolean started;
    private long generation;
    private OwnedKafkaArchiveCapture.Identity startupIdentity;

    ApacheKafkaRuntime(Network network, KafkaRuntimeTarget target) {
        Objects.requireNonNull(network, "network");
        this.target = Objects.requireNonNull(target, "target");
        Map<String, String> environment = new LinkedHashMap<>(
                brokerEnvironment(target.brokerPolicy()));
        environment.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        this.configuredEnvironment = Collections.unmodifiableMap(environment);
        this.container = new KafkaContainer(DockerImageName.parse(target.imageReference()))
                .withNetwork(network)
                .withNetworkAliases(target.networkAlias())
                .withListener(target.internalBootstrapServers())
                .withEnv(configuredEnvironment)
                .withStartupTimeout(STARTUP_TIMEOUT)
                .withLogConsumer(new Slf4jLogConsumer(
                        LoggerFactory.getLogger("KAFKA_V1_CONTAINER_LOGS")));
        this.ownedNetworkId = network::getId;
        this.archiveDriverFactory = (id, networkId) -> archiveDriver(
                container.getDockerClient(), id, networkId);
    }

    /** Lifecycle/archive seam for no-Docker tests; normal construction above is unchanged. */
    ApacheKafkaRuntime(KafkaRuntimeTarget target, KafkaContainer container,
                       Supplier<String> ownedNetworkId,
                       BiFunction<String, String, OwnedKafkaArchiveCapture.Driver> archiveDriverFactory) {
        this.target = Objects.requireNonNull(target);
        this.container = Objects.requireNonNull(container);
        this.ownedNetworkId = Objects.requireNonNull(ownedNetworkId);
        this.archiveDriverFactory = Objects.requireNonNull(archiveDriverFactory);
        var environment = new LinkedHashMap<>(brokerEnvironment(target.brokerPolicy()));
        environment.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        this.configuredEnvironment = Collections.unmodifiableMap(environment);
    }

    @Override
    public synchronized void start() {
        if (started) {
            throw new IllegalStateException("Kafka is already running");
        }
        startAttempted = true;
        container.start();
        started = true;
        generation++;
        startupIdentity = null;
    }

    @Override
    public synchronized void stop() {
        if (!startAttempted) {
            return;
        }
        container.stop();
        started = false;
        startAttempted = false;
        startupIdentity = null;
    }

    /** Opt-in startup observation; ordinary start/endpoints/stop add no archive/inspect calls. */
    OwnedKafkaArchiveCapture.Identity observeIdentity(MonotonicDeadline deadline) {
        var binding = ownedBinding();
        OwnedKafkaArchiveCapture.Identity observed = OwnedKafkaArchiveCapture.observe(
                binding, archiveDriverFactory.apply(binding.containerId(), binding.networkId()), deadline);
        synchronized (this) {
            if (!started || generation != binding.generation()
                    || !Objects.equals(container.getContainerId(), binding.containerId())) {
                throw new IllegalStateException("Kafka owner changed during startup observation");
            }
            if (startupIdentity != null) throw new IllegalStateException("Startup identity already observed");
            startupIdentity = observed;
            return observed;
        }
    }

    OwnedKafkaArchiveCapture.Receipt copyOutputPartition(OwnedKafkaArchiveCapture.Identity expected,
            OwnedKafkaArchiveCapture.Partition partition, Path evidenceDirectory, String partialName,
            long maximumBytes, MonotonicDeadline deadline) {
        final OwnedKafkaArchiveCapture.Binding binding;
        synchronized (this) {
            if (startupIdentity == null || startupIdentity != expected) {
                throw new IllegalArgumentException("Exact observed startup identity required");
            }
            binding = ownedBinding();
        }
        // No owner monitor/driver lock is retained here. Cleanup can remove this captured ID
        // while its archive stream is blocked; the capture then remains explicitly partial.
        return OwnedKafkaArchiveCapture.copy(binding,
                archiveDriverFactory.apply(binding.containerId(), binding.networkId()), expected,
                partition, evidenceDirectory, partialName, maximumBytes, deadline);
    }

    private synchronized OwnedKafkaArchiveCapture.Binding ownedBinding() {
        if (!started) throw new IllegalStateException("Kafka is not started");
        String id = container.getContainerId();
        String networkId = ownedNetworkId.get();
        return new OwnedKafkaArchiveCapture.Binding(id, networkId, target.networkAlias(),
                target.clusterAlias(), target.imageReference(), generation);
    }

    KafkaLogInventory listPartitionFiles(OwnedKafkaArchiveCapture.Identity expected,
            OwnedKafkaArchiveCapture.Partition partition, int maximumBytes, MonotonicDeadline deadline) {
        var binding = expectedOwner(expected);
        return KafkaLogInventory.capture(binding,
                archiveDriverFactory.apply(binding.containerId(), binding.networkId()),
                expected, partition, maximumBytes, deadline);
    }

    OwnedKafkaArchiveCapture.Receipt copyLogFile(OwnedKafkaArchiveCapture.Identity expected,
            KafkaLogInventory inventory, String basename, Path evidenceDirectory, String partialName,
            long maximumBytes, MonotonicDeadline deadline) {
        var binding = expectedOwner(expected);
        return OwnedKafkaArchiveCapture.copyLog(binding,
                archiveDriverFactory.apply(binding.containerId(), binding.networkId()), expected,
                inventory, basename, evidenceDirectory, partialName, maximumBytes, deadline);
    }

    private synchronized OwnedKafkaArchiveCapture.Binding expectedOwner(OwnedKafkaArchiveCapture.Identity expected) {
        if (startupIdentity == null || startupIdentity != expected) {
            throw new IllegalArgumentException("Exact observed startup identity required");
        }
        return ownedBinding();
    }

    static OwnedKafkaArchiveCapture.Driver archiveDriver(
            DockerClient client, String capturedId, String ownedNetworkId) {
        return new OwnedKafkaArchiveCapture.Driver() {
            @Override public OwnedKafkaArchiveCapture.Inspection inspect() {
                try (var command = client.inspectContainerCmd(capturedId)) {
                    var response = command.exec();
                    var networks = response.getNetworkSettings() == null ? null
                            : response.getNetworkSettings().getNetworks();
                    var matches = networks == null ? List.<com.github.dockerjava.api.model.ContainerNetwork>of()
                            : networks.values().stream()
                                .filter(network -> network != null && ownedNetworkId.equals(network.getNetworkID())).toList();
                    var network = matches.size() == 1 ? matches.getFirst() : null;
                    return new OwnedKafkaArchiveCapture.Inspection(response.getId(), response.getImageId(),
                            network == null ? null : network.getNetworkID(),
                            network == null || network.getAliases() == null ? List.of() : network.getAliases(),
                            response.getState() != null && Boolean.TRUE.equals(response.getState().getRunning()));
                }
            }

            @Override public InputStream openArchive(String directory) {
                var command = client.copyArchiveFromContainerCmd(capturedId, directory);
                try {
                    InputStream stream = command.exec();
                    return new FilterInputStream(stream) {
                        @Override public void close() throws IOException {
                            try { super.close(); } finally { command.close(); }
                        }
                    };
                } catch (RuntimeException | Error failure) {
                    try { command.close(); }
                    catch (RuntimeException closeFailure) { failure.addSuppressed(closeFailure); }
                    throw failure;
                }
            }

            @Override public KafkaLogInventory.Command listFileNames(String directory, int maximumBytes,
                    ContainerOperationDeadline deadline, Runnable checkActive) throws IOException {
                return KafkaInventoryCommand.run(client, capturedId, maximumBytes, deadline, checkActive,
                        "/usr/bin/find", directory, "-maxdepth", "1", "-print0");
            }

            @Override public KafkaLogInventory.Command fileMetadata(String path, int maximumBytes,
                    ContainerOperationDeadline deadline, Runnable checkActive) throws IOException {
                return KafkaInventoryCommand.run(client, capturedId, maximumBytes, deadline, checkActive,
                        "/bin/stat", "-c", "%f %s %Y %i", path);
            }
        };
    }

    @Override
    public synchronized KafkaRuntimeEndpoints endpoints() {
        requireStarted();
        return new KafkaRuntimeEndpoints(
                target.clusterAlias(),
                target.imageReference(),
                target.internalBootstrapServers(),
                container.getBootstrapServers());
    }

    String configuredImageReference() {
        return target.imageReference();
    }

    String configuredNetworkAlias() {
        return target.networkAlias();
    }

    String configuredInternalListener() {
        return target.internalBootstrapServers();
    }

    Map<String, String> configuredEnvironment() {
        return configuredEnvironment;
    }

    private void requireStarted() {
        if (!started || !container.isRunning()) {
            throw new IllegalStateException("Kafka is not running");
        }
    }

    private static Map<String, String> brokerEnvironment(KafkaBrokerPolicy policy) {
        Map<String, String> configuration = policy.kafkaConfiguration();
        return Map.of(
                "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                configuration.get("transaction.max.timeout.ms"),
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR",
                configuration.get("offsets.topic.replication.factor"),
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                configuration.get("transaction.state.log.replication.factor"),
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
                configuration.get("transaction.state.log.min.isr"),
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS",
                configuration.get("group.initial.rebalance.delay.ms"));
    }
    public KafkaLogCapture captureKafkaLogs(
            List<KafkaLogCapture.Partition> partitions,
            Path directory, MonotonicDeadline deadline) {
        var binding = ownedBinding();
        return SelectiveKafkaLogCapture.collect(binding,
                archiveDriverFactory.apply(binding.containerId(), binding.networkId()), partitions, directory, deadline);
    }
}
