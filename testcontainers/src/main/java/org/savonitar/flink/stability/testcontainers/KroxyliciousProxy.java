package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaProxyEndpoint;
import org.savonitar.flink.stability.runtime.api.KafkaProxyTarget;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Objects;

/**
 * A pinned Kroxylicious proxy running the flink-stability fault filter (SPEC-004). The filter JAR
 * is embedded in this module; rules and evidence pass through a bind-mounted control directory.
 */
final class KroxyliciousProxy {
    /** Kroxylicious 0.21.0, which bundles kafka-clients 4.2.0; pinned by digest. */
    static final String IMAGE = "quay.io/kroxylicious/kroxylicious:0.21.0"
            + "@sha256:6bb6612d7f223eeee226fe656bbc5ebd9e41f03fec78e22df975ff810ea71c42";
    static final String FILTER_RESOURCE = "kroxylicious/flink-stability-fault-filter.jar";
    static final String CONTROL_PATH = "/flink-stability/proxy";
    private static final String FILTER_PATH = "/opt/flink-stability/fault-filter.jar";
    private static final String CONFIGURATION_PATH = "/opt/flink-stability/proxy.yaml";
    /** Broker node IDs the gateway maps to the ports after the bootstrap port. */
    private static final int HIGHEST_NODE_ID = 9;
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

    private final KafkaProxyTarget target;
    private final Path controlDirectory;
    private final GenericContainer<?> container;

    KroxyliciousProxy(Network network, KafkaProxyTarget target, Path controlDirectory) {
        Objects.requireNonNull(network, "network");
        this.target = Objects.requireNonNull(target, "target");
        this.controlDirectory = prepareControlDirectory(controlDirectory);
        this.container = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withNetwork(network)
                .withNetworkAliases(target.listenHost())
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource(FILTER_RESOURCE), FILTER_PATH)
                .withCopyToContainer(
                        Transferable.of(configuration(target)), CONFIGURATION_PATH)
                .withFileSystemBind(
                        this.controlDirectory.toString(), CONTROL_PATH, BindMode.READ_WRITE)
                .withEnv("KROXYLICIOUS_CLASSPATH", FILTER_PATH)
                .withCommand("--config", CONFIGURATION_PATH)
                .waitingFor(Wait.forLogMessage(".*Kroxylicious is started.*", 1)
                        .withStartupTimeout(STARTUP_TIMEOUT))
                .withLogConsumer(new Slf4jLogConsumer(
                        LoggerFactory.getLogger("KROXYLICIOUS_CONTAINER_LOGS")));
    }

    KafkaProxyEndpoint start() {
        container.start();
        return new KafkaProxyEndpoint(
                target.proxyAlias(), target.bootstrapServers(), IMAGE, controlDirectory);
    }

    void stop() {
        container.stop();
    }

    /** One virtual cluster whose every connection passes through the fault filter. */
    static String configuration(KafkaProxyTarget target) {
        return """
                virtualClusters:
                  - name: %s
                    targetCluster:
                      bootstrapServers: %s
                    gateways:
                      - name: default
                        portIdentifiesNode:
                          bootstrapAddress: %s
                          nodeStartPort: %d
                          nodeIdRanges:
                            - name: brokers
                              start: 0
                              end: %d
                filterDefinitions:
                  - name: flink-stability-faults
                    type: org.savonitar.flink.stability.faultproxy.FaultInjection
                    config:
                      controlDirectory: %s
                defaultFilters:
                  - flink-stability-faults
                """.formatted(
                target.proxyAlias(),
                target.upstreamBootstrapServers(),
                target.bootstrapServers(),
                target.listenPort() + 1,
                HIGHEST_NODE_ID,
                CONTROL_PATH);
    }

    /** The proxy runs as its own UID, so its evidence directory must be writable by anyone. */
    private static Path prepareControlDirectory(Path directory) {
        Path absolute = Objects.requireNonNull(directory, "controlDirectory")
                .toAbsolutePath()
                .normalize();
        try {
            for (Path created : new Path[] {
                    absolute, absolute.resolve("rules"), absolute.resolve("events")}) {
                Files.createDirectories(created);
                try {
                    Files.setPosixFilePermissions(
                            created, PosixFilePermissions.fromString("rwxrwxrwx"));
                } catch (UnsupportedOperationException ignored) {
                    // Docker Desktop and other non-POSIX filesystems mediate bind-mount access.
                }
            }
            return absolute.toRealPath();
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "Could not prepare the Kafka proxy control directory " + absolute, failure);
        }
    }
}
