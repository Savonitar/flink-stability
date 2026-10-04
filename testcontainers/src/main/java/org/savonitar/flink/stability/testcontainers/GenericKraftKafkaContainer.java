package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEvidence;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeLaunch;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

/** Explicit tool layout; no vendor entrypoint is inferred or invoked. */
final class GenericKraftKafkaContainer extends GenericContainer<GenericKraftKafkaContainer> {
    private final KafkaRuntimeTarget target;
    private final int node;
    private final int hostPort;
    private KafkaRuntimeLaunch launch;
    private volatile KafkaRuntimeEvidence.Container receipt;

    GenericKraftKafkaContainer(Network network, KafkaRuntimeTarget target, int node, int hostPort) {
        super(DockerImageName.parse(target.imageReference()));
        this.target = target;
        this.node = node;
        this.hostPort = hostPort;
        withNetwork(network).withNetworkAliases(target.brokerAlias(node));
        withExposedPorts(9092);
        setPortBindings(List.of(hostPort + ":9092"));
        withCreateContainerCmdModifier(command -> command.withHostName(target.brokerAlias(node))
                .withEntrypoint("/bin/sh", "-ec"));
        withCommand(new String[] {KafkaRuntimeLaunch.command(target.layout()).getLast()});
        waitingFor(Wait.forSuccessfulCommand(KafkaRuntimeLaunch.readinessCommand(target.layout()))
                .withStartupTimeout(Duration.ofMinutes(2)));
        withStartupTimeout(Duration.ofMinutes(2));
    }

    @Override protected void configure() {
        super.configure();
        configureLaunch(getHost());
    }

    void configureLaunch(String host) {
        launch = KafkaRuntimeLaunch.genericKraft(target, node, host, Integer.toString(hostPort));
        withEnv(launch.environment());
    }

    @Override protected void containerIsCreated(String containerId) {
        super.containerIsCreated(containerId);
        retainCreated(containerId, null);
        try (var inspect = getDockerClient().inspectContainerCmd(containerId)) {
            var observed = inspect.exec();
            var config = observed.getConfig();
            verifyCreated(containerId, observed.getImageId(), java.util.Arrays.asList(config.getEnv()),
                    java.util.Arrays.asList(config.getEntrypoint()), java.util.Arrays.asList(config.getCmd()));
        }
    }

    String bootstrapServers() { return getHost() + ":" + getMappedPort(9092); }

    void verifyCreated(String containerId, String actualImageId, List<String> environment,
                       List<String> entrypoint, List<String> command) {
        retainCreated(containerId, actualImageId);
        KafkaImageIdentity.verify(target, actualImageId);
        // Only inspected inputs become a verified receipt; inherited unrelated image env is not claimed.
        for (var entry : launch.environment().entrySet()) {
            var actual = environment.stream().filter(value -> value.startsWith(entry.getKey() + "=")).toList();
            if (!actual.equals(List.of(entry.getKey() + "=" + entry.getValue())))
                throw new IllegalStateException("Kafka created-container environment differs at " + entry.getKey());
        }
        if (!entrypoint.equals(launch.command().subList(0, 2)) || !command.equals(launch.command().subList(2, 3)))
            throw new IllegalStateException("Kafka created-container command differs from the resolved layout");
        receipt = new KafkaRuntimeEvidence.Container(target.brokerAlias(node), containerId, actualImageId,
                launch.environment(), launch.command(), launch.logDirectory(), true, false, null, launch.readinessCommand());
    }

    private void retainCreated(String containerId, String actualImageId) {
        if (launch == null) throw new IllegalStateException("Created Kafka container has no resolved launch configuration");
        receipt = new KafkaRuntimeEvidence.Container(target.brokerAlias(node), containerId, actualImageId,
                launch.environment(), launch.command(), launch.logDirectory(), false, false, null, launch.readinessCommand());
    }

    void markReady() { receipt = evidence().started(); }

    java.util.Optional<KafkaRuntimeEvidence.Container> retainedEvidence() {
        return java.util.Optional.ofNullable(receipt);
    }

    KafkaRuntimeEvidence.Container evidence() {
        if (receipt == null) throw new IllegalStateException("Kafka created-container identity was not observed");
        return receipt;
    }
}
