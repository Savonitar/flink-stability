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

/** Explicit /opt/kafka distribution layout; no vendor entrypoint is inferred or invoked. */
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
        withCommand(new String[] {KafkaRuntimeLaunch.COMMAND.getLast()});
        waitingFor(Wait.forSuccessfulCommand(KafkaRuntimeLaunch.READINESS_COMMAND)
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
            verifyCreated(containerId, inspect.exec().getImageId());
        }
    }

    String bootstrapServers() { return getHost() + ":" + getMappedPort(9092); }

    void verifyCreated(String containerId, String actualImageId) {
        retainCreated(containerId, actualImageId);
        KafkaImageIdentity.verify(target, actualImageId);
        receipt = new KafkaRuntimeEvidence.Container(target.brokerAlias(node), containerId, actualImageId,
                launch.environment(), launch.command(), launch.logDirectory(), true, false);
    }

    private void retainCreated(String containerId, String actualImageId) {
        if (launch == null) throw new IllegalStateException("Created Kafka container has no resolved launch configuration");
        receipt = new KafkaRuntimeEvidence.Container(target.brokerAlias(node), containerId, actualImageId,
                launch.environment(), launch.command(), launch.logDirectory(), false, false);
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
