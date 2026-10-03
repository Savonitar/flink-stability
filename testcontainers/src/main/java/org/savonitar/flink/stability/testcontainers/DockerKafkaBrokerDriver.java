package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.kafka.KafkaContainer;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Physical actions are confined to one exact owned container and stable host endpoint. */
final class DockerKafkaBrokerDriver implements KafkaBrokerFault.Driver {
    private final KafkaContainer broker; private final KafkaRuntimeTarget target; private final int node, port;
    private final String id, networkId; private final BooleanSupplier started; private final KafkaBrokerAdmin admin;
    private final Map<String, KafkaBrokerControl.Snapshot> identities;
    private final String name;
    DockerKafkaBrokerDriver(KafkaContainer broker, KafkaRuntimeTarget target, int node, String networkId,
            BooleanSupplier started, KafkaBrokerAdmin admin, Map<String, KafkaBrokerControl.Snapshot> identities) {
        this.broker = broker; this.target = target; this.node = node; this.networkId = networkId;
        this.started = started; this.admin = admin; this.identities = identities; name = "broker-" + node;
        id = broker.getContainerId(); port = broker.getMappedPort(9092);
    }
    @Override public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline deadline) {
        return ContainerDriverCallBoundary.call(ContainerOperationDeadline.shared("broker inspection", deadline), "inspect " + name, () -> {
            requireOwned(deadline);
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
                var snapshot = new KafkaBrokerControl.Snapshot(id, response.getImageId(), networkId, node, running,
                        Boolean.TRUE.equals(response.getState().getPaused()));
                var original = identities.putIfAbsent(name, snapshot);
                if (original != null && (!original.containerId().equals(id) || !original.imageId().equals(snapshot.imageId())
                        || !original.networkId().equals(networkId))) throw new IllegalStateException("Broker identity changed");
                return snapshot;
            }
        });
    }
    @Override public void mutate(boolean restart, MonotonicDeadline deadline) {
        mutate(restart ? KafkaBrokerControl.Action.RESTART : KafkaBrokerControl.Action.KILL, deadline);
    }
    @Override public void mutate(KafkaBrokerControl.Action action, MonotonicDeadline deadline) {
        ContainerDriverCallBoundary.run(ContainerOperationDeadline.shared("broker operation", deadline), action + " " + name, () -> {
            requireOwned(deadline);
            switch (action) {
                case KILL -> { try (var command = broker.getDockerClient().killContainerCmd(id).withSignal("KILL")) { command.exec(); } }
                // TERM only: never escalate graceful rolling shutdown to KILL on timeout.
                case STOP -> { try (var command = broker.getDockerClient().killContainerCmd(id).withSignal("TERM")) { command.exec(); } }
                case ROLLING_RESTART -> throw new IllegalArgumentException("Rolling restart must use the shared broker fault engine");
                case RESTART -> { try (var command = broker.getDockerClient().startContainerCmd(id)) { command.exec(); } }
                case PAUSE -> { try (var command = broker.getDockerClient().pauseContainerCmd(id)) { command.exec(); } }
                case RESUME -> { try (var command = broker.getDockerClient().unpauseContainerCmd(id)) { command.exec(); } }
            }
        });
    }
    @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> requested, MonotonicDeadline deadline) throws Exception {
        requireOwned(deadline); return admin.leaders(requested, deadline);
    }
    private void requireOwned(MonotonicDeadline deadline) {
        if (!started.getAsBoolean() || !id.equals(broker.getContainerId()) || deadline.remaining().isZero()
                || Thread.currentThread().isInterrupted()) throw new IllegalStateException("Broker owner/deadline unavailable");
    }
}
