package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.*;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import org.testcontainers.containers.GenericContainer;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Physical actions are confined to one exact owned container and stable host endpoint. */
final class DockerKafkaBrokerDriver implements KafkaBrokerFault.Driver {
    private final GenericContainer<?> broker; private final KafkaRuntimeTarget target; private final int node, port;
    private final String id, networkId; private final BooleanSupplier started; private final KafkaBrokerAdmin admin;
    private final Map<String, KafkaBrokerControl.Snapshot> identities;
    private final String name;
    private volatile List<String> confirmedPortConfiguration;
    DockerKafkaBrokerDriver(GenericContainer<?> broker, KafkaRuntimeTarget target, int node, String networkId,
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
                boolean paused = Boolean.TRUE.equals(response.getState().getPaused());
                List<String> liveConfiguration = null;
                if (running) {
                    var bindings = bindings(response.getNetworkSettings().getPorts());
                    var configured = bindings(response.getHostConfig() == null
                            ? null : response.getHostConfig().getPortBindings());
                    if (bindings == null || bindings.length == 0) {
                        // Docker may omit runtime ports while paused. Configuration is a continuity
                        // check only; an unpaused observation must establish the live mapping first.
                        if (!paused || confirmedPortConfiguration == null || !matchesPort(configured)
                                || !confirmedPortConfiguration.equals(describe(configured)))
                            throw new IllegalStateException("Broker published port observation missing; expected 9092/tcp->"
                                    + port + ", runtime=" + describe(bindings) + ", configured=" + describe(configured)
                                    + ", paused=" + paused + "; advertised endpoint unconfirmed");
                    } else if (!matchesPort(bindings)) {
                        throw new IllegalStateException("Broker published port mismatch; expected 9092/tcp->"
                                + port + ", observed=" + describe(bindings) + "; advertised endpoint unconfirmed");
                    } else if (!paused && matchesPort(configured)) {
                        liveConfiguration = describe(configured);
                    }
                }
                requireOwned(deadline);
                var snapshot = new KafkaBrokerControl.Snapshot(id, response.getImageId(), networkId, node, running, paused);
                var original = identities.putIfAbsent(name, snapshot);
                if (original != null && (!original.containerId().equals(id) || !original.imageId().equals(snapshot.imageId())
                        || !original.networkId().equals(networkId))) throw new IllegalStateException("Broker identity changed");
                if (!paused) confirmedPortConfiguration = liveConfiguration;
                return snapshot;
            } catch (RuntimeException failure) {
                confirmedPortConfiguration = null;
                throw failure;
            }
        });
    }
    @Override public void mutate(boolean restart, MonotonicDeadline deadline) {
        mutate(restart ? KafkaBrokerControl.Action.RESTART : KafkaBrokerControl.Action.KILL, deadline);
    }
    @Override public void mutate(KafkaBrokerControl.Action action, MonotonicDeadline deadline) {
        ContainerDriverCallBoundary.run(ContainerOperationDeadline.shared("broker operation", deadline), action + " " + name, () -> {
            requireOwned(deadline);
            if (action == KafkaBrokerControl.Action.KILL || action == KafkaBrokerControl.Action.STOP
                    || action == KafkaBrokerControl.Action.RESTART) confirmedPortConfiguration = null;
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
    private static Ports.Binding[] bindings(Ports ports) {
        return ports == null || ports.getBindings() == null ? null : ports.getBindings().get(ExposedPort.tcp(9092));
    }
    private boolean matchesPort(Ports.Binding[] bindings) {
        return bindings != null && bindings.length > 0 && Arrays.stream(bindings)
                .allMatch(binding -> binding != null && Integer.toString(port).equals(binding.getHostPortSpec()));
    }
    private static List<String> describe(Ports.Binding[] bindings) {
        if (bindings == null) return List.of("omitted");
        return Arrays.stream(bindings).map(binding -> binding == null ? "null"
                : String.valueOf(binding.getHostIp()) + ":" + String.valueOf(binding.getHostPortSpec())).sorted().toList();
    }
    @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> requested, MonotonicDeadline deadline) throws Exception {
        requireOwned(deadline); return admin.leaders(requested, deadline);
    }
    private void requireOwned(MonotonicDeadline deadline) {
        if (!started.getAsBoolean() || !id.equals(broker.getContainerId()) || deadline.remaining().isZero()
                || Thread.currentThread().isInterrupted()) throw new IllegalStateException("Broker owner/deadline unavailable");
    }
}
