package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Capability;
import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;

/** Sidecar joins only the exact owned TaskManager namespace; no host mounts or privileged mode. */
final class DockerPacketFaultSidecar implements PacketFaultBackend.Driver {
    private final DockerClient docker;
    private final TaskManagerControl.Identity taskManager;
    private final String network;
    private final KafkaBrokerAdmin admin;
    private final List<KafkaContainer> brokers;
    private GenericContainer<?> sidecar;
    private String imageId;
    DockerPacketFaultSidecar(DockerClient docker, TaskManagerControl.Identity taskManager, String network,
                            KafkaBrokerAdmin admin, List<KafkaContainer> brokers) {
        this.docker=docker; this.taskManager=taskManager; this.network=network; this.admin=admin; this.brokers=List.copyOf(brokers);
    }
    @Override public PacketFaultControl.Binding resolve(KafkaBrokerControl.Target target, MonotonicDeadline deadline) throws Exception {
        var selected = admin.select(target, deadline);
        if (selected.leader() < 1 || selected.leader() > brokers.size()) throw new IllegalStateException("Unowned selected broker");
        String brokerId = brokers.get(selected.leader()-1).getContainerId();
        var tm = inspect(taskManager.runtimeId(), deadline); var broker = inspect(brokerId, deadline);
        return new PacketFaultControl.Binding(tm.getId(), tm.getImageId(), broker.getId(), broker.getImageId(), network,
                address(broker), 19092, selected);
    }
    private InspectContainerResponse inspect(String id, MonotonicDeadline deadline) {
        return ContainerDriverCallBoundary.call(ContainerOperationDeadline.shared("packet identity", deadline), "inspect packet endpoint", () -> {
            try (var command = docker.inspectContainerCmd(id)) {
                var value = command.exec();
                if (!id.equals(value.getId()) || !Boolean.TRUE.equals(value.getState().getRunning())
                        || Boolean.TRUE.equals(value.getState().getPaused())
                        || value.getNetworkSettings().getNetworks().values().stream().noneMatch(n -> network.equals(n.getNetworkID())))
                    throw new IllegalStateException("Packet endpoint identity/state/network unavailable");
                return value;
            }
        });
    }
    private String address(InspectContainerResponse value) {
        return value.getNetworkSettings().getNetworks().values().stream().filter(n -> network.equals(n.getNetworkID()))
                .map(com.github.dockerjava.api.model.ContainerNetwork::getIpAddress).findFirst().orElseThrow();
    }
    @Override public void verify(PacketFaultControl.Binding binding, MonotonicDeadline deadline) {
        var tm = inspect(binding.taskManagerId(), deadline); var broker = inspect(binding.brokerId(), deadline);
        if (!tm.getImageId().equals(binding.taskManagerImageId()) || !broker.getImageId().equals(binding.brokerImageId())
                || !address(broker).equals(binding.brokerIpv4()) || !binding.networkId().equals(network)
                || !binding.taskManagerId().equals(taskManager.runtimeId())
                || !binding.brokerId().equals(brokers.get(binding.selection().leader()-1).getContainerId()))
            throw new IllegalStateException("Packet namespace or selected broker binding changed");
    }
    static GenericContainer<?> container(PacketFaultControl.Request request, PacketFaultControl.Binding binding, String chain) {
        return new GenericContainer<>(DockerImageName.parse(request.image()))
                .withNetworkMode("container:" + binding.taskManagerId())
                .withCreateContainerCmdModifier(DockerPacketFaultSidecar::configure)
                .withCommand(watchdog(request, binding, chain))
                .waitingFor(Wait.forLogMessage(".*packet-watchdog-ready.*\\n", 1));
    }
    static void configure(com.github.dockerjava.api.command.CreateContainerCmd command) {
        command.withEntrypoint("/bin/sh", "-c");
        command.getHostConfig().withCapDrop(Capability.ALL).withCapAdd(Capability.NET_ADMIN)
                .withReadonlyRootfs(true).withTmpFs(Map.of("/run", "rw,noexec,nosuid,size=1m"));
    }
    static String watchdog(PacketFaultControl.Request request, PacketFaultControl.Binding binding, String chain) {
        if (!chain.matches("FSCHAOS_[0-9a-f]{12}")) throw new IllegalArgumentException("Invalid owned chain");
        // Only a successful preflight writes the activation marker. A foreign qdisc is never healed.
        String cleanup = request.action() == PacketFaultControl.Action.BLACKHOLE
                ? String.join("; ", PacketFaultBackend.heal(request.action(), binding, "eth0", chain).stream()
                    .map(command -> String.join(" ", command) + " 2>/dev/null || true").toList())
                : "device=$(cat /run/flink-packet-owned); tc qdisc del dev \"$device\" root handle " + PacketFaultBackend.ROOT_HANDLE + " 2>/dev/null || true";
        long seconds = request.timeout().plusSeconds(30).toSeconds();
        return "cleanup() { if [ -f /run/flink-packet-owned ]; then " + cleanup + "; fi; }; "
                + "trap 'cleanup; exit 0' TERM INT; trap cleanup EXIT; "
                + "echo packet-watchdog-ready; sleep " + seconds + " & wait $!";
    }
    @Override public void open(PacketFaultControl.Request request, PacketFaultControl.Binding binding, String chain, MonotonicDeadline deadline) {
        sidecar = container(request, binding, chain).withStartupTimeout(deadline.remaining());
        ContainerDriverCallBoundary.run(ContainerOperationDeadline.shared("packet sidecar startup", deadline), "start packet sidecar", sidecar::start);
        ContainerDriverCallBoundary.run(ContainerOperationDeadline.shared("packet image identity", deadline), "verify packet image digest", () -> {
            imageId = sidecar.getContainerInfo().getImageId();
            try (var command = docker.inspectImageCmd(imageId)) {
                var image = command.exec();
                String expected = request.image().substring("docker.io/".length());
                if (image.getRepoDigests() == null || image.getRepoDigests().stream().noneMatch(digest -> digest.equals(expected) || digest.equals(request.image())))
                    throw new IllegalStateException("Sidecar content digest not confirmed");
            }
        });
    }
    @Override public String sidecarId() { return sidecar == null ? null : sidecar.getContainerId(); }
    @Override public String imageId() { return imageId; }
    @Override public PacketFaultControl.Receipt exec(List<String> command, MonotonicDeadline deadline) {
        return ContainerDriverCallBoundary.call(ContainerOperationDeadline.shared("packet command", deadline), "execute packet command", () -> {
            var result = sidecar.execInContainer(command.toArray(String[]::new));
            return new PacketFaultControl.Receipt(clockMillis(), command, Math.toIntExact(result.getExitCode()), result.getStdout(), result.getStderr());
        });
    }
    @Override public void close(MonotonicDeadline deadline) {
        if (sidecar == null) return;
        String id = sidecar.getContainerId();
        if (id == null) throw new IllegalStateException("Ambiguous startup; cannot confirm helper removal");
        ContainerDriverCallBoundary.run(ContainerOperationDeadline.shared("packet sidecar cleanup", deadline), "remove healed packet sidecar", () -> {
            sidecar.stop();
            try (var command = docker.inspectContainerCmd(id)) {
                command.exec(); throw new IllegalStateException("Healed sidecar still exists");
            } catch (NotFoundException removed) { /* Exact helper no longer exists. */ }
        });
    }
}
