package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Experimental packet backend; canonical activation requires a reviewed image pin and live probe. */
public interface PacketFaultControl {
    enum Action { LOSS, DELAY, BLACKHOLE }
    record Request(String taskManager, KafkaBrokerControl.Target broker, String image, Action action,
                   int lossPercent, int delayMillis, int jitterMillis, Duration duration, Duration timeout) {
        public Request {
            Checks.taskManagerOrdinal(taskManager); Objects.requireNonNull(broker); Objects.requireNonNull(action);
            if (image == null || !image.matches("docker\\.io/nicolaka/netshoot@sha256:[0-9a-f]{64}"))
                throw new IllegalArgumentException("Packet sidecar requires an explicit immutable public netshoot digest");
            if (action == Action.LOSS ? lossPercent < 1 || lossPercent > 100 : lossPercent != 0)
                throw new IllegalArgumentException("Loss must be an integer 1..100 percent, only for LOSS");
            if (action == Action.DELAY ? delayMillis < 1 || delayMillis > 5000 || jitterMillis < 0 || jitterMillis > delayMillis
                    : delayMillis != 0 || jitterMillis != 0) throw new IllegalArgumentException("Invalid delay/jitter");
            if (duration == null || duration.isZero() || duration.isNegative() || duration.compareTo(Duration.ofMinutes(2)) > 0
                    || timeout == null || timeout.compareTo(duration) <= 0 || timeout.compareTo(Duration.ofMinutes(3)) > 0)
                throw new IllegalArgumentException("Packet hold must be in (0,2m], with a larger timeout no greater than 3m");
        }
    }
    record Binding(String taskManagerId, String taskManagerImageId, String brokerId, String brokerImageId,
                   String networkId, String brokerIpv4, int brokerPort, KafkaBrokerControl.Selection selection) {
        public Binding {
            for (String id : List.of(taskManagerId, brokerId)) if (!id.matches("[0-9a-f]{12,64}"))
                throw new IllegalArgumentException("Exact Docker container IDs required");
            Checks.requireNonBlank(taskManagerImageId, "taskManagerImageId"); Checks.requireNonBlank(brokerImageId, "brokerImageId");
            Checks.requireNonBlank(networkId, "networkId"); Objects.requireNonNull(selection);
            String[] octets = brokerIpv4.split("\\.", -1);
            if (octets.length != 4) throw new IllegalArgumentException("IPv4 broker address required");
            for (String octet : octets) if (!octet.matches("0|[1-9][0-9]{0,2}") || Integer.parseInt(octet) > 255)
                throw new IllegalArgumentException("Invalid broker IPv4 address");
            int first = Integer.parseInt(octets[0]);
            if (first == 0 || first == 127 || first >= 224 || brokerPort != 19092)
                throw new IllegalArgumentException("Packet faults target the owned internal Kafka IPv4 client listener");
        }
    }
    record Counters(long packets, long dropped) {
        public Counters { if (packets < 0 || dropped < 0) throw new IllegalArgumentException("Negative counters"); }
    }
    record Receipt(long atMillis, List<String> command, int exitCode, String stdout, String stderr) {
        public Receipt { command = List.copyOf(command); }
    }
    record Evidence(Request request, Binding binding, String sidecarId, String sidecarImageId, String device,
                    long startedAtMillis, long completedAtMillis, long heldNanos, Counters before, Counters after,
                    boolean healed, boolean sidecarRemoved, List<Receipt> receipts, String error) {
        public Evidence { receipts = List.copyOf(receipts); }
        public boolean confirmed() {
            return error == null && binding != null && sidecarId != null && sidecarImageId != null && device != null
                    && binding.selection().requested().equals(request.broker()) && healed && sidecarRemoved
                    && heldNanos >= request.duration().toNanos() && before != null && after != null
                    && after.packets() >= before.packets() && after.dropped() >= before.dropped()
                    && (request.action() == Action.DELAY ? after.packets() > before.packets() : after.dropped() > before.dropped());
        }
    }
    default Evidence packetFault(Request request) { throw new UnsupportedOperationException("Packet fault backend unavailable"); }
}
