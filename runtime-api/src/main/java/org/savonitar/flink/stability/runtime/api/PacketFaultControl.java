package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Scoped packet faults; counter and cleanup evidence is required independently of the data oracle. */
public interface PacketFaultControl {
    String IMAGE = "docker.io/nicolaka/netshoot@sha256:7f08c4aff13ff61a35d30e30c5c1ea8396eac6ab4ce19fd02d5a4b3b5d0d09a2";
    String PLATFORM = "linux/arm64";
    String IMAGE_ID = "sha256:3be5270ddd56ae26af7c6ea7ac66c393dbe1d8cb8bceb46ec0b1f4a55c1574ad";
    enum Action { LOSS, DELAY, BLACKHOLE }
    record Request(String taskManager, KafkaBrokerControl.Target broker, String image, Action action,
                   int lossPercent, int delayMillis, int jitterMillis, Duration duration, Duration timeout,
                   List<KafkaBrokerControl.Target> additionalBrokers) {
        public Request(String taskManager, KafkaBrokerControl.Target broker, String image, Action action,
                       int lossPercent, int delayMillis, int jitterMillis, Duration duration, Duration timeout) {
            this(taskManager, broker, image, action, lossPercent, delayMillis, jitterMillis, duration, timeout, List.of());
        }
        public Request {
            Checks.taskManagerOrdinal(taskManager); Objects.requireNonNull(broker); Objects.requireNonNull(action);
            additionalBrokers = List.copyOf(additionalBrokers);
            if (!additionalBrokers.isEmpty()) {
                var targets = new java.util.ArrayList<>(additionalBrokers); targets.add(broker);
                if (action != Action.DELAY || targets.size() != 3
                        || targets.stream().anyMatch(t -> t.kind() != KafkaBrokerControl.TargetKind.NAMED)
                        || !targets.stream().map(KafkaBrokerControl.Target::name).collect(java.util.stream.Collectors.toSet())
                            .equals(java.util.Set.of("broker-1", "broker-2", "broker-3")))
                    throw new IllegalArgumentException("Multi-broker packet faults require delay to exactly the three owned brokers");
            }
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
                    boolean healed, boolean sidecarRemoved, List<Receipt> receipts, String error,
                    List<Binding> additionalBindings) {
        public Evidence { receipts = List.copyOf(receipts); additionalBindings = List.copyOf(additionalBindings); }
        private boolean allBindingsMatch() {
            if (additionalBindings.size() != request.additionalBrokers().size()) return false;
            var ids = new java.util.HashSet<String>(); ids.add(binding.brokerId());
            var addresses = new java.util.HashSet<String>(); addresses.add(binding.brokerIpv4());
            for (int i = 0; i < additionalBindings.size(); i++) {
                var other = additionalBindings.get(i);
                if (!other.selection().requested().equals(request.additionalBrokers().get(i))
                        || !other.taskManagerId().equals(binding.taskManagerId()) || !other.networkId().equals(binding.networkId())
                        || !ids.add(other.brokerId()) || !addresses.add(other.brokerIpv4())) return false;
            }
            return true;
        }
        public boolean confirmed() {
            return error == null && binding != null && sidecarId != null && sidecarImageId != null && device != null
                    && binding.selection().requested().equals(request.broker()) && allBindingsMatch() && healed && sidecarRemoved
                    && heldNanos >= request.duration().toNanos() && before != null && after != null
                    && after.packets() >= before.packets() && after.dropped() >= before.dropped()
                    && (request.action() == Action.DELAY ? after.packets() > before.packets() : after.dropped() > before.dropped());
        }
    }
    static Evidence unconfirmed(Request request, String error) {
        return new Evidence(request, null, null, null, null, 0, 0, 0, null, null, false, false, List.of(), error, List.of());
    }
    default Evidence packetFault(Request request) { throw new UnsupportedOperationException("Packet fault backend unavailable"); }
}
