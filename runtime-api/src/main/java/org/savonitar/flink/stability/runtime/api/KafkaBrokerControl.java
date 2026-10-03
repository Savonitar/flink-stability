package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;

/** Named owned-broker operations with physical process and partition leadership observations. */
public interface KafkaBrokerControl {
    default Evidence brokerOperation(String name, boolean restart, List<KafkaLogCapture.Partition> partitions, Duration timeout) {
        throw new UnsupportedOperationException("Named Kafka broker operations unavailable");
    }

    enum Action { KILL, RESTART, PAUSE, RESUME, STOP, ROLLING_RESTART;
        public boolean heals() { return this == RESTART || this == RESUME; }
    }
    enum TargetKind { NAMED, PARTITION_LEADER, TRANSACTION_COORDINATOR }
    record Target(TargetKind kind, String name, String topic, int partition, String transactionalIdPrefix) {
        public Target {
            java.util.Objects.requireNonNull(kind);
            switch (kind) {
                case NAMED -> { if (name == null || !name.matches("broker-[1-3]")) throw new IllegalArgumentException("Invalid broker name"); }
                case PARTITION_LEADER -> { Checks.requireNonBlank(topic, "topic"); if (partition < 0) throw new IllegalArgumentException("Invalid partition"); }
                case TRANSACTION_COORDINATOR -> Checks.requireNonBlank(transactionalIdPrefix, "transactionalIdPrefix");
            }
        }
    }
    enum RollingOrder { FIXED, COORDINATOR_FIRST, COORDINATOR_LAST }
    record Request(Target target, Action action, Duration duration, Duration timeout, Integer commitTransactionVersion,
                   RollingOrder order, boolean preferredElection) {
        public Request(Target target, Action action, Duration duration, Duration timeout, Integer commitTransactionVersion) {
            this(target, action, duration, timeout, commitTransactionVersion, null, false);
        }
        public Request(Target target, Action action, Duration duration, Duration timeout) {
            this(target, action, duration, timeout, null);
        }
        public Request {
            java.util.Objects.requireNonNull(target);
            if (commitTransactionVersion != null && (target.kind() != TargetKind.TRANSACTION_COORDINATOR
                    || commitTransactionVersion < 1 || commitTransactionVersion > 2))
                throw new IllegalArgumentException("Commit witness requires a transaction coordinator and explicit TV1/TV2");
            if (action == Action.ROLLING_RESTART) {
                if (order == null || target.kind() != TargetKind.TRANSACTION_COORDINATOR || commitTransactionVersion != null
                        || !duration.isZero() || timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofMinutes(5)) > 0)
                    throw new IllegalArgumentException("Rolling restart requires a sink coordinator, order, no hold, and timeout in (0,5m]");
            } else {
            if (order != null || preferredElection) throw new IllegalArgumentException("Rolling options require rolling-restart");
            if (action != Action.KILL && action != Action.PAUSE) throw new IllegalArgumentException("Expected kill or pause");
            if (duration.isZero() || duration.isNegative() || duration.compareTo(Duration.ofMinutes(2)) > 0
                    || timeout.compareTo(duration) <= 0 || timeout.compareTo(Duration.ofMinutes(5)) > 0)
                throw new IllegalArgumentException("Invalid broker fault duration/timeout");
            }
        }
    }
    record Selection(Target requested, String broker, String topic, int partition, int partitionCount,
                     String transactionalId, Long producerId, Integer producerEpoch, int leader) {}
    default List<Evidence> brokerFault(Request request, List<KafkaLogCapture.Partition> partitions) {
        throw new UnsupportedOperationException("Runtime Kafka broker targets unavailable");
    }
    /** The Kafka coordinator uses Utils.abs, which maps MIN_VALUE to zero. */
    static int transactionStatePartition(String transactionalId, int count) {
        if (count < 1) throw new IllegalArgumentException("Invalid metadata partition count");
        int hash = java.util.Objects.requireNonNull(transactionalId).hashCode();
        return (hash == Integer.MIN_VALUE ? 0 : Math.abs(hash)) % count;
    }
    static int transactionStatePartitionCount(List<Integer> partitions) {
        var ids = partitions.stream().sorted().toList();
        if (ids.isEmpty()) throw new IllegalArgumentException("Empty transaction-state metadata");
        for (int i = 0; i < ids.size(); i++) if (ids.get(i) != i)
            throw new IllegalArgumentException("Noncontiguous transaction-state metadata");
        return ids.size();
    }

    record Snapshot(String containerId, String imageId, String networkId, int brokerId, boolean running, boolean paused) {
        public Snapshot(String containerId, String imageId, String networkId, int brokerId, boolean running) {
            this(containerId, imageId, networkId, brokerId, running, false);
        }
        public Snapshot {
            containerId = Checks.requireNonBlank(containerId, "containerId");
            imageId = Checks.requireNonBlank(imageId, "imageId");
            networkId = Checks.requireNonBlank(networkId, "networkId");
            if (brokerId < 1 || brokerId > 3) throw new IllegalArgumentException("Unknown broker ID");
        }
    }
    record Leadership(String topic, int partition, int leader, List<Integer> replicas, List<Integer> inSyncReplicas) {
        public Leadership { replicas = List.copyOf(replicas); inSyncReplicas = List.copyOf(inSyncReplicas); }
    }
    /** Per-broker timeline and the complete ISR barrier before advancing to another broker. */
    record RollingProgress(int ordinal, List<String> order, RollingOrder mode, long startedAtMillis, long recoveredAtMillis,
                           Selection reference, List<Leadership> fullyReplicated, boolean preferredElectionRequested,
                           boolean preferredElectionConfirmed) {
        public RollingProgress { order = List.copyOf(order); fullyReplicated = List.copyOf(fullyReplicated); }
        public boolean confirmed(String broker) {
            return ordinal >= 1 && ordinal <= 3 && order.size() == 3
                    && new java.util.HashSet<>(order).equals(java.util.Set.of("broker-1", "broker-2", "broker-3"))
                    && order.get(ordinal - 1).equals(broker) && mode != null && reference != null
                    && reference.transactionalId() != null && recoveredAtMillis >= startedAtMillis
                    && fullyReplicated.stream().anyMatch(p -> p.topic().equals("__transaction_state"))
                    && fullIsr(fullyReplicated) && (!preferredElectionRequested || preferredElectionConfirmed);
        }
    }
    static boolean fullIsr(List<Leadership> partitions) {
        return !partitions.isEmpty() && partitions.stream().map(p -> p.topic() + "/" + p.partition()).distinct().count() == partitions.size()
                && partitions.stream().allMatch(p -> p.replicas().size() == 3 && p.inSyncReplicas().size() == 3
                    && new java.util.HashSet<>(p.replicas()).equals(java.util.Set.of(1, 2, 3))
                    && new java.util.HashSet<>(p.inSyncReplicas()).equals(java.util.Set.of(1, 2, 3))
                    && p.inSyncReplicas().contains(p.leader()));
    }
    record Evidence(String target, boolean restart, Snapshot before, Snapshot after,
                    List<Leadership> leadersBefore, List<Leadership> leadersAfter, String error,
                    Action action, Selection selection, KafkaCommitWindow commitWindow, RollingProgress rolling) {
        public Evidence(String target, boolean restart, Snapshot before, Snapshot after, List<Leadership> leadersBefore,
                        List<Leadership> leadersAfter, String error, Action action, Selection selection, KafkaCommitWindow commitWindow) {
            this(target, restart, before, after, leadersBefore, leadersAfter, error, action, selection, commitWindow, null);
        }
        public Evidence withRolling(RollingProgress progress) {
            return new Evidence(target, restart, before, after, leadersBefore, leadersAfter, error, action, selection, commitWindow, progress);
        }
        public Evidence(String target, boolean restart, Snapshot before, Snapshot after,
                        List<Leadership> leadersBefore, List<Leadership> leadersAfter, String error,
                        Action action, Selection selection) {
            this(target, restart, before, after, leadersBefore, leadersAfter, error, action, selection, null);
        }
        public Evidence withCommitWindow(KafkaCommitWindow window) {
            return new Evidence(target, restart, before, after, leadersBefore, leadersAfter, error, action, selection, window, rolling);
        }
        public Evidence(String target, boolean restart, Snapshot before, Snapshot after,
                        List<Leadership> leadersBefore, List<Leadership> leadersAfter, String error) {
            this(target, restart, before, after, leadersBefore, leadersAfter, error,
                    restart ? Action.RESTART : Action.KILL, null);
        }
        public Evidence withSelection(Selection selected) {
            return new Evidence(target, restart, before, after, leadersBefore, leadersAfter, error, action, selected, commitWindow, rolling);
        }
        public Evidence withError(String failure) {
            return new Evidence(target, restart, before, after, leadersBefore, leadersAfter,
                    error == null ? failure : error + "; " + failure, action, selection, commitWindow, rolling);
        }
        public Evidence { leadersBefore = List.copyOf(leadersBefore); leadersAfter = List.copyOf(leadersAfter); }
        public boolean confirmed() {
            if (rolling != null && !rolling.confirmed(target)) return false;
            if (commitWindow != null && (!commitWindow.confirmed(selection) || after == null
                    || !after.equals(commitWindow.ongoing().broker()) || !after.equals(commitWindow.committed().broker()))) return false;
            if (error != null || before == null || after == null || before.brokerId() < 1
                    || !target.equals("broker-" + before.brokerId()) || before.brokerId() != after.brokerId()
                    || before.containerId() == null || before.imageId() == null || before.networkId() == null
                    || !before.containerId().equals(after.containerId()) || !before.imageId().equals(after.imageId())
                    || !before.networkId().equals(after.networkId()) || restart != action.heals() || !statesMatch()
                    || leadersBefore.isEmpty() || leadersAfter.size() != leadersBefore.size()) return false;
            if (selection != null && (!target.equals(selection.broker()) || selection.leader() != before.brokerId()
                    || (!restart && selection.topic() != null && leadersBefore.stream().noneMatch(value ->
                        value.topic().equals(selection.topic()) && value.partition() == selection.partition()
                                && value.leader() == before.brokerId())))) return false;
            if (leadersBefore.stream().map(item -> item.topic() + ":" + item.partition()).distinct().count() != leadersBefore.size()
                    || leadersAfter.stream().map(item -> item.topic() + ":" + item.partition()).distinct().count() != leadersAfter.size()
                    || java.util.stream.Stream.concat(leadersBefore.stream(), leadersAfter.stream()).anyMatch(item ->
                            item.replicas().size() != 3 || !new java.util.HashSet<>(item.replicas()).equals(java.util.Set.of(1, 2, 3))
                                    || !item.replicas().containsAll(item.inSyncReplicas()))) return false;
            if (restart) return leadersBefore.stream().allMatch(old -> leadersAfter.stream().anyMatch(now ->
                    samePartition(old, now) && now.leader() > 0 && now.inSyncReplicas().size() >= 2
                            && now.inSyncReplicas().contains(after.brokerId())));
            if (action == Action.STOP) return leadersBefore.stream().allMatch(old -> leadersAfter.stream().anyMatch(now ->
                    samePartition(old, now) && now.leader() > 0 && now.leader() != before.brokerId()
                            && now.inSyncReplicas().size() >= 2 && !now.inSyncReplicas().contains(before.brokerId())));
            var affected = leadersBefore.stream().filter(leader -> leader.leader() == before.brokerId()).toList();
            return !affected.isEmpty() && affected.stream().allMatch(old -> leadersAfter.stream().anyMatch(now ->
                    samePartition(old, now) && now.leader() > 0 && now.leader() != old.leader()
                            && now.inSyncReplicas().size() >= 2 && !now.inSyncReplicas().contains(before.brokerId())));
        }
        private boolean statesMatch() {
            return switch (action) {
                case KILL, STOP -> before.running() && !before.paused() && !after.running() && !after.paused();
                case RESTART -> !before.running() && !before.paused() && after.running() && !after.paused();
                case PAUSE -> before.running() && !before.paused() && after.running() && after.paused();
                case RESUME -> before.running() && before.paused() && after.running() && !after.paused();
                case ROLLING_RESTART -> false; // The orchestration emits physical STOP/RESTART observations.
            };
        }
        private static boolean samePartition(Leadership left, Leadership right) {
            return left.topic().equals(right.topic()) && left.partition() == right.partition();
        }
    }
}
