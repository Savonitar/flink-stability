package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;

/** Named owned-broker operations with physical process and partition leadership observations. */
public interface KafkaBrokerControl {
    default Evidence brokerOperation(String name, boolean restart, List<KafkaLogCapture.Partition> partitions, Duration timeout) {
        throw new UnsupportedOperationException("Named Kafka broker operations unavailable");
    }

    record Snapshot(String containerId, String imageId, String networkId, int brokerId, boolean running) {
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
    record Evidence(String target, boolean restart, Snapshot before, Snapshot after,
                    List<Leadership> leadersBefore, List<Leadership> leadersAfter, String error) {
        public Evidence { leadersBefore = List.copyOf(leadersBefore); leadersAfter = List.copyOf(leadersAfter); }
        public boolean confirmed() {
            if (error != null || before == null || after == null || before.brokerId() < 1
                    || !target.equals("broker-" + before.brokerId()) || before.brokerId() != after.brokerId()
                    || before.containerId() == null || before.imageId() == null || before.networkId() == null
                    || !before.containerId().equals(after.containerId()) || !before.imageId().equals(after.imageId())
                    || !before.networkId().equals(after.networkId()) || before.running() == restart || after.running() != restart
                    || leadersBefore.isEmpty() || leadersAfter.size() != leadersBefore.size()) return false;
            if (leadersBefore.stream().map(item -> item.topic() + ":" + item.partition()).distinct().count() != leadersBefore.size()
                    || leadersAfter.stream().map(item -> item.topic() + ":" + item.partition()).distinct().count() != leadersAfter.size()
                    || java.util.stream.Stream.concat(leadersBefore.stream(), leadersAfter.stream()).anyMatch(item ->
                            item.replicas().size() != 3 || !new java.util.HashSet<>(item.replicas()).equals(java.util.Set.of(1, 2, 3))
                                    || !item.replicas().containsAll(item.inSyncReplicas()))) return false;
            if (restart) return leadersBefore.stream().allMatch(old -> leadersAfter.stream().anyMatch(now ->
                    samePartition(old, now) && now.leader() > 0 && now.inSyncReplicas().size() >= 2
                            && now.inSyncReplicas().contains(after.brokerId())));
            var affected = leadersBefore.stream().filter(leader -> leader.leader() == before.brokerId()).toList();
            return !affected.isEmpty() && affected.stream().allMatch(old -> leadersAfter.stream().anyMatch(now ->
                    samePartition(old, now) && now.leader() > 0 && now.leader() != old.leader()
                            && now.inSyncReplicas().size() >= 2 && !now.inSyncReplicas().contains(before.brokerId())));
        }
        private static boolean samePartition(Leadership left, Leadership right) {
            return left.topic().equals(right.topic()) && left.partition() == right.partition();
        }
    }
}
