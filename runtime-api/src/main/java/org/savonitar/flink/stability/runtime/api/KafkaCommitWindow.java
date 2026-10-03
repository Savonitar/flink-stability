package org.savonitar.flink.stability.runtime.api;

/** Ordered Admin observations while the selected coordinator broker remains unavailable. */
public record KafkaCommitWindow(int transactionVersion, long brokerSessionTimeoutMillis,
                                Observation ongoing, Observation committed, String error) {
    public record Transaction(String transactionalId, long producerId, int producerEpoch, String state, int coordinatorId) {}
    public record Observation(String transactionalId, long producerId, int producerEpoch,
                              String state, int coordinatorId, long observedAtMillis,
                              long elapsedAfterFaultNanos, KafkaBrokerControl.Snapshot broker,
                              KafkaBrokerControl.Leadership partition) {}

    public boolean confirmed(KafkaBrokerControl.Selection selected) {
        if (error != null || selected == null || selected.transactionalId() == null
                || selected.producerId() == null || selected.producerEpoch() == null
                || brokerSessionTimeoutMillis <= 0 || ongoing == null || committed == null
                || transactionVersion < 1 || transactionVersion > 2
                || !"ONGOING".equals(ongoing.state()) || !"COMPLETE_COMMIT".equals(committed.state())
                || ongoing.elapsedAfterFaultNanos() < 0
                || committed.elapsedAfterFaultNanos() <= ongoing.elapsedAfterFaultNanos()
                || committed.elapsedAfterFaultNanos() <= java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(brokerSessionTimeoutMillis)
                || ongoing.producerEpoch() != selected.producerEpoch()) return false;
        int delta = committed.producerEpoch() - ongoing.producerEpoch();
        if (delta != 0 && (transactionVersion != 2 || delta != 1)) return false;
        return matches(ongoing, selected) && matches(committed, selected)
                && ongoing.partition().leader() == committed.partition().leader();
    }
    private static boolean matches(Observation value, KafkaBrokerControl.Selection selected) {
        var broker = value.broker(); var partition = value.partition();
        return selected.transactionalId().equals(value.transactionalId()) && value.producerId() == selected.producerId()
                && broker != null && broker.brokerId() == selected.leader() && (!broker.running() || broker.paused())
                && partition != null && selected.topic().equals(partition.topic()) && selected.partition() == partition.partition()
                && value.coordinatorId() == partition.leader() && partition.leader() > 0 && partition.leader() != selected.leader()
                && partition.inSyncReplicas().size() >= 2 && !partition.inSyncReplicas().contains(selected.leader());
    }
}
