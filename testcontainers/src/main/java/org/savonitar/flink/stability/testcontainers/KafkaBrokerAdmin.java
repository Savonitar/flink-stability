package org.savonitar.flink.stability.testcontainers;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** One Admin owner for selection and all fault observations under the caller's deadline. */
final class KafkaBrokerAdmin implements AutoCloseable {
    private final Admin admin;
    KafkaBrokerAdmin(String bootstrap, Duration timeout) {
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms",
                Math.toIntExact(timeout.toMillis()), "request.timeout.ms", Math.toIntExact(timeout.toMillis())));
    }
    KafkaBrokerAdmin(Admin admin) { this.admin = admin; }
    List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> requested, MonotonicDeadline deadline) throws Exception {
        var topics = topics(requested.stream().map(KafkaLogCapture.Partition::topic).distinct().toList(), deadline);
        return requested.stream().map(partition -> {
            var info = topics.get(partition.topic()).partitions().stream()
                    .filter(item -> item.partition() == partition.partition()).findFirst().orElseThrow();
            return leadership(partition.topic(), info);
        }).toList();
    }
    KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline deadline) throws Exception {
        if (target.kind() == KafkaBrokerControl.TargetKind.NAMED)
            return new KafkaBrokerControl.Selection(target, target.name(), null, -1, 0, null, null, null,
                    Integer.parseInt(target.name().substring(7)));
        String topic = target.topic(), id = null; int partition = target.partition(), count = 0;
        Long producer = null; Integer epoch = null;
        if (target.kind() == KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR) {
            var listed = admin.listTransactions(new ListTransactionsOptions().filterStates(Set.of(TransactionState.ONGOING)))
                    .all().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS);
            if (listed.size() > 100_000) throw new IllegalStateException("Transaction listing limit exceeded");
            var ids = listed.stream().map(TransactionListing::transactionalId)
                    .filter(value -> value.startsWith(target.transactionalIdPrefix())).sorted().toList();
            if (ids.isEmpty()) throw new IllegalStateException("No open sink transaction observed");
            // Deterministic tie-break among currently open transactions, then independently recheck state.
            id = ids.getFirst();
            var description = admin.describeTransactions(List.of(id)).all()
                    .get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS).get(id);
            if (description == null || description.state() != TransactionState.ONGOING)
                throw new IllegalStateException("Selected sink transaction is no longer open");
            producer = description.producerId(); epoch = description.producerEpoch();
            topic = "__transaction_state";
            var metadata = topics(List.of(topic), deadline).get(topic);
            count = KafkaBrokerControl.transactionStatePartitionCount(metadata.partitions().stream()
                    .map(TopicPartitionInfo::partition).toList());
            partition = KafkaBrokerControl.transactionStatePartition(id, count);
        }
        var observed = leaders(List.of(new KafkaLogCapture.Partition(topic, partition)), deadline).getFirst();
        if (observed.leader() < 1 || observed.leader() > 3) throw new IllegalStateException("Partition leader is not an owned broker");
        return new KafkaBrokerControl.Selection(target, "broker-" + observed.leader(), topic, partition, count,
                id, producer, epoch, observed.leader());
    }
    long sessionTimeoutMillis(int brokerId, MonotonicDeadline deadline) throws Exception {
        var resource = new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.BROKER, Integer.toString(brokerId));
        var config = admin.describeConfigs(List.of(resource)).all().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS).get(resource);
        var entry = config == null ? null : config.get("broker.session.timeout.ms");
        if (entry == null || entry.value() == null) throw new IllegalStateException("Broker session timeout unavailable");
        long value = Long.parseLong(entry.value());
        if (value <= 0 || value > 120_000) throw new IllegalStateException("Unsupported broker session timeout");
        return value;
    }
    KafkaCommitWindow.Transaction transaction(String id, MonotonicDeadline deadline) throws Exception {
        var value = admin.describeTransactions(List.of(id)).all().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS).get(id);
        if (value == null) throw new IllegalStateException("Selected transaction unavailable");
        return new KafkaCommitWindow.Transaction(id, value.producerId(), value.producerEpoch(), value.state().name(), value.coordinatorId());
    }
    List<KafkaLogCapture.Partition> transactionPartitions(MonotonicDeadline deadline) throws Exception {
        var metadata = topics(List.of("__transaction_state"), deadline).get("__transaction_state");
        int count = KafkaBrokerControl.transactionStatePartitionCount(metadata.partitions().stream().map(TopicPartitionInfo::partition).toList());
        if (count > 128) throw new IllegalStateException("Transaction-state partition bound exceeded");
        return metadata.partitions().stream().map(p -> new KafkaLogCapture.Partition("__transaction_state", p.partition())).toList();
    }
    void electPreferred(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline) throws Exception {
        var selected = partitions.stream().map(p -> new org.apache.kafka.common.TopicPartition(p.topic(), p.partition()))
                .collect(java.util.stream.Collectors.toSet());
        admin.electLeaders(org.apache.kafka.common.ElectionType.PREFERRED, selected).all()
                .get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS);
    }
    private Map<String, TopicDescription> topics(List<String> names, MonotonicDeadline deadline) throws Exception {
        return admin.describeTopics(names).allTopicNames().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS);
    }
    private static KafkaBrokerControl.Leadership leadership(String topic, TopicPartitionInfo info) {
        return new KafkaBrokerControl.Leadership(topic, info.partition(), info.leader() == null ? -1 : info.leader().id(),
                info.replicas().stream().map(org.apache.kafka.common.Node::id).toList(),
                info.isr().stream().map(org.apache.kafka.common.Node::id).toList());
    }
    @Override public void close() { admin.close(Duration.ZERO); }
}
