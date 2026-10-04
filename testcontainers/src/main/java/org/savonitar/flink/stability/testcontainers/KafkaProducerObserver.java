package org.savonitar.flink.stability.testcontainers;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.savonitar.flink.stability.runtime.api.KafkaProducerSnapshot;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** One bounded observation of the owned broker; it never mutates transactions. */
final class KafkaProducerObserver {
    static KafkaProducerSnapshot observe(String bootstrap, String topic, int partition,
            String prefix, Duration timeout) throws Exception {
        var deadline = MonotonicDeadline.start(timeout, System::nanoTime);
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Math.toIntExact(timeout.toMillis()));
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(timeout.toMillis()));
        Admin admin = Admin.create(config);
        try {
            var ids = admin.listTransactions().all().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS)
                    .stream().map(value -> value.transactionalId()).filter(id -> id.startsWith(prefix)).sorted().toList();
            var descriptions = admin.describeTransactions(ids).all().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS);
            var transactions = ids.stream().map(id -> {
                var value = descriptions.get(id);
                return new KafkaProducerSnapshot.Transaction(id, value.state().toString(), value.producerId(),
                        value.producerEpoch(), value.transactionTimeoutMs(),
                        value.transactionStartTimeMs().isPresent() ? value.transactionStartTimeMs().getAsLong() : null);
            }).toList();
            var tp = new TopicPartition(topic, partition);
            var producers = admin.describeProducers(List.of(tp)).all().get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS)
                    .get(tp).activeProducers().stream().map(value -> new KafkaProducerSnapshot.Producer(
                            value.producerId(), value.producerEpoch(), value.lastSequence(),
                            value.currentTransactionStartOffset().isPresent() ? value.currentTransactionStartOffset().getAsLong() : null)).toList();
            long end = admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all()
                    .get(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS).get(tp).offset();
            return new KafkaProducerSnapshot(topic, partition, end, producers, transactions);
        } finally {
            admin.close(deadline.remaining());
        }
    }
}
