package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/** Lists one prefix's transactions through the Kafka Admin API (KIP-664). */
public final class KafkaAdminTransactionLister {

    /**
     * Lists every transactional ID that starts with {@code transactionalIdPrefix} and describes
     * each one. The broker cannot filter by prefix, so the filter runs here. Each of the two
     * Admin calls, and closing the client, is bounded by {@code callTimeout}.
     */
    public KafkaTransactionListing list(
            String bootstrapServers,
            String transactionalIdPrefix,
            Duration callTimeout) throws Exception {
        Objects.requireNonNull(transactionalIdPrefix, "transactionalIdPrefix");
        int timeoutMs = Math.toIntExact(Math.min(Integer.MAX_VALUE, callTimeout.toMillis()));
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        Admin admin = Admin.create(properties);
        try {
            List<String> ids = unwrap(() -> admin.listTransactions().all().get()).stream()
                    .map(TransactionListing::transactionalId)
                    .filter(id -> id.startsWith(transactionalIdPrefix))
                    .sorted()
                    .toList();
            if (ids.isEmpty()) {
                return new KafkaTransactionListing(transactionalIdPrefix, List.of());
            }
            Map<String, TransactionDescription> descriptions =
                    unwrap(() -> admin.describeTransactions(ids).all().get());
            return new KafkaTransactionListing(transactionalIdPrefix, ids.stream()
                    .map(id -> transaction(id, descriptions.get(id)))
                    .toList());
        } finally {
            admin.close(callTimeout);
        }
    }

    private static KafkaTransactionListing.Transaction transaction(
            String transactionalId,
            TransactionDescription description) {
        return new KafkaTransactionListing.Transaction(
                transactionalId,
                description.state().toString(),
                description.producerId(),
                description.producerEpoch(),
                description.topicPartitions().stream()
                        .map(Object::toString)
                        .sorted()
                        .toList());
    }

    private static <T> T unwrap(AdminCall<T> call) throws Exception {
        try {
            return call.get();
        } catch (ExecutionException failure) {
            throw failure.getCause() instanceof Exception cause ? cause : failure;
        }
    }

    @FunctionalInterface
    private interface AdminCall<T> {
        T get() throws ExecutionException, InterruptedException;
    }
}
