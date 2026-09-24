package org.savonitar.flink.stability.core.validation.kafka;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The transactions of one transactional-ID prefix, as the Kafka coordinators report them after
 * the Flink process fence. Nothing can resolve an open one afterwards except its broker timeout.
 */
public record KafkaTransactionListing(String transactionalIdPrefix, List<Transaction> transactions) {
    private static final Set<String> RESOLVED_STATES =
            Set.of("Empty", "CompleteCommit", "CompleteAbort");

    public KafkaTransactionListing {
        Objects.requireNonNull(transactionalIdPrefix, "transactionalIdPrefix");
        transactions = List.copyOf(Objects.requireNonNull(transactions, "transactions"));
    }

    /** Transactions still open or mid-completion; each pins its partitions' last stable offset. */
    public List<Transaction> unresolved() {
        return transactions.stream()
                .filter(transaction -> !RESOLVED_STATES.contains(transaction.state()))
                .toList();
    }

    /** One transactional ID with Kafka's state name, e.g. {@code Ongoing} or {@code CompleteCommit}. */
    public record Transaction(
            String transactionalId,
            String state,
            long producerId,
            int producerEpoch,
            List<String> topicPartitions) {
        public Transaction {
            Objects.requireNonNull(transactionalId, "transactionalId");
            Objects.requireNonNull(state, "state");
            topicPartitions = List.copyOf(Objects.requireNonNull(
                    topicPartitions, "topicPartitions"));
        }
    }
}
