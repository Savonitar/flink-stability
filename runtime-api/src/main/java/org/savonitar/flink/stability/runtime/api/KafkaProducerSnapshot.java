package org.savonitar.flink.stability.runtime.api;

import java.util.List;

/** Read-only broker observations. Record timestamps are event time, not append time. */
public record KafkaProducerSnapshot(String topic, int partition, long logEndOffset,
        List<Producer> producers, List<Transaction> transactions) {
    public KafkaProducerSnapshot {
        producers = List.copyOf(producers);
        transactions = List.copyOf(transactions);
    }
    public record Producer(long producerId, int epoch, int lastSequence, Long transactionStartOffset) {}
    public record Transaction(String transactionalId, String state, long producerId, int epoch,
                              long timeoutMs, Long startTimeMs) {}
}
