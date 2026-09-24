package org.savonitar.flink.stability.core.validation.kafka;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaTransactionListingTest {

    @Test
    void onlyOpenOrCompletingTransactionsAreUnresolved() {
        KafkaTransactionListing listing = new KafkaTransactionListing("eos", List.of(
                transaction("eos-0-1", "CompleteCommit"),
                transaction("eos-0-2", "CompleteAbort"),
                transaction("eos-0-3", "Empty"),
                transaction("eos-0-4", "Ongoing"),
                transaction("eos-0-5", "PrepareCommit"),
                transaction("eos-0-6", "PrepareAbort")));

        assertEquals(List.of("eos-0-4", "eos-0-5", "eos-0-6"), listing.unresolved().stream()
                .map(KafkaTransactionListing.Transaction::transactionalId)
                .toList());
    }

    private static KafkaTransactionListing.Transaction transaction(String id, String state) {
        return new KafkaTransactionListing.Transaction(id, state, 1, 0, List.of());
    }
}
