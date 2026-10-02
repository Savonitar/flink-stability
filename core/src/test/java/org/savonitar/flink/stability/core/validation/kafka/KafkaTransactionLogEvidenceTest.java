package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.*;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KafkaTransactionLogEvidenceTest {
    @Test void kafkaHashUsesActualPartitionCountIncludingMinValue() {
        assertEquals(Integer.MIN_VALUE, "polygenelubricants".hashCode());
        assertEquals(0, KafkaAdminTransactionLister.transactionStatePartition("polygenelubricants", 17));
        for (String id : List.of("bounded-eos-0-1", "transaction-Δ-🚀", "negative"))
            for (int count : List.of(1, 7, 53))
                assertEquals(org.apache.kafka.common.utils.Utils.abs(id.hashCode()) % count,
                        KafkaAdminTransactionLister.transactionStatePartition(id, count));
        assertThrows(IllegalArgumentException.class, () -> KafkaAdminTransactionLister.transactionStatePartition("x", 0));
    }

    @Test void joinsBothMarkersByEpochAndLatestOffsetNotTimestamp() {
        var evidence = new KafkaTransactionLogEvidence();
        var deadline = deadline();
        evidence.add(source("__transaction_state", "new.log"), coordinator(40, (byte) 4, 3, 1), deadline);
        evidence.add(source("__transaction_state", "old.log"), coordinator(10, (byte) 1, 2, 999), deadline);
        evidence.add(source("output", "abort.log"), marker(20, 2, ControlRecordType.ABORT), deadline);
        evidence.add(source("output", "commit.log"), marker(30, 3, ControlRecordType.COMMIT), deadline);
        var summary = evidence.summary();
        assertTrue(summary.decoded()); assertEquals(2, summary.chains().size());
        for (var chain : summary.chains()) {
            assertEquals("task-0-1", chain.transactionalId()); assertEquals(17, chain.producerId());
            assertEquals(40, chain.latestCoordinator().offset());
            assertEquals("CompleteCommit", chain.latestCoordinator().record().decoded().stateName());
            assertEquals(chain.producerEpoch() == 2 ? "ABORT" : "COMMIT", chain.markers().getFirst().type());
        }
    }

    @Test void tombstoneDoesNotEraseObservedIdentityOrClaimCurrentState() {
        var evidence = new KafkaTransactionLogEvidence();
        evidence.add(source("__transaction_state", "old.log"), coordinator(2, (byte) 1, 2, 1), deadline());
        evidence.add(source("__transaction_state", "last.log"), segment(MemoryRecords.withRecords(9,
                Compression.none().build(), new SimpleRecord(key(), null))), deadline());
        assertEquals(KafkaTransactionStateDecoder.Status.TOMBSTONE,
                evidence.summary().chains().getFirst().latestCoordinator().record().status());
        assertNull(evidence.summary().chains().getFirst().latestCoordinator().record().decoded());
    }

    @Test void unknownVersionIsDiagnosticAndCannotSupplyAnIdentity() {
        var evidence = new KafkaTransactionLogEvidence();
        evidence.add(source("__transaction_state", "unknown.log"), segment(MemoryRecords.withRecords(
                Compression.none().build(), new SimpleRecord(key(), new byte[]{0, 2}))), deadline());
        assertFalse(evidence.summary().decoded()); assertTrue(evidence.summary().chains().isEmpty());
        assertEquals(KafkaTransactionStateDecoder.Status.UNSUPPORTED_VERSION,
                evidence.summary().coordinatorRecords().getFirst().record().status());
    }

    @Test void unmatchedDataIdentityIsRetainedAndExpiredBudgetRejects() {
        var evidence = new KafkaTransactionLogEvidence();
        var data = marker(1, 2, ControlRecordType.COMMIT);
        evidence.add(source("output", "data.log"), data, deadline());
        assertNull(evidence.summary().chains().getFirst().transactionalId());
        assertNull(evidence.summary().chains().getFirst().latestCoordinator());
        var time = new java.util.concurrent.atomic.AtomicLong();
        var expired = MonotonicDeadline.start(Duration.ofMillis(1), time::get); time.set(1_000_000);
        assertThrows(IllegalArgumentException.class, () -> evidence.add(source("output", "data.log"), data, expired));
    }

    private static KafkaTransactionLogEvidence.Source source(String topic, String file) {
        return new KafkaTransactionLogEvidence.Source(topic, 0, file);
    }
    private static MonotonicDeadline deadline() { return MonotonicDeadline.start(Duration.ofSeconds(60), System::nanoTime); }
    private static byte[] key() { return MessageUtil.toVersionPrefixedBytes((short) 0,
            new TransactionLogKey().setTransactionalId("task-0-1")); }
    private static KafkaLogSegmentDecoder.Segment coordinator(long offset, byte state, int epoch, long timestamp) {
        var value = new TransactionLogValue().setProducerId(17).setProducerEpoch((short) epoch)
                .setTransactionTimeoutMs(1000).setTransactionStatus(state).setTransactionStartTimestampMs(timestamp)
                .setTransactionLastUpdateTimestampMs(timestamp).setTransactionPartitions(List.of());
        return segment(MemoryRecords.withRecords(offset, Compression.none().build(),
                new SimpleRecord(key(), MessageUtil.toVersionPrefixedBytes((short) 1, value))));
    }
    private static KafkaLogSegmentDecoder.Segment marker(long offset, int epoch, ControlRecordType type) {
        return segment(MemoryRecords.withEndTransactionMarker(offset, 25, 9, 17, (short) epoch,
                new EndTransactionMarker(type, 19)));
    }
    private static KafkaLogSegmentDecoder.Segment segment(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate(); byte[] bytes = new byte[buffer.remaining()]; buffer.get(bytes);
        return new KafkaLogSegmentDecoder().decode(bytes, 100);
    }
}
