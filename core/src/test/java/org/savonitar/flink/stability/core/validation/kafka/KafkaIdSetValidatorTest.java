package org.savonitar.flink.stability.core.validation.kafka;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaIdSetValidatorTest {
    @Test
    void completeExactSnapshotPasses() {
        KafkaIdSetValidationResult result = validator(List.of("0", "1", "2"))
                .validate("kafka:9092", "output", 3, Duration.ofMinutes(2));

        assertEquals(KafkaIdSetValidationResult.Status.PASS, result.status());
        assertEquals("validator.kafka.id-set.match", result.reason());
        assertEquals(3, result.evidence().observedCount());
        assertEquals(Map.of(0, 3L), result.evidence().endOffsets());
        assertTrue(result.evidence().snapshotComplete());
    }

    @Test
    void malformedIdsHaveDeterministicPrecedence() {
        KafkaIdSetValidationResult result = validator(
                List.of("0", "01", "7", "0"))
                .validate("kafka:9092", "output", 2, Duration.ofMinutes(2));

        assertEquals("validator.kafka.id-set.malformed-ids", result.reason());
        assertEquals(1, totals(result).malformedCount());
        assertEquals(
                List.of(new KafkaIdSetValidationResult.RecordSample(0, 1, "01")),
                result.evidence().malformedSamples());
        assertTrue(totals(result).unexpectedCount() > 0);
        assertTrue(totals(result).duplicateCount() > 0);
    }

    @Test
    void unexpectedIdsPrecedeDuplicatesAndMissingIds() {
        KafkaIdSetValidationResult result = validator(List.of("0", "0", "4"))
                .validate("kafka:9092", "output", 3, Duration.ofMinutes(2));

        assertEquals("validator.kafka.id-set.unexpected-ids", result.reason());
        assertEquals(
                List.of(new KafkaIdSetValidationResult.RecordSample(0, 2, "4")),
                result.evidence().unexpectedSamples());
        assertEquals(1, totals(result).duplicateCount());
        assertEquals(2, totals(result).missingCount());
    }

    @Test
    void duplicateIdsPrecedeMissingIds() {
        KafkaIdSetValidationResult result = validator(List.of("0", "0", "2"))
                .validate("kafka:9092", "output", 3, Duration.ofMinutes(2));

        assertEquals("validator.kafka.id-set.duplicate-ids", result.reason());
        assertEquals(
                List.of(new KafkaIdSetValidationResult.RecordSample(0, 1, "0")),
                result.evidence().duplicateSamples());
        assertEquals(List.of(1L), result.evidence().missingSamples());
    }

    @Test
    void anomalySamplesUseTheSmallestCoordinateIndependentOfPartitionEncounterOrder() {
        KafkaTopicSnapshot.ObservedRecord partitionOneDuplicate =
                new KafkaTopicSnapshot.ObservedRecord(1, 0, "0");
        KafkaTopicSnapshot.ObservedRecord partitionZeroDuplicate =
                new KafkaTopicSnapshot.ObservedRecord(0, 0, "0");
        KafkaTopicSnapshot.ObservedRecord partitionOneUnexpected =
                new KafkaTopicSnapshot.ObservedRecord(1, 1, "7");
        KafkaTopicSnapshot.ObservedRecord partitionZeroUnexpected =
                new KafkaTopicSnapshot.ObservedRecord(0, 1, "7");

        for (List<KafkaTopicSnapshot.ObservedRecord> records : List.of(
                List.of(
                        partitionOneDuplicate,
                        partitionZeroDuplicate,
                        partitionOneUnexpected,
                        partitionZeroUnexpected),
                List.of(
                        partitionZeroUnexpected,
                        partitionOneUnexpected,
                        partitionZeroDuplicate,
                        partitionOneDuplicate))) {
            KafkaTopicSnapshot snapshot = new KafkaTopicSnapshot(
                    "output", Map.of(0, 0L, 1, 0L), Map.of(0, 2L, 1, 2L), records);
            KafkaIdSetValidationResult result = new KafkaIdSetValidator(
                            (bootstrap, topic, timeout, maximumRecords) -> snapshot)
                    .validate("kafka:9092", "output", 1, Duration.ofMinutes(2));

            assertEquals(
                    List.of(new KafkaIdSetValidationResult.RecordSample(1, 0, "0")),
                    result.evidence().duplicateSamples());
            assertEquals(
                    List.of(new KafkaIdSetValidationResult.RecordSample(0, 1, "7")),
                    result.evidence().unexpectedSamples());
        }
    }

    @Test
    void completeSnapshotWithMissingIdsFailsAuthoritatively() {
        KafkaIdSetValidationResult result = validator(List.of("0", "3"))
                .validate("kafka:9092", "output", 5, Duration.ofMinutes(2));

        assertEquals("validator.kafka.id-set.missing-ids", result.reason());
        assertEquals(3, totals(result).missingCount());
        assertEquals(List.of(1L, 2L, 4L), result.evidence().missingSamples());
    }

    @Test
    void inabilityToReachKafkaFailsTheExperimentWithVerificationReason() {
        KafkaSnapshotReader unreachable = (bootstrap, topic, timeout, maximumRecords) -> {
            throw new KafkaSnapshotException(
                    "verification.kafka.unreachable-after-timeout",
                    "Kafka remained unreachable for PT2M");
        };

        KafkaIdSetValidationResult result = new KafkaIdSetValidator(unreachable)
                .validate("kafka:9092", "output", 100, Duration.ofMinutes(2));

        assertEquals(KafkaIdSetValidationResult.Status.FAIL, result.status());
        assertEquals("verification.kafka.unreachable-after-timeout", result.reason());
        assertFalse(result.evidence().snapshotComplete());
        assertTrue(result.evidence().defectTotals().isEmpty());
    }

    @Test
    void deadlineExpiryBeforeBrokerDiscoveryIsClassifiedAsUnreachable() {
        AtomicBoolean readerCalled = new AtomicBoolean();
        AtomicInteger clockCalls = new AtomicInteger();
        KafkaSnapshotReader reader = (bootstrap, topic, timeout, maximumRecords) -> {
            readerCalled.set(true);
            return snapshot(List.of("0"));
        };

        KafkaIdSetValidationResult result = new KafkaIdSetValidator(
                        reader,
                        () -> clockCalls.getAndIncrement() == 0
                                ? 0L
                                : Duration.ofMillis(1).toNanos())
                .validate("kafka:9092", "output", 1, Duration.ofMillis(1));

        assertEquals("verification.kafka.unreachable-after-timeout", result.reason());
        assertFalse(readerCalled.get());
        assertFalse(result.evidence().snapshotComplete());
        assertTrue(result.evidence().defectTotals().isEmpty());
    }

    @Test
    void tombstoneIsMalformedAndRetainsItsExactKafkaCoordinate() {
        KafkaTopicSnapshot snapshot = new KafkaTopicSnapshot(
                "output",
                Map.of(0, 0L),
                Map.of(0, 1L),
                List.of(new KafkaTopicSnapshot.ObservedRecord(0, 0, null)));

        KafkaIdSetValidationResult result = new KafkaIdSetValidator(
                        (bootstrap, topic, timeout, maximumRecords) -> snapshot)
                .validate("kafka:9092", "output", 1, Duration.ofMinutes(2));

        assertEquals("validator.kafka.id-set.malformed-ids", result.reason());
        assertEquals(
                List.of(new KafkaIdSetValidationResult.RecordSample(0, 0, null)),
                result.evidence().malformedSamples());
    }

    @Test
    void timeoutRetainsPartialBoundsAndRawRecordEvidence() {
        KafkaSnapshotException.PartialEvidence partial =
                new KafkaSnapshotException.PartialEvidence(
                        Map.of(0, 0L),
                        Map.of(0, 5L),
                        1,
                        List.of(new KafkaTopicSnapshot.ObservedRecord(0, 0, "0")));
        KafkaSnapshotReader incomplete = (bootstrap, topic, timeout, maximumRecords) -> {
            throw new KafkaSnapshotException(
                    "verification.kafka.incomplete-after-timeout",
                    "deadline",
                    null,
                    partial);
        };

        KafkaIdSetValidationResult result = new KafkaIdSetValidator(incomplete)
                .validate("kafka:9092", "output", 5, Duration.ofMinutes(2));

        assertEquals("verification.kafka.incomplete-after-timeout", result.reason());
        assertEquals(Map.of(0, 5L), result.evidence().endOffsets());
        assertEquals(
                List.of(new KafkaIdSetValidationResult.RecordSample(0, 0, "0")),
                result.evidence().observedSamples());
        assertFalse(result.evidence().snapshotComplete());
        assertTrue(result.evidence().defectTotals().isEmpty());
    }

    @Test
    void snapshotFailurePreservesTruthfulCountBeyondItsBoundedSamples() {
        List<KafkaTopicSnapshot.ObservedRecord> samples = new java.util.ArrayList<>();
        for (int offset = 0; offset < 100; offset++) {
            samples.add(new KafkaTopicSnapshot.ObservedRecord(
                    0, offset, Integer.toString(offset)));
        }
        KafkaSnapshotException.PartialEvidence partial =
                new KafkaSnapshotException.PartialEvidence(
                        Map.of(0, 0L),
                        Map.of(0, 1_000_000L),
                        1_000_000,
                        samples);
        KafkaSnapshotReader incomplete = (bootstrap, topic, timeout, maximumRecords) -> {
            throw new KafkaSnapshotException(
                    "verification.kafka.incomplete-after-timeout",
                    "deadline",
                    null,
                    partial);
        };

        KafkaIdSetValidationResult result = new KafkaIdSetValidator(incomplete)
                .validate("kafka:9092", "output", 1_000_000, Duration.ofMinutes(2));

        assertEquals(1_000_000, result.evidence().observedCount());
        assertEquals(100, result.evidence().observedSamples().size());
        assertEquals(
                new KafkaIdSetValidationResult.RecordSample(0, 99, "99"),
                result.evidence().observedSamples().get(99));
        assertFalse(result.evidence().snapshotComplete());
        assertTrue(result.evidence().defectTotals().isEmpty());
    }

    @Test
    void decodingAndComparisonShareTheSameAbsoluteDeadlineAsSnapshotCapture() {
        AtomicLong clock = new AtomicLong();
        KafkaTopicSnapshot snapshot = snapshot(List.of("0"));
        KafkaSnapshotReader reader = (bootstrap, topic, timeout, maximumRecords) -> {
            clock.set(Duration.ofSeconds(2).toNanos());
            return snapshot;
        };

        KafkaIdSetValidationResult result = new KafkaIdSetValidator(reader, clock::get)
                .validate("kafka:9092", "output", 1, Duration.ofSeconds(1));

        assertEquals("verification.kafka.incomplete-after-timeout", result.reason());
        assertEquals(Map.of(0, 1L), result.evidence().endOffsets());
        assertFalse(result.evidence().snapshotComplete());
        assertTrue(result.evidence().defectTotals().isEmpty());
    }

    @Test
    void negativeNanoTimeOriginDoesNotExpireAHealthyComparison() {
        AtomicLong clock = new AtomicLong(-1_000_000L);
        KafkaIdSetValidationResult result = new KafkaIdSetValidator(
                        (bootstrap, topic, timeout, maximumRecords) -> snapshot(List.of("0")),
                        clock::get)
                .validate("kafka:9092", "output", 1, Duration.ofSeconds(1));

        assertEquals(KafkaIdSetValidationResult.Status.PASS, result.status());
    }

    @Test
    void evidenceCollectionsAreImmutable() {
        KafkaIdSetValidationResult result = validator(List.of("0", "0"))
                .validate("kafka:9092", "output", 1, Duration.ofMinutes(2));

        assertThrows(
                UnsupportedOperationException.class,
                () -> result.evidence().duplicateSamples().add(
                        new KafkaIdSetValidationResult.RecordSample(0, 2, "0")));
        assertThrows(
                UnsupportedOperationException.class,
                () -> result.evidence().endOffsets().put(1, 2L));
    }

    private static KafkaIdSetValidator validator(List<String> values) {
        KafkaTopicSnapshot snapshot = snapshot(values);
        return new KafkaIdSetValidator(
                (bootstrap, topic, timeout, maximumRecords) -> snapshot);
    }

    private static KafkaIdSetValidationResult.DefectTotals totals(
            KafkaIdSetValidationResult result) {
        return result.evidence().defectTotals().orElseThrow();
    }

    private static KafkaTopicSnapshot snapshot(List<String> values) {
        List<KafkaTopicSnapshot.ObservedRecord> records = new java.util.ArrayList<>();
        for (int index = 0; index < values.size(); index++) {
            records.add(new KafkaTopicSnapshot.ObservedRecord(0, index, values.get(index)));
        }
        return new KafkaTopicSnapshot(
                "output", Map.of(0, 0L), Map.of(0, (long) values.size()), records);
    }
}
