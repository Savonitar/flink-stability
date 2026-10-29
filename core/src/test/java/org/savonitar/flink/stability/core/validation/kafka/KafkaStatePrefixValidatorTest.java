package org.savonitar.flink.stability.core.validation.kafka;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class KafkaStatePrefixValidatorTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test void independentlyChecksHandwrittenTwoKeyPrefixesAndIgnoresArrivalOrder() {
        var rows = List.of(json(4,0,3,9), json(1,1,1,2), json(2,0,2,4),
                json(0,0,1,1), json(5,1,3,12), json(3,1,2,6));
        var result = validate(rows, 6, 2);
        assertEquals(KafkaIdSetValidationResult.Status.PASS, result.status());
        assertEquals("kafka.state-prefix", result.validatorType());
        assertEquals("validator.kafka.state-prefix.match", result.reason());
        assertEquals(0, result.stateEvidence().mismatchCount().orElseThrow());
        assertEquals(6, result.evidence().defectTotals().orElseThrow().distinctExpectedCount());
    }

    @Test void completeIdsWithResetStateFailOnlyTheStateCheck() {
        var result = validate(List.of(json(0,0,1,1), json(1,1,1,2), json(2,0,1,3), json(3,1,1,4)), 4, 2);
        assertEquals("validator.kafka.state-prefix.state-mismatch", result.reason());
        assertEquals(2, result.stateEvidence().mismatchCount().orElseThrow());
        var totals = result.evidence().defectTotals().orElseThrow();
        assertEquals(0, totals.duplicateCount() + totals.missingCount() + totals.unexpectedCount() + totals.malformedCount());
        var first = result.stateEvidence().mismatchSamples().getFirst();
        assertEquals(2, first.id());
        assertEquals(2, first.expectedCount());
        assertEquals(4, first.expectedSum());
        assertEquals(3, first.actualSum());
    }

    @Test void detectsWrongKeyCountAndSumIndividually() {
        for (String row : List.of(json(0,1,1,1), json(0,0,2,1), json(0,0,1,2)))
            assertEquals("validator.kafka.state-prefix.state-mismatch", validate(List.of(row),1,2).reason());
    }

    @Test void recordsAllDefectsWithStablePrecedence() {
        var result = validate(List.of(json(0,0,1,1), json(0,0,2,99), json(8,0,5,25), "{}"), 3, 2);
        assertEquals("validator.kafka.state-prefix.malformed-records", result.reason());
        var totals = result.evidence().defectTotals().orElseThrow();
        assertEquals(1, totals.malformedCount());
        assertEquals(1, totals.duplicateCount());
        assertEquals(1, totals.unexpectedCount());
        assertEquals(2, totals.missingCount());
        assertEquals(1, result.stateEvidence().mismatchCount().orElseThrow());
    }

    @Test void missingDuplicateAndUnexpectedIdsHaveDistinctReasons() {
        assertEquals("validator.kafka.state-prefix.missing-ids", validate(List.of(json(0,0,1,1)),2,2).reason());
        assertEquals("validator.kafka.state-prefix.duplicate-ids", validate(List.of(json(0,0,1,1),json(0,0,1,1)),2,2).reason());
        assertEquals("validator.kafka.state-prefix.unexpected-ids", validate(List.of(json(1,1,1,2)),1,2).reason());
    }

    @Test void outOfRangeStateFieldsAreMalformedBeforeIdAndStateClassification() {
        for (String row : List.of(json(-1, 0, 1, 1), json(Long.MIN_VALUE, 0, 1, 1),
                json(0, -1, 1, 1), json(0, 2, 1, 1), json(0, Long.MAX_VALUE, 1, 1),
                json(0, 0, 0, 1), json(0, 0, -1, 1), json(0, 0, 1, 0), json(0, 0, 1, Long.MIN_VALUE),
                json(99, 0, 0, 1))) {
            var result = validate(List.of(row), 1, 2);
            assertEquals("validator.kafka.state-prefix.malformed-records", result.reason(), row);
            var totals = result.evidence().defectTotals().orElseThrow();
            assertEquals(1, totals.malformedCount(), row);
            assertEquals(0, totals.unexpectedCount(), row);
            assertEquals(1, totals.missingCount(), row);
            assertEquals(0, result.stateEvidence().mismatchCount().orElseThrow(), row);
        }
        assertEquals("validator.kafka.state-prefix.unexpected-ids", validate(List.of(json(Long.MAX_VALUE, 0, 1, 1)), 1, 2).reason());
        assertEquals("validator.kafka.state-prefix.state-mismatch", validate(List.of(json(0, 0, Long.MAX_VALUE, Long.MAX_VALUE)), 1, 2).reason());
    }

    @Test void semanticJsonFormattingRemainsValidWithoutAByteCanonicalizationRequirement() {
        for (String row : List.of(" { \"prefixSum\":1, \"count\":1, \"key\":0, \"id\":0, \"version\":1 }\n",
                json(0, 0, 1, 1).replace("\"id\":0", "\"id\":-0"),
                json(0, 0, 1, 1).replace("\"id\"", "\"\\u0069d\""))) {
            assertEquals(KafkaIdSetValidationResult.Status.PASS, validate(List.of(row), 1, 2).status(), row);
        }
    }

    @Test void everyFieldRequiresAnIntegralSignedLongAndEscapedDuplicatesRemainMalformed() {
        String valid = json(0, 0, 1, 1);
        for (String field : List.of("version", "id", "key", "count", "prefixSum")) {
            long original = field.equals("id") || field.equals("key") ? 0 : 1;
            for (String invalid : List.of("true", "1.0", "\"1\"", "9223372036854775808", "-9223372036854775809")) {
                String row = valid.replace("\"" + field + "\":" + original, "\"" + field + "\":" + invalid);
                assertEquals("validator.kafka.state-prefix.malformed-records", validate(List.of(row), 1, 2).reason(), row);
            }
        }
        String escapedDuplicate = valid.replace("}", ",\"\\u0069d\":0}");
        assertEquals("validator.kafka.state-prefix.malformed-records", validate(List.of(escapedDuplicate), 1, 2).reason());
    }

    @Test void rejectsMalformedAndAmbiguousJson() {
        var correct = json(0,0,1,1);
        for (String row : List.of("", "null", "[]", "{", correct + "{}", correct.replace("\"version\":1", "\"version\":2"),
                correct.replace("\"id\":0", "\"id\":0,\"id\":0"), correct.replace("\"id\":0", "\"id\":0.0"),
                correct.replace("\"id\":0", "\"id\":\"0\""), correct.replace("\"id\":0", "\"id\":null"),
                correct.replace("\"id\":0", "\"id\":9223372036854775808"),
                correct.replace("\"key\":0", "\"unknown\":0"), correct.replace("}", ",\"unknown\":0}"))) {
            assertEquals("validator.kafka.state-prefix.malformed-records", validate(List.of(row),1,1).reason(), row);
        }
    }

    @Test void rejectsMalformedUtf8RatherThanAcceptingReplacementCharacters() {
        byte[] start = json(0,0,1,1).getBytes(StandardCharsets.UTF_8);
        byte[] malformed = java.util.Arrays.copyOf(start, start.length + 2);
        malformed[start.length] = (byte) 0xc3;
        malformed[start.length + 1] = (byte) 0x28;
        var result = validateBytes(java.util.Arrays.asList(malformed, null), 1,1);
        assertEquals("validator.kafka.state-prefix.malformed-records", result.reason());
        assertEquals(2, result.evidence().defectTotals().orElseThrow().malformedCount());
    }

    @Test void overSixtyFourCharactersArePreservedAndOversizedValidPrefixCannotPass() {
        String padded = json(0,0,1,1) + " ".repeat(80);
        assertTrue(padded.length() > 64);
        assertEquals(KafkaIdSetValidationResult.Status.PASS, validate(List.of(padded),1,1).status());
        String oversized = json(0,0,1,1) + " ".repeat(1024);
        var record = new KafkaStateTopicSnapshot.ObservedRecord(0,0,oversized.getBytes(StandardCharsets.UTF_8));
        assertEquals(1024, record.retainedBytes());
        assertTrue(record.originalSize() > 1024);
        assertEquals("validator.kafka.state-prefix.malformed-records", validate(List.of(oversized),1,1).reason());
    }

    @Test void boundsMismatchSamplesWithoutLosingTotals() {
        var rows = IntStream.range(0,150).mapToObj(id -> json(id,0,2,2)).toList();
        var result = validate(rows,150,1);
        assertEquals(150, result.stateEvidence().mismatchCount().orElseThrow());
        assertEquals(100, result.stateEvidence().mismatchSamples().size());
        assertEquals(99, result.stateEvidence().mismatchSamples().getLast().offset());
    }

    @Test void rejectsPartialOrUnorderedManifestBeforeReadingKafka() {
        var good = manifest(2);
        var partial = new KafkaInputManifest(good.clusterAlias(),good.topic(),good.totalRecords(),good.records(),
                good.beginningOffsets(),good.exclusiveEndOffsets(),KafkaInputManifest.EvidenceStatus.PARTIAL,good.reconciliation());
        var records = List.of(ack(0,1),ack(1,0));
        var unordered = new KafkaInputManifest("main","input",2,records,Map.of(0,0L),Map.of(0,2L),
                KafkaInputManifest.EvidenceStatus.COMPLETE,good.reconciliation());
        AtomicBoolean called = new AtomicBoolean();
        var validator = new KafkaStatePrefixValidator((b,t,d,n,bytes) -> { called.set(true); return snapshot(List.of()); });
        for (var input : List.of(partial, unordered)) {
            var result = validator.validate("kafka:9092","output",input,2,TIMEOUT);
            assertEquals("verification.kafka.input-manifest-invalid", result.reason());
            assertFalse(result.evidence().snapshotComplete());
        }
        assertFalse(called.get());
    }

    @Test void rejectsNoncanonicalInputPayloadHashBeforeReadingKafka() {
        var good = manifest(1);
        var acknowledgement = good.records().getFirst();
        var wrongPayload = new KafkaInputManifest.RecordAcknowledgement(0,"0".repeat(64),
                acknowledgement.producerAttempts(),0,0,acknowledgement.acknowledgementOutcome(),
                acknowledgement.terminalDisposition());
        var input = new KafkaInputManifest(good.clusterAlias(),good.topic(),1,List.of(wrongPayload),
                good.beginningOffsets(),good.exclusiveEndOffsets(),good.evidenceStatus(),good.reconciliation());
        var validator = new KafkaStatePrefixValidator((b,t,d,n,bytes) -> {
            fail("Invalid input must fail before Kafka access");
            return null;
        });
        assertEquals("verification.kafka.input-manifest-invalid",validator.validate("k","output",input,1,TIMEOUT).reason());
    }

    @Test void inputManifestCheckingUsesTheSameDeadline() {
        AtomicLong clock = new AtomicLong();
        var validator = new KafkaStatePrefixValidator((b,t,d,n,bytes) -> {
            fail("Expired manifest validation must not read Kafka");
            return null;
        },clock::getAndIncrement);
        var result = validator.validate("k","output",manifest(1),1,Duration.ofNanos(1));
        assertEquals("verification.kafka.incomplete-after-timeout",result.reason());
        assertFalse(result.evidence().snapshotComplete());
    }

    @Test void validatesKeyCountBounds() {
        var validator = new KafkaStatePrefixValidator((b,t,d,n,bytes) -> snapshot(List.of(json(0,0,1,1))));
        assertThrows(IllegalArgumentException.class, () -> validator.validate("k","output",manifest(1),0,TIMEOUT));
        assertThrows(IllegalArgumentException.class, () -> validator.validate("k","output",manifest(1),1025,TIMEOUT));
        assertEquals(KafkaIdSetValidationResult.Status.PASS,validator.validate("k","output",manifest(1),1024,TIMEOUT).status());
    }

    @Test void partialSnapshotDoesNotInventExactTotalsOrMissingIds() {
        var validator = new KafkaStatePrefixValidator((b,t,d,n,bytes) -> {
            throw new KafkaSnapshotException("verification.kafka.incomplete-after-timeout","incomplete", null,
                    new KafkaSnapshotException.PartialEvidence(Map.of(0,0L),Map.of(0,10L),3,
                            List.of(new KafkaTopicSnapshot.ObservedRecord(0,0,json(0,0,1,1)))));
        });
        var result = validator.validate("k","output",manifest(3),1,TIMEOUT);
        assertEquals("verification.kafka.incomplete-after-timeout",result.reason());
        assertEquals(3,result.evidence().observedCount());
        assertTrue(result.evidence().defectTotals().isEmpty());
        assertTrue(result.evidence().missingSamples().isEmpty());
        assertTrue(result.stateEvidence().mismatchCount().isEmpty());
    }

    @Test void readerAndComparisonShareOneDeadlineAndBound() {
        AtomicLong clock = new AtomicLong();
        AtomicBoolean called = new AtomicBoolean();
        var validator = new KafkaStatePrefixValidator((b,t,d,n,bytes) -> {
            assertEquals(1_000_000,n);
            assertEquals(128L * 1024 * 1024,bytes);
            called.set(true);
            clock.set(11);
            return snapshot(List.of(json(0,0,1,1)));
        },clock::get);
        var result = validator.validate("k","output",manifest(1),1,Duration.ofNanos(10));
        assertTrue(called.get());
        assertEquals("verification.kafka.incomplete-after-timeout",result.reason());
        assertFalse(result.evidence().snapshotComplete());
        assertTrue(result.stateEvidence().mismatchCount().isEmpty());
    }

    @Test void injectedReaderCannotBypassRecordOrByteBudget() {
        var record = new KafkaStateTopicSnapshot.ObservedRecord(0,0,
                ("[]" + " ".repeat(1022)).getBytes(StandardCharsets.UTF_8));
        var rows = Collections.nCopies(128 * 1024 + 1,record);
        var validator = new KafkaStatePrefixValidator((b,t,d,n,bytes) -> new KafkaStateTopicSnapshot(
                "output",Map.of(0,0L),Map.of(0,1L),rows));
        var result = validator.validate("k","output",manifest(1),1,TIMEOUT);
        assertEquals("verification.kafka.output-byte-limit-exceeded",result.reason());
        assertFalse(result.evidence().snapshotComplete());
    }

    @Test void resultCannotClaimPassFromIncompleteOrDefectiveEvidence() {
        assertThrows(IllegalArgumentException.class, () -> new KafkaStatePrefixValidationResult(
                KafkaIdSetValidationResult.Status.PASS,"validator.kafka.state-prefix.match","injected pass",
                KafkaIdSetValidationResult.Evidence.unavailable(1),
                new KafkaStatePrefixValidationResult.StateEvidence(1,OptionalLong.empty(),List.of())));
        for (var failed : List.of(validate(List.of(),1,1),
                validate(List.of("{}"),1,1), validate(List.of(json(1,0,1,1)),1,1),
                validate(List.of(json(0,0,1,1),json(0,0,1,1)),1,1),
                validate(List.of(json(0,0,2,2)),1,1))) {
            assertEquals(KafkaIdSetValidationResult.Status.FAIL,failed.status());
            assertThrows(IllegalArgumentException.class, () -> new KafkaStatePrefixValidationResult(
                    KafkaIdSetValidationResult.Status.PASS,"validator.kafka.state-prefix.match","injected pass",
                    failed.evidence(),failed.stateEvidence()),failed.reason());
        }
    }

    @Test void rawRecordRetainsDefensiveCopies() {
        byte[] bytes = {1,2};
        var record = new KafkaStateTopicSnapshot.ObservedRecord(0,0,bytes);
        bytes[0] = 3;
        record.bytes()[0] = 4;
        assertArrayEquals(new byte[]{1,2},record.bytes());
    }

    static KafkaInputManifest manifest(int total) {
        return new KafkaInputManifest("main","input",total,IntStream.range(0,total).mapToObj(id -> ack(id,id)).toList(),
                Map.of(0,0L),Map.of(0,(long)total),KafkaInputManifest.EvidenceStatus.COMPLETE,
                new KafkaInputManifest.ReconciliationEvidence(Map.of("isolation.level","read_uncommitted"),
                        IntStream.range(0,total).mapToObj(i -> (long)i).toList(),true));
    }

    private static KafkaInputManifest.RecordAcknowledgement ack(int id, int offset) {
        return new KafkaInputManifest.RecordAcknowledgement(id,Digests.sha256(Integer.toString(id).getBytes(StandardCharsets.UTF_8)),
                List.of(new KafkaInputManifest.ProducerAttempt(1,0,0,offset,KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED)),
                0,offset,KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED,KafkaInputManifest.TerminalDisposition.PRESENT);
    }

    static String json(long id,long key,long count,long sum) {
        return "{\"version\":1,\"id\":"+id+",\"key\":"+key+",\"count\":"+count+",\"prefixSum\":"+sum+"}";
    }

    private static KafkaStatePrefixValidationResult validate(List<String> rows,int total,int keys) {
        return validateBytes(rows.stream().map(row -> row.getBytes(StandardCharsets.UTF_8)).toList(),total,keys);
    }

    private static KafkaStatePrefixValidationResult validateBytes(List<byte[]> rows,int total,int keys) {
        var records = new ArrayList<KafkaStateTopicSnapshot.ObservedRecord>();
        for (int i=0;i<rows.size();i++) records.add(new KafkaStateTopicSnapshot.ObservedRecord(0,i,rows.get(i)));
        var snapshot = new KafkaStateTopicSnapshot("output",Map.of(0,0L),Map.of(0,(long)rows.size()),records);
        return new KafkaStatePrefixValidator((b,t,d,n,bytes) -> snapshot).validate("k","output",manifest(total),keys,TIMEOUT);
    }

    private static KafkaStateTopicSnapshot snapshot(List<String> rows) {
        var records = new ArrayList<KafkaStateTopicSnapshot.ObservedRecord>();
        for (int i=0;i<rows.size();i++) records.add(new KafkaStateTopicSnapshot.ObservedRecord(0,i,rows.get(i).getBytes(StandardCharsets.UTF_8)));
        return new KafkaStateTopicSnapshot("output",Map.of(0,0L),Map.of(0,(long)rows.size()),records);
    }
}
