package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.savonitar.flink.stability.core.validation.kafka.KafkaTransactionStateDecoder.Status;

/** Actual Kafka generated messages and physical segments only; no broker, socket or filesystem write. */
public final class KafkaTransactionStateDecoderTest {
    private static final KafkaTransactionStateDecoder DECODER = new KafkaTransactionStateDecoder();
    private static final KafkaTransactionStateDecoder.Limits LIMITS = KafkaTransactionStateDecoder.Limits.bounded();
    private static final String ID = "txn-\u0394-\uD83D\uDE80";
    private static int checks;

    @org.junit.jupiter.api.Test
    void generatedVersionsAndNullableFields() {
        for (short version : new short[]{0, 1}) {
            var source = value(version);
            byte[] key = key(), bytes = encode(version, source);
            var result = decode(key, bytes);
            eq(Status.DECODED, result.status()); check(result.knownSemantics());
            eq((short) 0, result.keyVersion()); eq(version, result.valueVersion()); eq(ID, result.transactionalId());
            eq(key.length, result.key().length()); eq(bytes.length, result.value().length());
            eq(sha(key), result.key().sha256()); eq(sha(bytes), result.value().sha256());
            var fields = result.decoded(); eq(17L, fields.producerId()); eq((short) 3, fields.producerEpoch());
            eq(2000, fields.timeoutMs()); eq((byte) 1, fields.stateId()); eq("Ongoing", fields.stateName());
            eq(1234L, fields.startTimestampMs()); eq(1290L, fields.lastUpdateTimestampMs());
            eq(version == 0 ? -1L : 11L, fields.previousProducerId());
            eq(version == 0 ? -1L : 19L, fields.nextProducerId());
            eq(version == 0 ? (short) 0 : (short) 2, fields.clientTransactionVersion());
            eq("output", fields.partitions().getFirst().topic());
            eq(List.of(0, 2, 2), fields.partitions().getFirst().ids()); // Preserve order/duplicates, do not turn into a set.
            var nullable = decode(key, encode(version, value(version).setTransactionPartitions(null)));
            eq(Status.DECODED, nullable.status()); eq(null, nullable.decoded().partitions());
            var empty = decode(key, encode(version, value(version).setTransactionPartitions(List.of())));
            eq(Status.DECODED, empty.status()); eq(List.of(), empty.decoded().partitions());
        }
        List<String> states = List.of("Empty", "Ongoing", "PrepareCommit", "PrepareAbort", "CompleteCommit",
                "CompleteAbort", "Dead", "PrepareEpochFence");
        for (byte state = 0; state < states.size(); state++) {
            var result = decode(key(), encode((short) 1, value((short) 1).setTransactionStatus(state)));
            eq(Status.DECODED, result.status()); eq(states.get(state), result.decoded().stateName());
        }
        var unknown = decode(key(), encode((short) 1, value((short) 1).setTransactionStatus((byte) 99)));
        eq(Status.UNSUPPORTED_SEMANTICS, unknown.status()); eq((byte) 99, unknown.decoded().stateId());
        eq("UNKNOWN", unknown.decoded().stateName()); check(!unknown.knownSemantics());
        var future = decode(key(), encode((short) 1, value((short) 1).setClientTransactionVersion((short) 9)));
        eq(Status.UNSUPPORTED_SEMANTICS, future.status()); eq((short) 9, future.decoded().clientTransactionVersion());
    }

    @org.junit.jupiter.api.Test
    void unknownTagsAndImmutableEvidence() {
        var source = value((short) 1);
        source.unknownTaggedFields().add(new RawTaggedField(7, new byte[]{1, 2, 3}));
        source.unknownTaggedFields().add(new RawTaggedField(9, new byte[0]));
        source.transactionPartitions().getFirst().unknownTaggedFields().add(new RawTaggedField(4, new byte[]{8, 9}));
        byte[] key = key(), bytes = encode((short) 1, source);
        var result = decode(key, bytes); eq(Status.UNSUPPORTED_SEMANTICS, result.status()); check(!result.knownSemantics());
        check(result.decoded().hasUnknownTags());
        eq(List.of(7, 9), result.decoded().unknownTags().stream().map(KafkaTransactionStateDecoder.Tag::id).toList());
        var tag = result.decoded().unknownTags().getFirst(); eq(3, tag.length()); eq(sha(new byte[]{1, 2, 3}), tag.sha256());
        eq("AQID", tag.base64()); eq("", result.decoded().unknownTags().get(1).base64());
        eq(4, result.decoded().partitions().getFirst().unknownTags().getFirst().id());
        String keyHash = result.key().sha256(), valueHash = result.value().sha256();
        Arrays.fill(key, (byte) 0); Arrays.fill(bytes, (byte) 0);
        source.unknownTaggedFields().getFirst().data()[0] = 99;
        source.transactionPartitions().clear();
        eq(keyHash, result.key().sha256()); eq(valueHash, result.value().sha256()); eq("AQID", tag.base64());
        unsupported(() -> result.decoded().unknownTags().clear());
        unsupported(() -> result.decoded().partitions().clear());
        unsupported(() -> result.decoded().partitions().getFirst().ids().clear());
        unsupported(() -> result.decoded().partitions().getFirst().unknownTags().clear());
    }

    @org.junit.jupiter.api.Test
    void tombstonesAndUnsupportedVersions() {
        var tombstone = decode(key(), null); eq(Status.TOMBSTONE, tombstone.status());
        eq(ID, tombstone.transactionalId()); eq(-1, tombstone.value().length());
        eq(null, tombstone.value().sha256()); eq(null, tombstone.valueVersion()); eq(null, tombstone.decoded());
        check(!tombstone.knownSemantics());
        var empty = decode(key(), new byte[0]); eq(Status.MALFORMED, empty.status());
        eq(0, empty.value().length()); eq(sha(new byte[0]), empty.value().sha256());
        eq(Status.MALFORMED, decode(null, null).status());
        for (short version : new short[]{-1, 1, 2, 32767}) {
            byte[] key = key(); ByteBuffer.wrap(key).putShort(version);
            var unknown = decode(key, null); eq(Status.UNSUPPORTED_VERSION, unknown.status());
            eq(version, unknown.keyVersion()); eq(null, unknown.transactionalId());
        }
        for (short version : new short[]{-1, 2, 32767}) {
            byte[] bytes = encode((short) 1, value((short) 1)); ByteBuffer.wrap(bytes).putShort(version);
            var unknown = decode(key(), bytes); eq(Status.UNSUPPORTED_VERSION, unknown.status());
            eq(version, unknown.valueVersion()); eq(ID, unknown.transactionalId()); eq(null, unknown.decoded());
        }
    }

    @org.junit.jupiter.api.Test
    void malformedAndNoncanonicalEncodings() {
        byte[] key = key(), bytes = encode((short) 1, value((short) 1));
        for (int size = 0; size < key.length; size++) eq(Status.MALFORMED, decode(Arrays.copyOf(key, size), bytes).status());
        for (int size = 0; size < bytes.length; size++) eq(Status.MALFORMED, decode(key, Arrays.copyOf(bytes, size)).status());
        eq(Status.UNSUPPORTED_ENCODING, decode(Arrays.copyOf(key, key.length + 1), bytes).status());
        eq(Status.UNSUPPORTED_ENCODING, decode(key, Arrays.copyOf(bytes, bytes.length + 1)).status());

        // A real generated known-tag value, mutated only at its declared size. Kafka's generated
        // reader consumes the long; our canonical gate must reject the differing framing.
        byte[] tagged = encode((short) 1, value((short) 0).setTransactionPartitions(null).setPreviousProducerId(11L));
        int sizeIndex = tagged.length - 9;
        eq((byte) 1, tagged[sizeIndex - 2]); eq((byte) 0, tagged[sizeIndex - 1]); eq((byte) 8, tagged[sizeIndex]);
        tagged[sizeIndex] = 7;
        var inconsistent = decode(key, tagged); eq(Status.UNSUPPORTED_ENCODING, inconsistent.status());
        check(inconsistent.detail().contains("canonical")); check(!inconsistent.knownSemantics());

        // Unknown-tag allocation must reject a huge advertised length from a tiny actual record.
        var withUnknown = value((short) 0).setTransactionPartitions(null);
        withUnknown.unknownTaggedFields().add(new RawTaggedField(7, new byte[0]));
        byte[] canonical = encode((short) 1, withUnknown);
        eq((byte) 0, canonical[canonical.length - 1]); eq((byte) 7, canonical[canonical.length - 2]);
        ByteBuffer huge = ByteBuffer.allocate(5); ByteUtils.writeUnsignedVarint(Integer.MAX_VALUE, huge); huge.flip();
        byte[] bad = Arrays.copyOf(canonical, canonical.length - 1 + huge.remaining());
        huge.get(bad, canonical.length - 1, huge.remaining());
        eq(Status.MALFORMED, decode(key, bad).status());

        withUnknown.unknownTaggedFields().add(new RawTaggedField(9, new byte[0]));
        byte[] repeatedTag = encode((short) 1, withUnknown);
        eq((byte) 7, repeatedTag[repeatedTag.length - 4]);
        eq((byte) 9, repeatedTag[repeatedTag.length - 2]);
        repeatedTag[repeatedTag.length - 2] = 7;
        eq(Status.UNSUPPORTED_ENCODING, decode(key, repeatedTag).status());

        byte[] invalidUtf8 = MessageUtil.toVersionPrefixedBytes((short) 0, new TransactionLogKey().setTransactionalId("x"));
        invalidUtf8[invalidUtf8.length - 1] = (byte) 0x80;
        eq(Status.UNSUPPORTED_ENCODING, decode(invalidUtf8, null).status());
    }

    @org.junit.jupiter.api.Test
    void retainedObjectAndByteLimits() {
        byte[] key = key(), bytes = encode((short) 1, value((short) 1));
        var exact = new KafkaTransactionStateDecoder.Limits(key.length + bytes.length, 1, 3, 1, 1);
        eq(Status.DECODED, DECODER.decode(key, bytes, exact, deadline()).status());
        var small = new KafkaTransactionStateDecoder.Limits(key.length + bytes.length - 1, 1, 3, 1, 1);
        var exceeded = DECODER.decode(key, bytes, small, deadline()); eq(Status.LIMIT, exceeded.status());
        eq(null, exceeded.key().sha256()); eq(null, exceeded.value().sha256());
        eq(Status.LIMIT, DECODER.decode(new byte[KafkaTransactionStateDecoder.MAX_RECORD_BYTES + 1], null, LIMITS, deadline()).status());
        var partitions = new KafkaTransactionStateDecoder.Limits(65536, 1, 2, 1, 1);
        eq(Status.LIMIT, DECODER.decode(key, bytes, partitions, deadline()).status());
        var twoTopics = value((short) 1).setTransactionPartitions(List.of(partitions("a", 0), partitions("b", 1)));
        eq(Status.LIMIT, DECODER.decode(key, encode((short) 1, twoTopics), exactTopics(), deadline()).status());
        var tags = value((short) 0);
        tags.transactionPartitions().getFirst().unknownTaggedFields().add(new RawTaggedField(1, new byte[]{4}));
        tags.unknownTaggedFields().add(new RawTaggedField(7, new byte[]{5}));
        eq(Status.LIMIT, DECODER.decode(key, encode((short) 1, tags),
                new KafkaTransactionStateDecoder.Limits(65536, 1, 4, 1, 100), deadline()).status());
        eq(Status.LIMIT, DECODER.decode(key, encode((short) 1, tags),
                new KafkaTransactionStateDecoder.Limits(65536, 1, 4, 2, 1), deadline()).status());
        illegal(() -> new KafkaTransactionStateDecoder.Limits(65537, 1, 1, 1, 1));
        illegal(() -> DECODER.decode(key, bytes, LIMITS, MonotonicDeadline.start(Duration.ofSeconds(61), () -> 0L)));
    }

    @org.junit.jupiter.api.Test
    void actualSegmentEndToEnd() {
        byte[] key = key(), value = encode((short) 1, value((short) 1));
        MemoryRecords memory = MemoryRecords.withRecords(Compression.none().build(),
                new SimpleRecord(55L, key, value), new SimpleRecord(56L, key, null));
        ByteBuffer buffer = memory.buffer().duplicate(); byte[] bytes = new byte[buffer.remaining()]; buffer.get(bytes);
        var segment = new KafkaLogSegmentDecoder().decode(bytes, 10);
        eq(2L, segment.recordCount()); eq(1, segment.batches().size());
        var batch = segment.batches().getFirst(); check(!batch.control());
        var row = batch.records().getFirst(); eq(0L, row.offset()); eq(55L, row.timestamp());
        var result = decode(Base64.getDecoder().decode(row.keyBase64()), Base64.getDecoder().decode(row.valueBase64()));
        eq(Status.DECODED, result.status()); eq(ID, result.transactionalId());
        eq(17L, result.decoded().producerId()); check(batch.producerId() != result.decoded().producerId());
        eq(sha(value), result.value().sha256());
        var tombstone = batch.records().get(1); eq(null, tombstone.valueBase64()); eq(1L, tombstone.offset());
        eq(Status.TOMBSTONE, decode(Base64.getDecoder().decode(tombstone.keyBase64()), null).status());
    }

    @org.junit.jupiter.api.Test
    void sharedDeadlineAndInterruption() {
        byte[] key = key(), bytes = encode((short) 1, value((short) 1));
        AtomicLong clock = new AtomicLong(); var shared = MonotonicDeadline.start(Duration.ofSeconds(1), clock::get);
        eq(Status.DECODED, DECODER.decode(key, bytes, LIMITS, shared).status());
        clock.set(Duration.ofSeconds(1).toNanos()); eq(Status.DEADLINE, DECODER.decode(key, bytes, LIMITS, shared).status());
        AtomicInteger observed = new AtomicInteger();
        var first = MonotonicDeadline.start(Duration.ofSeconds(1), () -> { observed.incrementAndGet(); return 0L; });
        eq(Status.DECODED, DECODER.decode(key, bytes, LIMITS, first).status());
        int finalCheck = observed.get(); AtomicInteger lateCalls = new AtomicInteger();
        var late = MonotonicDeadline.start(Duration.ofSeconds(1),
                () -> lateCalls.incrementAndGet() >= finalCheck ? Duration.ofSeconds(1).toNanos() : 0L);
        eq(Status.DEADLINE, DECODER.decode(key, bytes, LIMITS, late).status());
        Thread.currentThread().interrupt();
        try { eq(Status.INTERRUPTED, DECODER.decode(key, bytes, LIMITS, deadline()).status()); check(Thread.currentThread().isInterrupted()); }
        finally { Thread.interrupted(); }
    }

    private static KafkaTransactionStateDecoder.Limits exactTopics() { return new KafkaTransactionStateDecoder.Limits(65536, 1, 4, 1, 1); }
    private static KafkaTransactionStateDecoder.Result decode(byte[] key, byte[] value) { return DECODER.decode(key, value, LIMITS, deadline()); }
    private static MonotonicDeadline deadline() { return MonotonicDeadline.start(Duration.ofSeconds(1), () -> 0L); }
    private static byte[] key() { return MessageUtil.toVersionPrefixedBytes((short) 0, new TransactionLogKey().setTransactionalId(ID)); }
    private static byte[] encode(short version, TransactionLogValue value) { return MessageUtil.toVersionPrefixedBytes(version, value); }
    private static TransactionLogValue value(short version) {
        var value = new TransactionLogValue().setProducerId(17L).setProducerEpoch((short) 3)
                .setTransactionTimeoutMs(2000).setTransactionStatus((byte) 1)
                .setTransactionPartitions(new ArrayList<>(List.of(partitions("output", 0, 2, 2))))
                .setTransactionStartTimestampMs(1234L).setTransactionLastUpdateTimestampMs(1290L);
        if (version == 1) value.setPreviousProducerId(11L).setNextProducerId(19L).setClientTransactionVersion((short) 2);
        return value;
    }
    private static TransactionLogValue.PartitionsSchema partitions(String topic, Integer... ids) {
        return new TransactionLogValue.PartitionsSchema().setTopic(topic).setPartitionIds(List.of(ids));
    }
    private static String sha(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception impossible) { throw new AssertionError(impossible); }
    }
    private static void eq(Object expected, Object actual) { checks++; if (!Objects.equals(expected, actual)) throw new AssertionError("Expected " + expected + " but got " + actual); }
    private static void check(boolean condition) { checks++; if (!condition) throw new AssertionError("Contract check failed"); }
    private static void unsupported(Runnable action) { checks++; try { action.run(); } catch (UnsupportedOperationException expected) { return; } throw new AssertionError("Mutable result"); }
    private static void illegal(Runnable action) { checks++; try { action.run(); } catch (IllegalArgumentException expected) { return; } throw new AssertionError("Missing input validation"); }
}
