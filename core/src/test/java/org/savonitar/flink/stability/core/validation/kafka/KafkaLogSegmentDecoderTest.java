package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Crc32C;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KafkaLogSegmentDecoderTest {
    private final KafkaLogSegmentDecoder decoder = new KafkaLogSegmentDecoder();

    @Test
    void preservesPhysicalCoordinatesAndBothMarkersAcrossProducerEpochs() {
        MemoryRecords first = data(11, 127, (short) 7, 3, true, Compression.none().build(),
                new SimpleRecord(21, utf8("key"), utf8("100")), new SimpleRecord(22, utf8("101")));
        MemoryRecords commit = marker(13, (short) 7, ControlRecordType.COMMIT);
        MemoryRecords second = data(14, 127, (short) 8, 0, true, Compression.none().build(),
                new SimpleRecord(23, utf8("102")));
        MemoryRecords abort = marker(15, (short) 8, ControlRecordType.ABORT);
        byte[] bytes = concat(first, commit, second, abort);

        var result = decoder.decode(bytes, 5);

        assertEquals(5, result.recordCount());
        assertEquals(4, result.batches().size());
        var firstBatch = result.batches().getFirst();
        assertEquals(0, firstBatch.fileByteOffset());
        assertEquals(first.sizeInBytes(), firstBatch.sizeBytes());
        assertEquals(RecordBatch.MAGIC_VALUE_V2, firstBatch.magic());
        assertEquals(11, firstBatch.baseOffset());
        assertEquals(12, firstBatch.lastOffset());
        assertEquals(127, firstBatch.producerId());
        assertEquals(7, firstBatch.producerEpoch());
        assertEquals(3, firstBatch.baseSequence());
        assertEquals(4, firstBatch.lastSequence());
        assertEquals(9, firstBatch.leaderEpoch());
        assertTrue(firstBatch.transactional());
        assertFalse(firstBatch.control());
        assertEquals("NONE", firstBatch.compression());
        assertEquals(List.of(100L, 101L), firstBatch.records().stream().map(KafkaLogSegmentDecoder.Entry::canonicalId).toList());
        assertEquals(new KafkaLogSegmentDecoder.Entry(11, 3, 21, base64(utf8("key")),
                base64(utf8("100")), 100L, null), firstBatch.records().getFirst());
        long position = 0;
        for (var batch : result.batches()) {
            assertEquals(position, batch.fileByteOffset());
            position += batch.sizeBytes();
        }
        assertEquals(bytes.length, position);
        assertEquals(8, result.batches().get(2).producerEpoch());
        assertEquals(127, result.batches().get(2).producerId());
        for (int index : List.of(1, 3)) {
            var batch = result.batches().get(index);
            assertTrue(batch.transactional());
            assertTrue(batch.control());
            assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
            var record = batch.records().getFirst();
            assertNull(record.canonicalId());
            assertEquals(new KafkaLogSegmentDecoder.Marker(index == 1 ? "COMMIT" : "ABORT", 19), record.marker());
            assertNotNull(record.keyBase64());
            assertNotNull(record.valueBase64());
        }
        assertThrows(UnsupportedOperationException.class, () -> result.batches().clear());
        assertThrows(UnsupportedOperationException.class, () -> firstBatch.records().clear());
    }

    @Test
    void absenceOfAMarkerDoesNotInventVisibilityAndRawNoncanonicalValuesRemainEvidence() {
        byte[][] values = {utf8("01"), new byte[0], new byte[]{(byte) 0xff},
                utf8(Long.toString(Long.MIN_VALUE)), null, utf8("9223372036854775808")};
        SimpleRecord[] records = Arrays.stream(values).map(value -> new SimpleRecord(5, new byte[0], value))
                .toArray(SimpleRecord[]::new);

        var batch = decoder.decode(concat(data(0, 127, (short) 3, 0, true,
                Compression.none().build(), records)), records.length).batches().getFirst();

        for (int index = 0; index < values.length; index++) {
            var decoded = batch.records().get(index);
            assertEquals("", decoded.keyBase64());
            assertEquals(base64(values[index]), decoded.valueBase64());
            assertNull(decoded.marker());
            assertEquals(index == 3 ? Long.MIN_VALUE : null, decoded.canonicalId());
        }
        assertTrue(batch.transactional());
        assertFalse(batch.control());
    }

    @Test
    void compactedOffsetGapsRemainPhysicalFacts() {
        MemoryRecordsBuilder builder = builder(100, 127, (short) 2, 5, true, false, Compression.none().build());
        builder.appendWithOffset(101, new SimpleRecord(1, utf8("1")));
        builder.appendWithOffset(103, new SimpleRecord(2, utf8("2")));

        var batch = decoder.decode(concat(builder.build()), 2).batches().getFirst();

        assertEquals(100, batch.baseOffset());
        assertEquals(103, batch.lastOffset());
        assertEquals(List.of(101L, 103L), batch.records().stream().map(KafkaLogSegmentDecoder.Entry::offset).toList());
        assertEquals(List.of(6, 8), batch.records().stream().map(KafkaLogSegmentDecoder.Entry::sequence).toList());
    }

    @Test
    void rejectsEveryTruncationAndTrailingBytesInsteadOfReturningAValidPrefix() {
        byte[] valid = concat(ordinary(0, "1", "2"));
        for (int length = 1; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertThrows(IllegalArgumentException.class, () -> decoder.decode(truncated, 2), "length=" + length);
        }
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(Arrays.copyOf(valid, valid.length + 1), 2));
        byte[] two = concat(ordinary(0, "1"), ordinary(1, "2"));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(Arrays.copyOf(two, two.length - 1), 2));
    }

    @Test
    void emptyFilesAndCompactedEmptyBatchesDoNotAssertPartitionVisibility() {
        assertEquals(new KafkaLogSegmentDecoder.Segment(List.of(), 0), decoder.decode(new byte[0], 1));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(new byte[0], 0));
        var compacted = decoder.decode(emptyBatches(1), 1);
        assertEquals(0, compacted.recordCount());
        assertEquals(1, compacted.batches().size());
        assertEquals(List.of(), compacted.batches().getFirst().records());
    }

    @Test
    void boundsEmptyBatchObjectAmplificationAndPerBatchRecordAllocation() {
        byte[] tooMany = emptyBatches(KafkaLogSegmentDecoder.MAX_BATCHES + 1);
        assertTrue(tooMany.length < KafkaLogSegmentDecoder.MAX_SEGMENT_BYTES);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> decoder.decode(tooMany, 1))
                .getMessage().contains("Batch count limit"));
        MemoryRecords oversized = data(0, -1, (short) -1, -1, false, Compression.none().build(),
                new SimpleRecord(1, new byte[KafkaLogSegmentDecoder.MAX_BATCH_BYTES]));
        byte[] bytes = concat(oversized);
        assertTrue(bytes.length < KafkaLogSegmentDecoder.MAX_SEGMENT_BYTES);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> decoder.decode(bytes, 1))
                .getMessage().contains("Batch byte limit"));
    }

    @Test
    void rejectsCrcCorruptionEvenWhenTheRecordStillLooksLikeAnId() {
        byte[] corrupt = concat(ordinary(0, "1"));
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(corrupt, 1));
    }

    @Test
    void rejectsAValidCrcWithAFalseRecordCountAndInvalidOffsetRanges() {
        byte[] malformed = concat(ordinary(0, "1"));
        ByteBuffer buffer = ByteBuffer.wrap(malformed);
        buffer.putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, 0);
        refreshCrc(malformed);
        MemoryRecords.readableRecords(buffer).batches().iterator().next().ensureValid();
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(malformed, 1));

        MemoryRecords negative = ordinary(0, "1");
        negative.batches().iterator().next().setLastOffset(-1);
        byte[] negativeBytes = concat(negative);
        byte[] overlapping = concat(ordinary(1, "1"), ordinary(1, "2"));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(negativeBytes, 1));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(overlapping, 2));
    }

    @Test
    void rejectsLimitsAndUnsupportedCompressionOrLegacyMagic() {
        byte[] bytes = concat(ordinary(0, "1", "2"));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(bytes, 1));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(bytes, 0));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(bytes, KafkaLogSegmentDecoder.MAX_RECORDS + 1));
        assertThrows(IllegalArgumentException.class,
                () -> decoder.decode(new byte[KafkaLogSegmentDecoder.MAX_SEGMENT_BYTES + 1], 1));
        byte[] gzip = concat(data(0, -1, (short) -1, -1, false, Compression.gzip().build(), new SimpleRecord(utf8("1"))));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> decoder.decode(gzip, 1))
                .getMessage().contains("compression"));
        var legacy = MemoryRecords.builder(ByteBuffer.allocate(1024), RecordBatch.MAGIC_VALUE_V1,
                Compression.none().build(), TimestampType.CREATE_TIME, 0);
        legacy.append(new SimpleRecord(1, utf8("1")));
        byte[] legacyBytes = concat(legacy.build());
        assertTrue(assertThrows(IllegalArgumentException.class, () -> decoder.decode(legacyBytes, 1))
                .getMessage().contains("magic"));
    }

    @Test
    void rejectsUnknownControlTypesAndMalformedOrFutureMarkerEncodings() {
        EndTransactionMarker commit = new EndTransactionMarker(ControlRecordType.COMMIT, 3);
        ByteBuffer goodKey = serialize(ControlRecordType.COMMIT.recordKey());
        ByteBuffer unknown = serialize(ControlRecordType.COMMIT.recordKey().set("type", (short) 123));
        ByteBuffer future = serialize(ControlRecordType.COMMIT.recordKey().set("version", (short) 1));
        ByteBuffer other = serialize(ControlRecordType.LEADER_CHANGE.recordKey());
        for (ByteBuffer key : List.of(unknown, future, other)) {
            byte[] bytes = replaceMarkerKey(key);
            assertRejectedMarker(bytes, key == future ? "Unsupported transaction marker encoding"
                    : "Unsupported control record");
        }
        ByteBuffer futureValue = commit.serializeValue();
        futureValue.putShort(0, (short) 1);
        byte[] extraValue = Arrays.copyOf(array(commit.serializeValue()), commit.serializeValue().remaining() + 1);
        for (ByteBuffer value : List.of(ByteBuffer.allocate(0), futureValue, ByteBuffer.wrap(extraValue))) {
            byte[] bytes = concat(control(goodKey, value));
            assertRejectedMarker(bytes, value.remaining() == 0 ? "Invalid Kafka batch encoding"
                    : "Unsupported transaction marker encoding");
        }
        // A null control key is not covered: Kafka validates keys while appending, and replacing
        // its length with null would require a separate wire-format mutation beyond this fixture.
        byte[] noValue = concat(control(goodKey, null));
        assertRejectedMarker(noValue, "Malformed transaction marker");
    }

    private void assertRejectedMarker(byte[] bytes, String reason) {
        MemoryRecords.readableRecords(ByteBuffer.wrap(bytes)).batches().iterator().next().ensureValid();
        assertTrue(assertThrows(IllegalArgumentException.class, () -> decoder.decode(bytes, 1))
                .getMessage().contains(reason));
    }

    private static byte[] replaceMarkerKey(ByteBuffer replacement) {
        byte[] bytes = concat(marker(0, (short) 0, ControlRecordType.COMMIT));
        var batch = MemoryRecords.readableRecords(ByteBuffer.wrap(bytes)).batches().iterator().next();
        ByteBuffer key = batch.iterator().next().key();
        assertEquals(key.remaining(), replacement.remaining());
        key.put(replacement.duplicate());
        refreshCrc(bytes);
        batch.ensureValid();
        assertEquals(replacement, batch.iterator().next().key());
        return bytes;
    }

    private static void refreshCrc(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int crcStart = DefaultRecordBatch.CRC_OFFSET + Integer.BYTES;
        buffer.putInt(DefaultRecordBatch.CRC_OFFSET, (int) Crc32C.compute(buffer, crcStart, buffer.limit() - crcStart));
    }

    private static MemoryRecords ordinary(long offset, String... values) {
        return data(offset, -1, (short) -1, -1, false, Compression.none().build(),
                Arrays.stream(values).map(value -> new SimpleRecord(1, utf8(value))).toArray(SimpleRecord[]::new));
    }

    private static MemoryRecords data(long offset, long producer, short epoch, int sequence,
                                      boolean transactional, Compression compression, SimpleRecord... records) {
        var builder = builder(offset, producer, epoch, sequence, transactional, false, compression);
        for (int index = 0; index < records.length; index++) builder.appendWithOffset(offset + index, records[index]);
        return builder.build();
    }

    private static MemoryRecordsBuilder builder(long offset, long producer, short epoch, int sequence,
                                                boolean transactional, boolean control, Compression compression) {
        return MemoryRecords.builder(ByteBuffer.allocate(8192), RecordBatch.MAGIC_VALUE_V2, compression,
                TimestampType.CREATE_TIME, offset, RecordBatch.NO_TIMESTAMP, producer, epoch, sequence,
                transactional, control, 9);
    }

    private static MemoryRecords marker(long offset, short epoch, ControlRecordType type) {
        return MemoryRecords.withEndTransactionMarker(offset, 25, 9, 127, epoch, new EndTransactionMarker(type, 19));
    }

    private static MemoryRecords control(ByteBuffer key, ByteBuffer value) {
        var builder = builder(0, 127, (short) 0, RecordBatch.NO_SEQUENCE, true, true, Compression.none().build());
        builder.appendControlRecordWithOffset(0, new SimpleRecord(1, key, value));
        return builder.build();
    }

    private static byte[] emptyBatches(int count) {
        ByteBuffer result = ByteBuffer.allocate(count * DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        for (int index = 0; index < count; index++) {
            ByteBuffer batch = result.slice();
            batch.limit(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
            DefaultRecordBatch.writeEmptyHeader(batch, RecordBatch.MAGIC_VALUE_V2, 127, (short) 0,
                    index, index, index, 9, TimestampType.CREATE_TIME, 1, true, false);
            result.position(result.position() + DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        }
        return result.array();
    }

    private static ByteBuffer serialize(Struct value) {
        ByteBuffer buffer = ByteBuffer.allocate(value.sizeOf());
        value.writeTo(buffer);
        return buffer.flip();
    }

    private static byte[] concat(MemoryRecords... inputs) {
        ByteBuffer buffer = ByteBuffer.allocate(Arrays.stream(inputs).mapToInt(MemoryRecords::sizeInBytes).sum());
        for (var input : inputs) buffer.put(input.buffer().duplicate());
        return buffer.array();
    }

    private static byte[] array(ByteBuffer value) {
        byte[] result = new byte[value.remaining()];
        value.duplicate().get(result);
        return result;
    }

    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String base64(byte[] value) { return value == null ? null : Base64.getEncoder().encodeToString(value); }
}
