package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.CompressionType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Offline physical record evidence; this does not infer transaction visibility or ownership. */
public final class KafkaLogSegmentDecoder {
    public static final int MAX_SEGMENT_BYTES = 64 * 1024 * 1024;
    public static final int MAX_BATCH_BYTES = 1024 * 1024;
    public static final int MAX_BATCHES = 100_000;
    public static final int MAX_RECORDS = 100_000;

    public record Segment(List<Batch> batches, long recordCount) {
        public Segment { batches = List.copyOf(batches); }
    }

    public record Batch(long fileByteOffset, int sizeBytes, byte magic, long baseOffset,
                        long lastOffset, long producerId, short producerEpoch, int baseSequence,
                        int lastSequence, int leaderEpoch, boolean transactional, boolean control,
                        String compression, List<Entry> records) {
        public Batch { records = List.copyOf(records); }
    }

    /** Null key/value remain tombstones/absent keys; a null canonicalId does not discard payload. */
    public record Entry(long offset, int sequence, long timestamp, String keyBase64,
                        String valueBase64, Long canonicalId, Marker marker) {}

    public record Marker(String type, int coordinatorEpoch) {}

    /**
     * Decodes an exact, complete segment of magic-2 uncompressed batches. Any rejection throws;
     * a successfully decoded prefix is never returned as a complete segment.
     */
    public Segment decode(byte[] segment, int maximumRecords) {
        if (segment == null || segment.length > MAX_SEGMENT_BYTES) {
            throw new IllegalArgumentException("Segment must be present and at most " + MAX_SEGMENT_BYTES + " bytes");
        }
        if (maximumRecords < 1 || maximumRecords > MAX_RECORDS) {
            throw new IllegalArgumentException("Maximum records must be between 1 and " + MAX_RECORDS);
        }
        if (segment.length == 0) return new Segment(List.of(), 0);
        int position = 0;
        try {
            MemoryRecords memory = MemoryRecords.readableRecords(ByteBuffer.wrap(segment).asReadOnlyBuffer());
            // Kafka's batch iterator deliberately tolerates an incomplete trailing batch.
            if (memory.validBytes() != segment.length) {
                throw rejected(memory.validBytes(), "Truncated batch or trailing bytes");
            }
            List<Batch> batches = new ArrayList<>();
            long recordCount = 0;
            long previousLastOffset = -1;
            for (var batch : memory.batches()) {
                // Compaction can retain empty batches, which do not consume the record budget.
                if (batches.size() >= MAX_BATCHES) throw rejected(position, "Batch count limit exceeded");
                if (batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                    throw rejected(position, "Unsupported record magic; only magic 2 is supported");
                }
                int size = batch.sizeInBytes();
                if (size < DefaultRecordBatch.RECORD_BATCH_OVERHEAD || size > segment.length - position) {
                    throw rejected(position, "Invalid batch size");
                }
                // Bound transient Kafka record/header allocations as well as the total source bytes.
                if (size > MAX_BATCH_BYTES) throw rejected(position, "Batch byte limit exceeded");
                batch.ensureValid();
                // Reject before obtaining an iterator: never decompress untrusted bytes here.
                if (batch.compressionType() != CompressionType.NONE) {
                    throw rejected(position, "Unsupported compression; only NONE is supported");
                }
                if (batch.baseOffset() < 0 || batch.lastOffset() < batch.baseOffset()
                        || batch.baseOffset() <= previousLastOffset) {
                    throw rejected(position, "Invalid or overlapping batch offsets");
                }
                Integer declaredCount = batch.countOrNull();
                if (declaredCount == null || declaredCount < 0) {
                    throw rejected(position, "Invalid batch record count");
                }
                if (declaredCount > maximumRecords - recordCount) {
                    throw rejected(position, "Record budget exceeded");
                }
                if (batch.isControlBatch() && (!batch.isTransactional() || declaredCount != 1
                        || batch.producerId() < 0 || batch.producerEpoch() < 0)) {
                    throw rejected(position, "Expected one transactional control marker with a producer identity");
                }
                List<Entry> entries = new ArrayList<>();
                long previousOffset = -1;
                long recordBytes = 0;
                for (var record : batch) {
                    if (entries.size() >= declaredCount || recordCount + entries.size() >= maximumRecords) {
                        throw rejected(position, "Actual record count exceeds its header or budget");
                    }
                    record.ensureValid();
                    if (record.offset() < batch.baseOffset() || record.offset() > batch.lastOffset()
                            || record.offset() <= previousOffset) {
                        throw rejected(position, "Invalid record offsets within batch");
                    }
                    if (record.sizeInBytes() <= 0) throw rejected(position, "Invalid record size");
                    recordBytes += record.sizeInBytes();
                    if (recordBytes > size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD) {
                        throw rejected(position, "Record sizes exceed batch payload");
                    }
                    Marker marker = batch.isControlBatch() ? marker(record, position) : null;
                    byte[] key = bytes(record.key());
                    byte[] value = bytes(record.value());
                    Long id = !batch.isControlBatch() && value != null && value.length <= 20
                            ? KafkaIdSetValidator.parseCanonicalId(new String(value, StandardCharsets.UTF_8)) : null;
                    entries.add(new Entry(record.offset(), record.sequence(), record.timestamp(),
                            base64(key), base64(value), id, marker));
                    previousOffset = record.offset();
                }
                if (entries.size() != declaredCount
                        || recordBytes != size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD) {
                    throw rejected(position, "Record count or consumed bytes differ from the batch header");
                }
                batches.add(new Batch(position, size, batch.magic(), batch.baseOffset(), batch.lastOffset(),
                        batch.producerId(), batch.producerEpoch(), batch.baseSequence(), batch.lastSequence(),
                        batch.partitionLeaderEpoch(), batch.isTransactional(), batch.isControlBatch(),
                        batch.compressionType().name(), entries));
                recordCount += entries.size();
                position += size;
                previousLastOffset = batch.lastOffset();
            }
            if (position != segment.length || batches.isEmpty()) {
                throw rejected(position, "Input was not completely consumed");
            }
            return new Segment(batches, recordCount);
        } catch (IllegalArgumentException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Kafka batch encoding or checksum at byte " + position
                    + " (" + failure.getClass().getSimpleName() + ")", failure);
        }
    }

    private static Marker marker(org.apache.kafka.common.record.Record record, int position) {
        if (!record.hasKey() || !record.hasValue() || record.headers().length != 0) {
            throw rejected(position, "Malformed transaction marker");
        }
        ControlRecordType type = ControlRecordType.parse(record.key());
        if (type != ControlRecordType.COMMIT && type != ControlRecordType.ABORT) {
            throw rejected(position, "Unsupported control record; expected COMMIT or ABORT");
        }
        EndTransactionMarker marker = EndTransactionMarker.deserialize(record);
        var key = type.recordKey();
        ByteBuffer canonicalKey = ByteBuffer.allocate(key.sizeOf());
        key.writeTo(canonicalKey);
        canonicalKey.flip();
        // Kafka accepts future positive versions as v0. This decoder promises only exact known encoding.
        if (!canonicalKey.equals(record.key()) || !marker.serializeValue().equals(record.value())) {
            throw rejected(position, "Unsupported transaction marker encoding or version");
        }
        return new Marker(marker.controlType().name(), marker.coordinatorEpoch());
    }

    private static byte[] bytes(ByteBuffer buffer) {
        if (buffer == null) return null;
        byte[] result = new byte[buffer.remaining()];
        buffer.duplicate().get(result);
        return result;
    }

    private static String base64(byte[] bytes) {
        return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
    }

    private static IllegalArgumentException rejected(long position, String reason) {
        return new IllegalArgumentException(reason + " at byte " + position);
    }
}
