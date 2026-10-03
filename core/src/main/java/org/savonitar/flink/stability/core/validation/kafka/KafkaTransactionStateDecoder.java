package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.nio.ByteBuffer;
import org.savonitar.flink.stability.runtime.api.Digests;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/** Pure, bounded interpretation of one coordinator record; ownership/history stay with its caller. */
public final class KafkaTransactionStateDecoder {
    public static final int MAX_RECORD_BYTES = 64 * 1024;

    public record Limits(int maxRecordBytes, int maxTopics, int maxPartitions,
                         int maxUnknownTags, int maxUnknownTagBytes) {
        public Limits {
            if (maxRecordBytes < 1 || maxRecordBytes > MAX_RECORD_BYTES
                    || maxTopics < 1 || maxTopics > 256
                    || maxPartitions < 1 || maxPartitions > 16_384
                    || maxUnknownTags < 1 || maxUnknownTags > 256
                    || maxUnknownTagBytes < 1 || maxUnknownTagBytes > MAX_RECORD_BYTES) {
                throw new IllegalArgumentException("Invalid coordinator-record limits");
            }
        }
        public static Limits bounded() { return new Limits(MAX_RECORD_BYTES, 128, 4096, 128, 32768); }
    }

    public enum Status {
        DECODED, TOMBSTONE, UNSUPPORTED_VERSION, UNSUPPORTED_ENCODING,
        UNSUPPORTED_SEMANTICS, MALFORMED, LIMIT, DEADLINE, INTERRUPTED
    }

    /** length=-1 means null. A null hash means it was not computed, never an empty payload hash. */
    public record BytesEvidence(int length, String sha256) {}
    /** Encoded bytes make the returned evidence immutable; order and numeric tag ID are retained. */
    public record Tag(int id, int length, String sha256, String base64) {}
    public record Partitions(String topic, List<Integer> ids, List<Tag> unknownTags) {
        public Partitions { ids = List.copyOf(ids); unknownTags = List.copyOf(unknownTags); }
    }
    /** Nullable partitions are intentionally distinct from an empty list. No last-epoch field exists. */
    public record Value(long producerId, long previousProducerId, long nextProducerId,
                        short producerEpoch, int timeoutMs, byte stateId, String stateName,
                        long startTimestampMs, long lastUpdateTimestampMs,
                        List<Partitions> partitions, short clientTransactionVersion,
                        List<Tag> unknownTags) {
        public Value {
            if (partitions != null) partitions = List.copyOf(partitions);
            unknownTags = List.copyOf(unknownTags);
        }
        public boolean hasUnknownTags() {
            return !unknownTags.isEmpty() || partitions != null
                    && partitions.stream().anyMatch(partition -> !partition.unknownTags().isEmpty());
        }
    }
    /** Nullable versions/id/value mean that stage was not decoded; raw payloads remain caller-owned. */
    public record Result(Status status, BytesEvidence key, BytesEvidence value,
                         Short keyVersion, Short valueVersion, String transactionalId,
                         Value decoded, String detail) {
        public boolean knownSemantics() {
            return status == Status.DECODED && decoded != null && !decoded.hasUnknownTags();
        }
    }

    public Result decode(byte[] key, byte[] value, Limits limits, MonotonicDeadline deadline) {
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(deadline, "deadline");
        if (deadline.timeout().compareTo(Duration.ofSeconds(60)) > 0) {
            throw new IllegalArgumentException("Caller deadline must be at most 60 seconds");
        }
        BytesEvidence keyEvidence = new BytesEvidence(length(key), null);
        BytesEvidence valueEvidence = new BytesEvidence(length(value), null);
        Short keyVersion = null, valueVersion = null;
        String transactionalId = null;
        Value decoded = null;
        try {
            remaining(deadline);
            long bytes = Math.max(0, length(key)) + (long) Math.max(0, length(value));
            require(bytes <= limits.maxRecordBytes(), Status.LIMIT, "Combined key/value byte limit");
            // The <=64 KiB copy is the exact byte identity parsed below, even if the caller reuses buffers.
            byte[] exactKey = key == null ? null : key.clone();
            byte[] exactValue = value == null ? null : value.clone();
            remaining(deadline);
            keyEvidence = evidence(exactKey, deadline);
            valueEvidence = evidence(exactValue, deadline);
            require(exactKey != null && exactKey.length >= 2, Status.MALFORMED,
                    "Missing key or truncated key version");
            ByteBuffer keyBuffer = ByteBuffer.wrap(exactKey).asReadOnlyBuffer();
            keyVersion = keyBuffer.getShort();
            require(keyVersion == 0,
                    Status.UNSUPPORTED_VERSION, "Unsupported transaction-log key version");
            remaining(deadline);
            TransactionLogKey generatedKey = new TransactionLogKey(new ByteBufferAccessor(keyBuffer), keyVersion);
            remaining(deadline);
            require(!keyBuffer.hasRemaining(), Status.UNSUPPORTED_ENCODING, "Unconsumed key bytes");
            remaining(deadline);
            byte[] canonicalKey = canonical(keyVersion, generatedKey, deadline);
            remaining(deadline);
            require(Arrays.equals(exactKey, canonicalKey), Status.UNSUPPORTED_ENCODING,
                    "Key differs from generated canonical encoding");
            transactionalId = generatedKey.transactionalId();
            if (exactValue == null) {
                remaining(deadline);
                return result(Status.TOMBSTONE, keyEvidence, valueEvidence, keyVersion, null,
                        transactionalId, null, "Null value; no historical-absence inference");
            }
            require(exactValue.length >= 2, Status.MALFORMED, "Truncated value version");
            ByteBuffer valueBuffer = ByteBuffer.wrap(exactValue).asReadOnlyBuffer();
            valueVersion = valueBuffer.getShort();
            require(valueVersion == 0 || valueVersion == 1,
                    Status.UNSUPPORTED_VERSION, "Unsupported transaction-log value version");
            remaining(deadline);
            TransactionLogValue generated = new TransactionLogValue(new ByteBufferAccessor(valueBuffer), valueVersion);
            remaining(deadline);
            require(!valueBuffer.hasRemaining(), Status.UNSUPPORTED_ENCODING, "Unconsumed value bytes");
            // Generated array/tag parsing is byte-bounded; these tighter object limits are post-parse.
            decoded = retain(generated, limits, deadline);
            remaining(deadline);
            byte[] canonicalValue = canonical(valueVersion, generated, deadline);
            remaining(deadline);
            require(Arrays.equals(exactValue, canonicalValue), Status.UNSUPPORTED_ENCODING,
                    "Value differs from generated canonical encoding (including tag framing)");
            if (decoded.hasUnknownTags() || decoded.stateName().equals("UNKNOWN") || decoded.clientTransactionVersion() < 0
                    || decoded.clientTransactionVersion() > 2) {
                remaining(deadline);
                return result(Status.UNSUPPORTED_SEMANTICS, keyEvidence, valueEvidence, keyVersion,
                        valueVersion, transactionalId, decoded, "Unknown tagged field, state or client transaction version");
            }
            remaining(deadline);
            return result(Status.DECODED, keyEvidence, valueEvidence, keyVersion, valueVersion,
                    transactionalId, decoded, "Known Kafka 4 fields decoded; no ownership or history-completeness claim");
        } catch (Rejected rejected) {
            return result(rejected.status, keyEvidence, valueEvidence, keyVersion, valueVersion,
                    transactionalId, decoded, rejected.getMessage());
        } catch (RuntimeException malformed) {
            // Never swallow fatal Errors; do not persist arbitrary exception messages or raw payloads.
            Status status = Thread.currentThread().isInterrupted() ? Status.INTERRUPTED
                    : deadline.remaining().isZero() ? Status.DEADLINE : Status.MALFORMED;
            return result(status, keyEvidence, valueEvidence, keyVersion, valueVersion,
                    transactionalId, decoded, "Generated message rejected record: " + malformed.getClass().getSimpleName());
        }
    }

    private static Value retain(TransactionLogValue value, Limits limits, MonotonicDeadline deadline) {
        List<Partitions> partitions = null;
        int partitionCount = 0;
        int[] tags = new int[2]; // One record-wide count/byte allowance, including nested topic tags.
        if (value.transactionPartitions() != null) {
            require(value.transactionPartitions().size() <= limits.maxTopics(), Status.LIMIT, "Topic count limit");
            partitions = new ArrayList<>();
            for (var partition : value.transactionPartitions()) {
                remaining(deadline);
                partitionCount += partition.partitionIds().size();
                require(partitionCount <= limits.maxPartitions(), Status.LIMIT, "Partition count limit");
                partitions.add(new Partitions(partition.topic(), partition.partitionIds(),
                        tags(partition.unknownTaggedFields(), limits, tags, deadline)));
            }
        }
        List<Tag> unknown = tags(value.unknownTaggedFields(), limits, tags, deadline);
        remaining(deadline);
        return new Value(value.producerId(), value.previousProducerId(), value.nextProducerId(),
                value.producerEpoch(), value.transactionTimeoutMs(), value.transactionStatus(),
                stateName(value.transactionStatus()), value.transactionStartTimestampMs(),
                value.transactionLastUpdateTimestampMs(), partitions, value.clientTransactionVersion(), unknown);
    }

    private static List<Tag> tags(List<RawTaggedField> fields, Limits limits, int[] used,
                                  MonotonicDeadline deadline) {
        List<Tag> result = new ArrayList<>();
        int previousTag = -1;
        for (RawTaggedField field : fields) {
            remaining(deadline);
            require(field.tag() >= 0 && field.tag() > previousTag, Status.UNSUPPORTED_ENCODING,
                    "Unknown tag IDs are not strictly increasing");
            previousTag = field.tag();
            require(++used[0] <= limits.maxUnknownTags(), Status.LIMIT, "Unknown tag count limit");
            byte[] data = field.data();
            used[1] += data.length;
            require(used[1] <= limits.maxUnknownTagBytes(), Status.LIMIT, "Unknown tag byte limit");
            String hash = sha(data);
            remaining(deadline);
            String base64 = Base64.getEncoder().encodeToString(data);
            remaining(deadline);
            result.add(new Tag(field.tag(), data.length, hash, base64));
        }
        return List.copyOf(result);
    }

    private static String stateName(byte state) {
        return switch (state) {
            case 0 -> "Empty"; case 1 -> "Ongoing"; case 2 -> "PrepareCommit";
            case 3 -> "PrepareAbort"; case 4 -> "CompleteCommit"; case 5 -> "CompleteAbort";
            case 6 -> "Dead"; case 7 -> "PrepareEpochFence"; default -> "UNKNOWN";
        };
    }

    private static byte[] canonical(short version, ApiMessage message, MonotonicDeadline deadline) {
        remaining(deadline);
        try {
            byte[] encoded = MessageUtil.toVersionPrefixedBytes(version, message);
            remaining(deadline);
            return encoded;
        } catch (Rejected rejected) {
            throw rejected;
        } catch (RuntimeException unsupported) {
            remaining(deadline);
            throw new Rejected(Status.UNSUPPORTED_ENCODING,
                    "Parsed message cannot be serialized canonically: " + unsupported.getClass().getSimpleName());
        }
    }

    private static Result result(Status status, BytesEvidence key, BytesEvidence value,
                                 Short keyVersion, Short valueVersion, String id, Value decoded, String detail) {
        return new Result(status, key, value, keyVersion, valueVersion, id, decoded, detail);
    }
    private static BytesEvidence evidence(byte[] bytes, MonotonicDeadline deadline) {
        remaining(deadline);
        String hash = bytes == null ? null : sha(bytes);
        remaining(deadline);
        return new BytesEvidence(length(bytes), hash);
    }
    private static int length(byte[] value) { return value == null ? -1 : value.length; }
    private static String sha(byte[] bytes) { return Digests.sha256(bytes); }
    private static void remaining(MonotonicDeadline deadline) {
        require(!Thread.currentThread().isInterrupted(), Status.INTERRUPTED, "Caller interrupted");
        require(!deadline.remaining().isZero(), Status.DEADLINE, "Shared caller deadline exhausted");
    }
    private static void require(boolean condition, Status status, String detail) {
        if (!condition) throw new Rejected(status, detail);
    }
    private static final class Rejected extends RuntimeException {
        private final Status status;
        private Rejected(Status status, String detail) { super(detail); this.status = status; }
    }
}
