package org.savonitar.flink.stability.core.validation.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongSupplier;

/** Independent arithmetic oracle over one complete fenced snapshot of keyed-prefix-v1 output. */
public final class KafkaStatePrefixValidator {
    public static final long MAX_RETAINED_BYTES = 128L * 1024 * 1024;
    private static final long MAX_RECORDS = ExecutableScenarioPlan.FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS;
    private static final Set<String> FIELDS = Set.of("version", "id", "key", "count", "prefixSum");
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(2)
                    .maxStringLength(KafkaStateTopicSnapshot.MAX_RECORD_BYTES).maxNumberLength(32).build()).build());
    private static final Comparator<KafkaStatePrefixValidationResult.StateMismatchSample> COORDINATES =
            Comparator.comparingInt(KafkaStatePrefixValidationResult.StateMismatchSample::partition)
                    .thenComparingLong(KafkaStatePrefixValidationResult.StateMismatchSample::offset);
    private final KafkaStateSnapshotReader reader;
    private final LongSupplier nanoTime;

    public KafkaStatePrefixValidator() { this(new KafkaClientStateSnapshotReader(), System::nanoTime); }
    public KafkaStatePrefixValidator(KafkaStateSnapshotReader reader) { this(reader, System::nanoTime); }
    KafkaStatePrefixValidator(KafkaStateSnapshotReader reader, LongSupplier nanoTime) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    public KafkaStatePrefixValidationResult validate(String bootstrapServers, String topic,
            KafkaInputManifest manifest, int keyCount, Duration timeout) {
        Objects.requireNonNull(manifest, "manifest");
        if (keyCount < 1 || keyCount > 1024) throw new IllegalArgumentException("keyCount must be in 1..1024");
        var deadline = MonotonicDeadline.start(timeout, nanoTime);
        var unavailable = KafkaIdSetValidationResult.Evidence.unavailable(manifest.totalRecords());
        try {
            if (!validManifest(manifest, deadline)) return failure(
                    "verification.kafka.input-manifest-invalid",
                    "State-prefix verification requires complete ordered integer input in one partition",
                    unavailable, keyCount, List.of());
        } catch (KafkaIdSetValidator.DeadlineExpired timeoutFailure) {
            return failure("verification.kafka.incomplete-after-timeout",
                    "Input manifest verification exceeded the shared deadline", unavailable, keyCount, List.of());
        }

        final KafkaStateTopicSnapshot snapshot;
        try {
            snapshot = reader.read(bootstrapServers, topic,
                    deadline.remainingOrThrow(KafkaIdSetValidator.DeadlineExpired::new), MAX_RECORDS, MAX_RETAINED_BYTES);
        } catch (KafkaIdSetValidator.DeadlineExpired expired) {
            return failure("verification.kafka.unreachable-after-timeout",
                    "Kafka verification deadline expired before broker discovery", unavailable, keyCount, List.of());
        } catch (KafkaSnapshotException incomplete) {
            var partial = incomplete.partialEvidence();
            var samples = partial.observedSamples().stream().limit(100)
                    .map(record -> new KafkaIdSetValidationResult.RecordSample(
                            record.partition(), record.offset(), record.value())).toList();
            return failure(incomplete.reason(), incomplete.getMessage(),
                    new KafkaIdSetValidationResult.Evidence(manifest.totalRecords(), partial.observedCount(),
                            Optional.empty(), samples, List.of(), List.of(), List.of(), List.of(),
                            partial.beginningOffsets(), partial.endOffsets(), false), keyCount, List.of());
        }

        var ids = new KafkaIdSetValidator.Accumulator(manifest.presentIds());
        var mismatches = new TreeSet<>(COORDINATES);
        long mismatchCount = 0;
        long retainedBytes = 0;
        long observedCount = 0;
        try {
            if (!topic.equals(snapshot.topic()) || snapshot.beginningOffsets().values().stream().anyMatch(offset -> offset != 0L))
                return failure("verification.kafka.snapshot-truncated", "Unexpected topic or truncated Kafka boundary",
                        ids.partialEvidence(snapshot.beginningOffsets(), snapshot.endOffsets()), keyCount, List.of());
            for (var record : snapshot.records()) {
                deadline.remainingOrThrow(KafkaIdSetValidator.DeadlineExpired::new);
                retainedBytes = Math.addExact(retainedBytes, record.retainedBytes());
                observedCount++;
                if (observedCount > MAX_RECORDS || retainedBytes > MAX_RETAINED_BYTES) {
                    return failure(observedCount > MAX_RECORDS ? "verification.kafka.output-limit-exceeded"
                                    : "verification.kafka.output-byte-limit-exceeded",
                            "Kafka state snapshot exceeded its bounded verification budget",
                            ids.partialEvidence(snapshot.beginningOffsets(), snapshot.endOffsets()), keyCount,
                            List.copyOf(mismatches));
                }
                var sample = new KafkaIdSetValidationResult.RecordSample(record.partition(), record.offset(), record.diagnosticValue());
                var decoded = decode(record);
                ids.accept(sample, decoded == null ? null : decoded.id());
                if (decoded == null || decoded.id() < 0 || decoded.id() >= manifest.totalRecords()) continue;
                long expectedKey = decoded.id() % keyCount;
                long s = decoded.id() / keyCount;
                long count = Math.addExact(s, 1);
                long expectedSum = Math.addExact(Math.multiplyExact(count, expectedKey + 1),
                        Math.multiplyExact(keyCount, Math.multiplyExact(s, count) / 2));
                if (decoded.key() != expectedKey || decoded.count() != count || decoded.sum() != expectedSum) {
                    mismatchCount++;
                    mismatches.add(new KafkaStatePrefixValidationResult.StateMismatchSample(record.partition(), record.offset(),
                            decoded.id(), expectedKey, decoded.key(), count, decoded.count(), expectedSum, decoded.sum()));
                    if (mismatches.size() > 100) mismatches.pollLast();
                }
            }
            deadline.remainingOrThrow(KafkaIdSetValidator.DeadlineExpired::new);
            var evidence = ids.evidence(snapshot.beginningOffsets(), snapshot.endOffsets(), deadline);
            var totals = evidence.defectTotals().orElseThrow();
            String defect = totals.malformedCount() > 0 ? "malformed-records"
                    : totals.unexpectedCount() > 0 ? "unexpected-ids" : totals.duplicateCount() > 0 ? "duplicate-ids"
                    : totals.missingCount() > 0 ? "missing-ids" : mismatchCount > 0 ? "state-mismatch" : null;
            return new KafkaStatePrefixValidationResult(defect == null ? KafkaIdSetValidationResult.Status.PASS
                            : KafkaIdSetValidationResult.Status.FAIL,
                    "validator.kafka.state-prefix." + (defect == null ? "match" : defect),
                    defect == null ? "Kafka IDs and keyed prefix state match the input manifest"
                            : "Kafka state-prefix output contains " + defect,
                    evidence, new KafkaStatePrefixValidationResult.StateEvidence(keyCount,
                            OptionalLong.of(mismatchCount), List.copyOf(mismatches)));
        } catch (KafkaIdSetValidator.DeadlineExpired expired) {
            return failure("verification.kafka.incomplete-after-timeout",
                    "State decoding or comparison exceeded the shared verification deadline",
                    ids.partialEvidence(snapshot.beginningOffsets(), snapshot.endOffsets()), keyCount, List.copyOf(mismatches));
        }
    }

    private static boolean validManifest(KafkaInputManifest manifest, MonotonicDeadline deadline) {
        if (manifest.evidenceStatus() != KafkaInputManifest.EvidenceStatus.COMPLETE
                || manifest.totalRecords() > MAX_RECORDS
                || !manifest.beginningOffsets().equals(java.util.Map.of(0, 0L))
                || !manifest.exclusiveEndOffsets().equals(java.util.Map.of(0, manifest.totalRecords()))) return false;
        for (var record : manifest.records()) {
            deadline.remainingOrThrow(KafkaIdSetValidator.DeadlineExpired::new);
            if (record.partition() != 0 || record.offset() != record.id()
                    || !record.payloadSha256().equals(Digests.sha256(Long.toString(record.id()).getBytes(StandardCharsets.UTF_8))))
                return false;
        }
        return true;
    }

    private static Decoded decode(KafkaStateTopicSnapshot.ObservedRecord record) {
        if (record.originalSize() < 0 || record.originalSize() > KafkaStateTopicSnapshot.MAX_RECORD_BYTES) return null;
        try {
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(record.bytes())).toString();
            try (var parser = JSON.createParser(text)) {
                JsonNode object = JSON.readTree(parser);
                if (object == null || !object.isObject() || object.size() != FIELDS.size() || parser.nextToken() != null) return null;
                for (String field : FIELDS) {
                    JsonNode value = object.get(field);
                    if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) return null;
                }
                if (object.path("version").longValue() != 1) return null;
                return new Decoded(object.path("id").longValue(), object.path("key").longValue(),
                        object.path("count").longValue(), object.path("prefixSum").longValue());
            }
        } catch (IOException malformed) { return null; }
    }

    private static KafkaStatePrefixValidationResult failure(String reason, String message,
            KafkaIdSetValidationResult.Evidence evidence, int keyCount,
            List<KafkaStatePrefixValidationResult.StateMismatchSample> samples) {
        return new KafkaStatePrefixValidationResult(KafkaIdSetValidationResult.Status.FAIL, reason, message, evidence,
                new KafkaStatePrefixValidationResult.StateEvidence(keyCount, OptionalLong.empty(), samples));
    }

    private record Decoded(long id, long key, long count, long sum) {}
}
