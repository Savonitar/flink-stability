package org.savonitar.flink.stability.core.validation.kafka;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.LongSupplier;

/**
 * Exact ID-set oracle over one complete, fenced Kafka snapshot. It compares output IDs with the
 * input manifest's present set, not a numeric range (SPEC-001 R7.3).
 */
public final class KafkaIdSetValidator {
    private static final int MAX_EVIDENCE_SAMPLES = 100;
    private static final long MAX_TERMINAL_RECORDS =
            ExecutableScenarioPlan.FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS;
    private static final int DEADLINE_CHECK_INTERVAL = 256;

    private static final Comparator<KafkaIdSetValidationResult.RecordSample> COORDINATE_ORDER =
            Comparator.comparingInt(KafkaIdSetValidationResult.RecordSample::partition)
                    .thenComparingLong(KafkaIdSetValidationResult.RecordSample::offset);

    private final KafkaSnapshotReader snapshotReader;
    private final LongSupplier nanoTime;

    public KafkaIdSetValidator() {
        this(new KafkaClientSnapshotReader(), System::nanoTime);
    }

    public KafkaIdSetValidator(KafkaSnapshotReader snapshotReader) {
        this(snapshotReader, System::nanoTime);
    }

    KafkaIdSetValidator(KafkaSnapshotReader snapshotReader, LongSupplier nanoTime) {
        this.snapshotReader = Objects.requireNonNull(snapshotReader, "snapshotReader");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /** {@code expectedIds} must be sorted, unique, and non-empty. */
    public KafkaIdSetValidationResult validate(
            String bootstrapServers,
            String topic,
            long[] expectedIds,
            Duration timeout) {
        Objects.requireNonNull(expectedIds, "expectedIds");
        if (expectedIds.length == 0) {
            throw new IllegalArgumentException("expectedIds must not be empty");
        }
        for (int index = 1; index < expectedIds.length; index++) {
            if (expectedIds[index] <= expectedIds[index - 1]) {
                throw new IllegalArgumentException("expectedIds must be sorted and unique");
            }
        }
        long expectedCount = expectedIds.length;
        Deadline deadline = Deadline.after(timeout, nanoTime);
        final KafkaTopicSnapshot snapshot;
        try {
            snapshot = snapshotReader.read(
                    bootstrapServers,
                    topic,
                    deadline.remaining(),
                    MAX_TERMINAL_RECORDS);
        } catch (DeadlineExpired expired) {
            return verificationFailure(
                    "verification.kafka.unreachable-after-timeout",
                    "Kafka verification deadline expired before broker discovery",
                    KafkaIdSetValidationResult.Evidence.unavailable(expectedCount));
        } catch (KafkaSnapshotException unavailable) {
            return verificationFailure(
                    unavailable.reason(),
                    unavailable.getMessage(),
                    partialEvidence(expectedCount, unavailable.partialEvidence()));
        }

        Accumulator accumulator = new Accumulator(expectedIds);
        try {
            int processed = 0;
            for (KafkaTopicSnapshot.ObservedRecord record : snapshot.records()) {
                if ((processed++ % DEADLINE_CHECK_INTERVAL) == 0) {
                    deadline.check();
                }
                accumulator.accept(record);
            }
            deadline.check();
            KafkaIdSetValidationResult.Evidence evidence = accumulator.evidence(
                    snapshot.beginningOffsets(), snapshot.endOffsets(), deadline);
            KafkaIdSetValidationResult.DefectTotals totals =
                    evidence.defectTotals().orElseThrow();
            if (totals.malformedCount() > 0) {
                return fail(
                        "validator.kafka.id-set.malformed-ids",
                        "Malformed record IDs",
                        evidence);
            }
            if (totals.unexpectedCount() > 0) {
                return fail(
                        "validator.kafka.id-set.unexpected-ids",
                        "Unexpected record IDs",
                        evidence);
            }
            if (totals.duplicateCount() > 0) {
                return fail(
                        "validator.kafka.id-set.duplicate-ids",
                        "Duplicate record IDs",
                        evidence);
            }
            if (totals.missingCount() > 0) {
                return fail(
                        "validator.kafka.id-set.missing-ids",
                        "Missing record IDs",
                        evidence);
            }
            return new KafkaIdSetValidationResult(
                    KafkaIdSetValidationResult.Status.PASS,
                    "validator.kafka.id-set.match",
                    "Kafka output exactly matches the input manifest",
                    evidence);
        } catch (DeadlineExpired expired) {
            return verificationFailure(
                    "verification.kafka.incomplete-after-timeout",
                    "Kafka ID decoding or comparison did not complete before the timeout",
                    accumulator.partialEvidence(
                            snapshot.beginningOffsets(), snapshot.endOffsets()));
        }
    }

    private static KafkaIdSetValidationResult.Evidence partialEvidence(
            long expectedCount,
            KafkaSnapshotException.PartialEvidence partial) {
        List<KafkaIdSetValidationResult.RecordSample> observed = partial.observedSamples().stream()
                .map(KafkaIdSetValidator::sample)
                .toList();
        return new KafkaIdSetValidationResult.Evidence(
                expectedCount,
                partial.observedCount(),
                java.util.Optional.empty(),
                observed,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                partial.beginningOffsets(),
                partial.endOffsets(),
                false);
    }

    private static KafkaIdSetValidationResult verificationFailure(
            String reason,
            String message,
            KafkaIdSetValidationResult.Evidence evidence) {
        return new KafkaIdSetValidationResult(
                KafkaIdSetValidationResult.Status.FAIL, reason, message, evidence);
    }

    private static KafkaIdSetValidationResult fail(
            String reason,
            String message,
            KafkaIdSetValidationResult.Evidence evidence) {
        return new KafkaIdSetValidationResult(
                KafkaIdSetValidationResult.Status.FAIL, reason, message, evidence);
    }

    private static Long parseCanonicalId(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        try {
            long parsed = Long.parseLong(raw);
            return Long.toString(parsed).equals(raw) ? parsed : null;
        } catch (NumberFormatException invalid) {
            return null;
        }
    }

    private static KafkaIdSetValidationResult.RecordSample sample(
            KafkaTopicSnapshot.ObservedRecord record) {
        return new KafkaIdSetValidationResult.RecordSample(
                record.partition(), record.offset(), record.value());
    }

    private static final class Accumulator {
        private final long[] expected;
        private final long expectedCount;
        private final Map<Long, ExpectedIdState> expectedIds = new HashMap<>();
        private final TreeSet<KafkaIdSetValidationResult.RecordSample> observedSamples =
                new TreeSet<>(COORDINATE_ORDER);
        private final TreeSet<KafkaIdSetValidationResult.RecordSample> malformedSamples =
                new TreeSet<>(COORDINATE_ORDER);
        private final TreeMap<Long, KafkaIdSetValidationResult.RecordSample> unexpectedSamples =
                new TreeMap<>();
        private final TreeMap<Long, KafkaIdSetValidationResult.RecordSample> duplicateSamples =
                new TreeMap<>();
        private long observedCount;
        private long malformedCount;
        private long unexpectedCount;
        private long duplicateCount;

        private Accumulator(long[] expected) {
            this.expected = expected;
            this.expectedCount = expected.length;
        }

        private void accept(KafkaTopicSnapshot.ObservedRecord record) {
            observedCount++;
            KafkaIdSetValidationResult.RecordSample coordinate = sample(record);
            retainSmallest(observedSamples, coordinate);
            Long id = parseCanonicalId(record.value());
            if (id == null) {
                malformedCount++;
                retainSmallest(malformedSamples, coordinate);
                return;
            }
            if (Arrays.binarySearch(expected, id) < 0) {
                unexpectedCount++;
                retainSmallest(unexpectedSamples, id, coordinate);
                return;
            }
            ExpectedIdState state = expectedIds.get(id);
            if (state == null) {
                expectedIds.put(id, new ExpectedIdState(coordinate));
            } else {
                state.observe(coordinate);
                duplicateCount++;
                retainSmallest(duplicateSamples, id, state.smallestDuplicateCoordinate());
            }
        }

        private KafkaIdSetValidationResult.Evidence evidence(
                Map<Integer, Long> beginningOffsets,
                Map<Integer, Long> endOffsets,
                Deadline deadline) {
            long missingCount = expectedCount - expectedIds.size();
            List<Long> missingSamples = new ArrayList<>();
            for (int index = 0;
                    index < expected.length && missingSamples.size() < MAX_EVIDENCE_SAMPLES;
                    index++) {
                if ((index % DEADLINE_CHECK_INTERVAL) == 0) {
                    deadline.check();
                }
                if (!expectedIds.containsKey(expected[index])) {
                    missingSamples.add(expected[index]);
                }
            }
            deadline.check();
            return new KafkaIdSetValidationResult.Evidence(
                    expectedCount,
                    observedCount,
                    java.util.Optional.of(new KafkaIdSetValidationResult.DefectTotals(
                            expectedIds.size(),
                            malformedCount,
                            unexpectedCount,
                            duplicateCount,
                            missingCount)),
                    List.copyOf(observedSamples),
                    List.copyOf(malformedSamples),
                    List.copyOf(unexpectedSamples.values()),
                    List.copyOf(duplicateSamples.values()),
                    missingSamples,
                    beginningOffsets,
                    endOffsets,
                    true);
        }

        private KafkaIdSetValidationResult.Evidence partialEvidence(
                Map<Integer, Long> beginningOffsets,
                Map<Integer, Long> endOffsets) {
            return new KafkaIdSetValidationResult.Evidence(
                    expectedCount,
                    observedCount,
                    java.util.Optional.empty(),
                    List.copyOf(observedSamples),
                    List.copyOf(malformedSamples),
                    List.copyOf(unexpectedSamples.values()),
                    List.copyOf(duplicateSamples.values()),
                    List.of(),
                    beginningOffsets,
                    endOffsets,
                    false);
        }

        private static void retainSmallest(
                TreeSet<KafkaIdSetValidationResult.RecordSample> samples,
                KafkaIdSetValidationResult.RecordSample sample) {
            samples.add(sample);
            if (samples.size() > MAX_EVIDENCE_SAMPLES) {
                samples.pollLast();
            }
        }

        private static void retainSmallest(
                TreeMap<Long, KafkaIdSetValidationResult.RecordSample> samples,
                long id,
                KafkaIdSetValidationResult.RecordSample sample) {
            samples.merge(id, sample, (left, right) ->
                    COORDINATE_ORDER.compare(left, right) <= 0 ? left : right);
            if (samples.size() > MAX_EVIDENCE_SAMPLES) {
                samples.pollLastEntry();
            }
        }

        private static final class ExpectedIdState {
            private KafkaIdSetValidationResult.RecordSample smallestCoordinate;
            private KafkaIdSetValidationResult.RecordSample secondSmallestCoordinate;

            private ExpectedIdState(KafkaIdSetValidationResult.RecordSample firstCoordinate) {
                this.smallestCoordinate = firstCoordinate;
            }

            private void observe(KafkaIdSetValidationResult.RecordSample coordinate) {
                if (COORDINATE_ORDER.compare(coordinate, smallestCoordinate) < 0) {
                    secondSmallestCoordinate = smallestCoordinate;
                    smallestCoordinate = coordinate;
                } else if (secondSmallestCoordinate == null
                        || COORDINATE_ORDER.compare(coordinate, secondSmallestCoordinate) < 0) {
                    secondSmallestCoordinate = coordinate;
                }
            }

            private KafkaIdSetValidationResult.RecordSample smallestDuplicateCoordinate() {
                return secondSmallestCoordinate;
            }
        }
    }

    private static final class Deadline {
        private final long startedAtNanos;
        private final long timeoutNanos;
        private final LongSupplier nanoTime;

        private Deadline(
                long startedAtNanos,
                long timeoutNanos,
                LongSupplier nanoTime) {
            this.startedAtNanos = startedAtNanos;
            this.timeoutNanos = timeoutNanos;
            this.nanoTime = nanoTime;
        }

        private static Deadline after(Duration timeout, LongSupplier nanoTime) {
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                nanos = Long.MAX_VALUE;
            }
            return new Deadline(nanoTime.getAsLong(), nanos, nanoTime);
        }

        private Duration remaining() {
            long elapsed = nanoTime.getAsLong() - startedAtNanos;
            if (elapsed < 0) {
                elapsed = 0;
            }
            long remaining = timeoutNanos - elapsed;
            if (remaining <= 0) {
                throw new DeadlineExpired();
            }
            return Duration.ofNanos(remaining);
        }

        private void check() {
            remaining();
        }
    }

    private static final class DeadlineExpired extends RuntimeException {}
}
