package org.savonitar.flink.stability.core.validation.kafka;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** Reference simulation deliberately uses only per-key updates, never the oracle formula or workload code. */
class KafkaStatePrefixArithmeticTest {
    @Test void closedFormMatchesIndependentSimulationForEverySupportedKeyCount() {
        for (int keys = 1; keys <= 1024; keys++) {
            long[] counts = new long[keys], sums = new long[keys];
            for (int id = 0; id < 4097; id++) {
                int key = id % keys;
                counts[key]++;
                sums[key] += id + 1L;
                var actual = KafkaStatePrefixValidator.expectedState(id, keys);
                if (actual.key() != key || actual.count() != counts[key] || actual.sum() != sums[key])
                    fail("Independent state differs at K=" + keys + ", id=" + id + ": " + actual);
            }
        }
    }

    @Test void maximumInputEndpointMatchesIndependentRepeatedAdditionWithoutOverflow() {
        long total = ExecutableScenarioPlan.FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS;
        assertEquals(1_000_000L, total, "Review arithmetic bounds when the executable input cap changes");
        for (int keys = 1; keys <= 1024; keys++) {
            long id = total - 1, key = id % keys, count = 0, sum = 0;
            for (long input = key; input <= id; input += keys) {
                count++;
                sum = Math.addExact(sum, input + 1);
            }
            var actual = KafkaStatePrefixValidator.expectedState(id, keys);
            assertEquals(key, actual.key());
            assertEquals(count, actual.count(), "K=" + keys);
            assertEquals(sum, actual.sum(), "K=" + keys);
        }
        assertEquals(500_000_500_000L, KafkaStatePrefixValidator.expectedState(total - 1, 1).sum());
        assertThrows(IllegalArgumentException.class, () -> KafkaStatePrefixValidator.expectedState(total, 1));
        assertThrows(IllegalArgumentException.class, () -> KafkaStatePrefixValidator.expectedState(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> KafkaStatePrefixValidator.expectedState(0, 0));
        assertThrows(IllegalArgumentException.class, () -> KafkaStatePrefixValidator.expectedState(0, 1025));
    }

    @Test void fullOracleAcceptsShuffledSimulationAndDetectsOnePositiveStateMutation() {
        int total = 4097;
        var manifest = KafkaStatePrefixValidatorTest.manifest(total);
        for (int keys : List.of(1, 2, 3, 7, 8, 63, 127, 511, 1023, 1024)) {
            long[] counts = new long[keys], sums = new long[keys];
            var rows = new ArrayList<String>();
            for (int id = 0; id < total; id++) {
                int key = id % keys;
                counts[key]++;
                sums[key] += id + 1L;
                rows.add(KafkaStatePrefixValidatorTest.json(id, key, counts[key], sums[key]));
            }
            Collections.shuffle(rows, new Random(41L + keys));
            var valid = validate(rows, manifest, keys);
            assertEquals(KafkaIdSetValidationResult.Status.PASS, valid.status(), "K=" + keys);
            assertEquals(total, valid.evidence().defectTotals().orElseThrow().distinctExpectedCount());
            assertEquals(0, valid.stateEvidence().mismatchCount().orElseThrow());

            int id = total - 1, key = id % keys;
            String correct = KafkaStatePrefixValidatorTest.json(id, key, counts[key], sums[key]);
            int position = rows.indexOf(correct);
            assertTrue(position >= 0);
            rows.set(position, KafkaStatePrefixValidatorTest.json(id, key, counts[key] + 1, sums[key]));
            var corrupt = validate(rows, manifest, keys);
            assertEquals("validator.kafka.state-prefix.state-mismatch", corrupt.reason(), "K=" + keys);
            assertEquals(1, corrupt.stateEvidence().mismatchCount().orElseThrow());
            var sample = corrupt.stateEvidence().mismatchSamples().getFirst();
            assertEquals(id, sample.id());
            assertEquals(position, sample.offset());
            assertEquals(counts[key], sample.expectedCount());
            assertEquals(sums[key], sample.expectedSum());
        }
    }

    private static KafkaStatePrefixValidationResult validate(List<String> rows,
            org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest manifest, int keys) {
        var records = new ArrayList<KafkaStateTopicSnapshot.ObservedRecord>();
        for (int offset = 0; offset < rows.size(); offset++)
            records.add(new KafkaStateTopicSnapshot.ObservedRecord(0, offset, rows.get(offset).getBytes(StandardCharsets.UTF_8)));
        var snapshot = new KafkaStateTopicSnapshot("output", Map.of(0, 0L), Map.of(0, (long) rows.size()), records);
        return new KafkaStatePrefixValidator((bootstrap, topic, timeout, limit, bytes) -> snapshot)
                .validate("unused", "output", manifest, keys, Duration.ofSeconds(30));
    }
}
