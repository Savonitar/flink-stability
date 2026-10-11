package org.savonitar.flink.stability.flinkjob;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkKafkaEosJobArgumentsTest {

    @Test
    void checkpointFailureHooksAreOptInWithDefaultPlannedDelay() {
        FlinkKafkaEosJobArguments defaults = FlinkKafkaEosJobArguments.from(new String[0]);
        assertEquals(Set.of(), defaults.failAsyncSnapshotForCheckpoints());
        assertEquals(500, defaults.failAsyncSnapshotDelayMs());
        assertNull(defaults.failAfterSnapshotOfCheckpoint());
        assertEquals(2000, defaults.failAfterSnapshotDelayMs());
        assertFalse(defaults.snapshotHooksEnabled());

        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--failAsyncSnapshotForCheckpoints", "2,3", "--failAfterSnapshotOfCheckpoint", "3"});
        assertEquals(Set.of(2L, 3L), arguments.failAsyncSnapshotForCheckpoints());
        assertEquals(500, arguments.failAsyncSnapshotDelayMs());
        assertEquals(3L, arguments.failAfterSnapshotOfCheckpoint());
        assertEquals(2000, arguments.failAfterSnapshotDelayMs());
        assertTrue(arguments.snapshotHooksEnabled());
    }

    @Test
    void parsesSingleCheckpointAndExplicitZeroPlannedDelayWithoutAsyncFailure() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--failAfterSnapshotOfCheckpoint", "8", "--failAfterSnapshotDelayMs", "0"});
        assertEquals(Set.of(7L), FlinkKafkaEosJobArguments.from(new String[]{
                "--failAsyncSnapshotForCheckpoints", "7"}).failAsyncSnapshotForCheckpoints());
        assertEquals(8L, arguments.failAfterSnapshotOfCheckpoint());
        assertEquals(0, arguments.failAfterSnapshotDelayMs());
        for (String delay : new String[]{"250", "2147483647"}) {
            assertEquals(Integer.parseInt(delay), FlinkKafkaEosJobArguments.from(new String[]{
                    "--failAfterSnapshotOfCheckpoint", "8", "--failAfterSnapshotDelayMs", delay}).failAfterSnapshotDelayMs());
        }
    }

    @Test
    void asyncFailureDelayIsNonnegativeAndIndependentOfOrdinaryDelay() {
        for (String delay : new String[]{"0", "500", "2147483647"}) {
            FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                    "--failAsyncSnapshotForCheckpoints", "2", "--failAsyncSnapshotDelayMs", delay,
                    "--snapshotAsyncDelayMs", "8000"});
            assertEquals(Integer.parseInt(delay), arguments.failAsyncSnapshotDelayMs());
            assertEquals(8000, arguments.snapshotAsyncDelayMs());
        }
        for (String value : new String[]{"", "-1", "01", "nan", "2,3", "2147483648"}) {
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{"--failAsyncSnapshotForCheckpoints", "2", "--failAsyncSnapshotDelayMs", value}), value);
        }
    }

    @Test
    void asyncFailureMustPrecedePlannedFailureWhenBothAreConfigured() {
        for (String[] delays : new String[][]{{"0", "0"}, {"500", "500"}, {"501", "500"}, {"2000", "2000"}}) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> FlinkKafkaEosJobArguments.from(new String[]{
                            "--failAsyncSnapshotForCheckpoints", "2,3", "--failAsyncSnapshotDelayMs", delays[0],
                            "--failAfterSnapshotOfCheckpoint", "3", "--failAfterSnapshotDelayMs", delays[1]}));
            assertTrue(failure.getMessage().contains("--failAsyncSnapshotDelayMs must be strictly smaller than --failAfterSnapshotDelayMs"));
            assertTrue(failure.getMessage().contains("checkpoint 3 must decline before the planned failure cancels the task"));
            assertTrue(failure.getMessage().contains("POOL_REUSE_SNAPSHOT_ASYNC_FAIL marker cannot match"));
        }
        // The default failure delay also participates, even if the target is not listed.
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(new String[]{
                "--failAsyncSnapshotForCheckpoints", "2", "--failAfterSnapshotOfCheckpoint", "3",
                "--failAfterSnapshotDelayMs", "500"}));
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--failAsyncSnapshotForCheckpoints", "2,3", "--failAsyncSnapshotDelayMs", "0",
                "--failAfterSnapshotOfCheckpoint", "3", "--failAfterSnapshotDelayMs", "1"});
        assertEquals(0, arguments.failAsyncSnapshotDelayMs());
        assertEquals(1, arguments.failAfterSnapshotDelayMs());
    }

    @Test
    void rejectsMalformedCheckpointListsAndDuplicateIds() {
        for (String value : new String[]{"", "0", "-1", "01", "nan", "2,2", "2,3,2", "2,0",
                "2,", ",2", "2,,3", "2, 3", "2,three", "9223372036854775808"}) {
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{"--failAsyncSnapshotForCheckpoints", value}), value);
        }
    }

    @Test
    void rejectsInvalidPlannedCheckpointAndDelay() {
        for (String value : new String[]{"0", "-1", "01", "nan", "3,4", "9223372036854775808"}) {
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{"--failAfterSnapshotOfCheckpoint", value}), value);
        }
        for (String value : new String[]{"-1", "01", "nan", "2,3", "2147483648"}) {
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{"--failAfterSnapshotOfCheckpoint", "3", "--failAfterSnapshotDelayMs", value}), value);
        }
    }

    @Test
    void rejectsMissingAndDuplicateValuesForEveryNewOption() {
        for (String option : new String[]{"--failAsyncSnapshotForCheckpoints",
                "--failAsyncSnapshotDelayMs", "--failAfterSnapshotOfCheckpoint", "--failAfterSnapshotDelayMs"}) {
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{option}), option);
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{option, "2", option, "3"}), option);
        }
    }

    @Test
    void delayOptionsRequireTheirOwnFailureTargets() {
        for (String[] options : new String[][]{
                {"--failAfterSnapshotDelayMs", "--failAfterSnapshotOfCheckpoint", "--failAsyncSnapshotForCheckpoints"},
                {"--failAsyncSnapshotDelayMs", "--failAsyncSnapshotForCheckpoints", "--failAfterSnapshotOfCheckpoint"}}) {
            for (String delay : new String[]{"0", "500", "2147483647"}) {
                IllegalArgumentException alone = assertThrows(IllegalArgumentException.class,
                        () -> FlinkKafkaEosJobArguments.from(new String[]{options[0], delay}));
                assertEquals(options[0] + " requires " + options[1], alone.getMessage());
                IllegalArgumentException otherTarget = assertThrows(IllegalArgumentException.class,
                        () -> FlinkKafkaEosJobArguments.from(new String[]{options[0], delay, options[2], "3"}));
                assertEquals(options[0] + " requires " + options[1], otherTarget.getMessage());
            }
        }
    }

    @Test
    void snapshotHooksDependOnlyOnConfiguredInjection() {
        assertFalse(FlinkKafkaEosJobArguments.from(new String[]{"--snapshotAsyncDelayMs", "0"}).snapshotHooksEnabled());
        assertFalse(FlinkKafkaEosJobArguments.from(new String[]{"--processingDelayMs", "5"}).snapshotHooksEnabled());
        for (String option : new String[]{"--snapshotAsyncDelayMs", "--failAsyncSnapshotForCheckpoints",
                "--failAfterSnapshotOfCheckpoint"}) {
            assertTrue(FlinkKafkaEosJobArguments.from(new String[]{option, "2"}).snapshotHooksEnabled());
        }
    }

    @Test
    void calibrationMaxBlockIsOptInAndIndependentOfProcessingDelay() {
        assertNull(FlinkKafkaEosJobArguments.from(new String[0]).producerMaxBlockMs());
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--producerMaxBlockMs", "5000", "--processingDelayMs", "5"});
        assertEquals(5000, arguments.producerMaxBlockMs());
        assertEquals(5, arguments.processingDelayMs());
    }

    @Test
    void rejectsInvalidOrDuplicateMaxBlock() {
        for (String value : new String[]{"0", "-1", "01", "2147483648", "nan"}) {
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{"--producerMaxBlockMs", value}));
        }
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                new String[]{"--producerMaxBlockMs"}));
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                new String[]{"--producerMaxBlockMs", "5", "--producerMaxBlockMs", "5"}));
    }

    @Test void packetRequestTimeoutIsExplicitAndStrictlyPositive() {
        assertNull(FlinkKafkaEosJobArguments.from(new String[0]).producerRequestTimeoutMs());
        assertEquals(30000, FlinkKafkaEosJobArguments.from(new String[]{"--producerRequestTimeoutMs", "30000"}).producerRequestTimeoutMs());
        for (String value : new String[]{"0", "-1", "01", "2147483648"})
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(new String[]{"--producerRequestTimeoutMs", value}));
    }

    @Test
    void defaultsProcessingDelayToZero() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[0]);

        assertEquals(0, arguments.processingDelayMs());
    }

    @Test
    void parsesExplicitProcessingDelay() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--processingDelayMs", "250"
        });

        assertEquals(250, arguments.processingDelayMs());
    }

    @Test
    void acceptsExplicitZeroDelay() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(
                new String[]{"--processingDelayMs", "0"});

        assertEquals(0, arguments.processingDelayMs());
    }

    @Test
    void rejectsUnknownArguments() {
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                new String[]{"--bootstrapServers", "kafka:9092"}));
    }

    @Test
    void rejectsDuplicateProcessingDelay() {
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(new String[]{
                "--processingDelayMs", "1",
                "--processingDelayMs", "2"
        }));
    }

    @Test
    void rejectsNonNumericProcessingDelay() {
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(new String[]{
                "--processingDelayMs", "not-a-number"
        }));
    }

    @Test
    void rejectsNegativeProcessingDelay() {
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(new String[]{
                "--processingDelayMs", "-1"
        }));
    }

    @Test
    void rejectsMissingProcessingDelayValue() {
        assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                new String[]{"--processingDelayMs"}));
    }
}
