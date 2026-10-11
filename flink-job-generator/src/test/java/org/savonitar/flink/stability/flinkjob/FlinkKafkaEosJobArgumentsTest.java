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
        assertNull(defaults.failAfterSnapshotOfCheckpoint());
        assertEquals(2000, defaults.failAfterSnapshotDelayMs());
        assertFalse(defaults.snapshotHooksEnabled());

        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--failAsyncSnapshotForCheckpoints", "2,3", "--failAfterSnapshotOfCheckpoint", "3"});
        assertEquals(Set.of(2L, 3L), arguments.failAsyncSnapshotForCheckpoints());
        assertEquals(3L, arguments.failAfterSnapshotOfCheckpoint());
        assertEquals(2000, arguments.failAfterSnapshotDelayMs());
        assertTrue(arguments.snapshotHooksEnabled());
    }

    @Test
    void parsesSingleCheckpointAndExplicitZeroPlannedDelay() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--failAsyncSnapshotForCheckpoints", "7", "--failAfterSnapshotOfCheckpoint", "8",
                "--failAfterSnapshotDelayMs", "0"});
        assertEquals(Set.of(7L), arguments.failAsyncSnapshotForCheckpoints());
        assertEquals(8L, arguments.failAfterSnapshotOfCheckpoint());
        assertEquals(0, arguments.failAfterSnapshotDelayMs());
        assertEquals(250, FlinkKafkaEosJobArguments.from(new String[]{
                "--failAfterSnapshotDelayMs", "250"}).failAfterSnapshotDelayMs());
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
                    new String[]{"--failAfterSnapshotDelayMs", value}), value);
        }
    }

    @Test
    void rejectsMissingAndDuplicateValuesForEveryNewOption() {
        for (String option : new String[]{"--failAsyncSnapshotForCheckpoints",
                "--failAfterSnapshotOfCheckpoint", "--failAfterSnapshotDelayMs"}) {
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{option}), option);
            assertThrows(IllegalArgumentException.class, () -> FlinkKafkaEosJobArguments.from(
                    new String[]{option, "2", option, "3"}), option);
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
