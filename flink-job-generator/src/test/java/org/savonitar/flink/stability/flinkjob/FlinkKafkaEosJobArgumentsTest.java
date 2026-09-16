package org.savonitar.flink.stability.flinkjob;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkKafkaEosJobArgumentsTest {

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
