package org.savonitar.flink.stability.flinkjob;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

class FlinkKafkaEosJobArgumentsTest {

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
