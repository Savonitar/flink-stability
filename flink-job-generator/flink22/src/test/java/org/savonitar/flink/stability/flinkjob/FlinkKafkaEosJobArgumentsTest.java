package org.savonitar.flink.stability.flinkjob;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkKafkaEosJobArgumentsTest {

    @Test
    void parsesRequiredBootstrapServersAndDefaultsProcessingDelayToZero() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(
                new String[]{"--bootstrapServers", "kafka:9092"});

        assertEquals("kafka:9092", arguments.bootstrapServers());
        assertEquals(0, arguments.processingDelayMs());
        assertEquals("flink-stability-legacy", arguments.transactionalIdPrefix());
    }

    @Test
    void parsesExplicitProcessingDelay() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--bootstrapServers", "kafka:9095",
                "--processingDelayMs", "250"
        });

        assertEquals("kafka:9095", arguments.bootstrapServers());
        assertEquals(250, arguments.processingDelayMs());
    }

    @Test
    void parsesExplicitTransactionalIdPrefix() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--bootstrapServers", "kafka:9095",
                "--transactionalIdPrefix", "scenario-attempt-job"
        });

        assertEquals("scenario-attempt-job", arguments.transactionalIdPrefix());
    }

    @Test
    void constructsExactlyOnceSinkWithTransactionalIdPrefix() {
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(new String[]{
                "--bootstrapServers", "kafka:9092",
                "--transactionalIdPrefix", "scenario-attempt-job"
        });

        FlinkKafkaEosJob.createSink(arguments, new Properties());
    }

    @Test
    void rejectsMissingBootstrapServers() {
        assertThrows(RuntimeException.class, () -> FlinkKafkaEosJobArguments.from(new String[0]));
    }

    @Test
    void rejectsNonNumericProcessingDelay() {
        assertThrows(NumberFormatException.class, () -> FlinkKafkaEosJobArguments.from(new String[]{
                "--bootstrapServers", "kafka:9092",
                "--processingDelayMs", "not-a-number"
        }));
    }
}
