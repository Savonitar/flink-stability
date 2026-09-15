package org.savonitar.flink.stability.core.spec.resolution;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidatorReasonRegistryTest {
    @Test
    void registersEveryStableIdSetFailureReason() {
        assertEquals(Set.of(
                        "validator.kafka.id-set.malformed-ids",
                        "validator.kafka.id-set.unexpected-ids",
                        "validator.kafka.id-set.duplicate-ids",
                        "validator.kafka.id-set.missing-ids"),
                ValidatorReasonRegistry.builtInReasons("kafka.id-set"));
    }
}
