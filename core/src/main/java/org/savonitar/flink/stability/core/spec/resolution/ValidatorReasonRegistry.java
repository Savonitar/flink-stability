package org.savonitar.flink.stability.core.spec.resolution;

import java.util.Map;
import java.util.Set;

/** Stable failure reasons emitted by the built-in v1 terminal validators. */
final class ValidatorReasonRegistry {
    private static final Map<String, Set<String>> BUILT_IN_REASONS = Map.of(
            "kafka.id-set", Set.of(
                    "validator.kafka.id-set.malformed-ids",
                    "validator.kafka.id-set.unexpected-ids",
                    "validator.kafka.id-set.duplicate-ids",
                    "validator.kafka.id-set.missing-ids"),
            "kafka.state-prefix", Set.of(
                    "validator.kafka.state-prefix.malformed-records",
                    "validator.kafka.state-prefix.unexpected-ids",
                    "validator.kafka.state-prefix.duplicate-ids",
                    "validator.kafka.state-prefix.missing-ids",
                    "validator.kafka.state-prefix.state-mismatch"),
            "kafka.no-hanging-transactions", Set.of(
                    "validator.kafka.transaction.ongoing-after-timeout"));

    private ValidatorReasonRegistry() {}

    static Set<String> builtInReasons(String validatorType) {
        return BUILT_IN_REASONS.getOrDefault(validatorType, Set.of());
    }
}
