package org.savonitar.flink.stability.core.validation.kafka;

/** The two supported terminal oracles share the same fixed-snapshot completeness evidence. */
public sealed interface KafkaTerminalValidationResult
        permits KafkaIdSetValidationResult, KafkaStatePrefixValidationResult {
    KafkaIdSetValidationResult.Status status();
    String reason();
    String message();
    KafkaIdSetValidationResult.Evidence evidence();
    KafkaIdSetValidationResult.Mode mode();
    String validatorType();
}
