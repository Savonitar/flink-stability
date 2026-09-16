package org.savonitar.flink.stability.core.execution.kafka;

import java.util.Objects;
import java.util.Optional;

/** Fails an experiment when exact Kafka input preparation cannot finish safely. */
public final class KafkaInputPreparationException extends RuntimeException {

    public static final String ACKNOWLEDGED_MISSING =
            "input-manifest.acknowledged-missing";
    public static final String INDETERMINATE = "input-manifest.indeterminate";
    public static final String INFRASTRUCTURE_SETUP_FAILED =
            "infrastructure.kafka-input-setup-failed";

    private final String reasonCode;
    private final KafkaInputManifest evidence;

    public KafkaInputPreparationException(String reasonCode, String message) {
        super(message);
        this.reasonCode = requireReasonCode(reasonCode);
        this.evidence = null;
    }

    public KafkaInputPreparationException(
            String reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = requireReasonCode(reasonCode);
        this.evidence = null;
    }

    public KafkaInputPreparationException(
            String reasonCode, String message, KafkaInputManifest evidence) {
        super(message);
        this.reasonCode = requireReasonCode(reasonCode);
        this.evidence = Objects.requireNonNull(evidence, "evidence");
    }

    public KafkaInputPreparationException(
            String reasonCode,
            String message,
            Throwable cause,
            KafkaInputManifest evidence) {
        super(message, cause);
        this.reasonCode = requireReasonCode(reasonCode);
        this.evidence = Objects.requireNonNull(evidence, "evidence");
    }

    public String reasonCode() {
        return reasonCode;
    }

    /** Returns every factual input-evidence status, including complete close-failure evidence. */
    public Optional<KafkaInputManifest> evidence() {
        return Optional.ofNullable(evidence);
    }

    private static String requireReasonCode(String reasonCode) {
        if (reasonCode == null || reasonCode.isBlank()) {
            throw new IllegalArgumentException("reasonCode must not be blank");
        }
        return reasonCode;
    }
}
