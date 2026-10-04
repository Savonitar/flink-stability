package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;

/** Shared pre-start pin check, exercised independently of Docker. */
final class KafkaImageIdentity {
    private KafkaImageIdentity() {}

    static void verify(KafkaRuntimeTarget target, String actual) {
        if (actual == null || !actual.matches("sha256:[0-9a-f]{64}"))
            throw new IllegalStateException("kafka.image.identity-unavailable: created container has no full image ID");
        if (target.imageId().filter(expected -> !expected.equals(actual)).isPresent())
            throw new IllegalStateException("kafka.image.pin-mismatch: created container differs from setup.kafka image_id");
    }
}
