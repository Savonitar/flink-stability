package org.savonitar.flink.stability.core.spec.resolution;

import java.util.Objects;
import java.util.regex.Pattern;

/** Canonical v1 registry predicate for the supported Apache Kafka broker image line. */
public final class KafkaBrokerImagePolicy {
    private static final Pattern SUPPORTED_V1_IMAGE = Pattern.compile(
            "^apache/kafka:4\\.0\\.(?:0|[1-9][0-9]*)"
                    + "(?:@sha256:[0-9a-f]{64})?$");

    private KafkaBrokerImagePolicy() {}

    public static boolean isSupportedV1(String imageReference) {
        return SUPPORTED_V1_IMAGE.matcher(
                Objects.requireNonNull(imageReference, "imageReference")).matches();
    }
}
