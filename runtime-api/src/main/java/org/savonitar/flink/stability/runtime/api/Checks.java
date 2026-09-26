package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;
import java.util.regex.Pattern;

/** Argument checks shared by the harness's value types. */
public final class Checks {
    private static final Pattern DOCKER_IMAGE_ID = Pattern.compile("sha256:[0-9a-f]{64}");

    private Checks() {}

    /** Returns the value; null is a NullPointerException and blank text an IllegalArgumentException. */
    public static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    /** A Docker image configuration identity, distinct from a registry manifest digest. */
    public static String requireDockerImageId(String value, String name) {
        requireNonBlank(value, name);
        if (!DOCKER_IMAGE_ID.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    name + " must be sha256: followed by 64 lowercase hexadecimal characters");
        }
        return value;
    }
}
