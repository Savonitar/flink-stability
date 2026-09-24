package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;

/** Argument checks shared by the harness's value types. */
public final class Checks {
    private Checks() {}

    /** Returns the value; null is a NullPointerException and blank text an IllegalArgumentException. */
    public static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
