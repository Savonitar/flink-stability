package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChecksTest {

    @Test
    void requireNonBlankRejectsNullAndBlankText() {
        assertEquals("value", Checks.requireNonBlank("value", "name"));
        assertThrows(NullPointerException.class, () -> Checks.requireNonBlank(null, "name"));
        assertThrows(IllegalArgumentException.class, () -> Checks.requireNonBlank(" ", "name"));
    }
}
