package org.savonitar.flink.stability.core.spec.document;

import org.junit.jupiter.api.function.Executable;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Assertions for specification failures, which differ only by the stage that found them. */
public final class SpecificationAssertions {
    private SpecificationAssertions() {}

    /** Asserts that the executable fails at the given stage and returns that failure. */
    public static SpecificationException assertFailsAt(Stage stage, Executable executable) {
        return assertFailsAt(stage, executable, "expected a " + stage + " failure");
    }

    /** As {@link #assertFailsAt(Stage, Executable)}, naming the case if nothing is thrown. */
    public static SpecificationException assertFailsAt(
            Stage stage,
            Executable executable,
            String message) {
        SpecificationException failure =
                assertThrows(SpecificationException.class, executable, message);
        assertEquals(stage, failure.stage(), failure::getMessage);
        return failure;
    }
}
