package org.savonitar.flink.stability.core.spec;

import java.util.Objects;

/** One Maven closure root and its position in the surrounding artifact declaration list. */
record MavenRuntimeRoot(int declarationIndex, MavenCoordinate coordinate) {
    MavenRuntimeRoot {
        if (declarationIndex < 0) {
            throw new IllegalArgumentException("declarationIndex must be non-negative");
        }
        Objects.requireNonNull(coordinate, "coordinate");
    }
}
