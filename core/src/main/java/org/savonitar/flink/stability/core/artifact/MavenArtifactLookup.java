package org.savonitar.flink.stability.core.artifact;

import java.nio.file.Path;
import java.util.List;

@FunctionalInterface
interface MavenArtifactLookup {
    Path resolve(MavenCoordinate coordinate, boolean offline) throws MavenArtifactLookupException;

    /**
     * Resolves the Maven runtime closure for roots in declaration order.
     *
     * <p>The default preserves this interface's single-abstract-method contract for the
     * existing primary-artifact lookup test doubles. Implementations that support dependency
     * graphs override this method.
     */
    default MavenRuntimeClosure resolveRuntimeClosure(
            List<MavenRuntimeRoot> roots,
            boolean offline) throws MavenArtifactLookupException {
        throw new UnsupportedOperationException("Maven runtime-closure resolution is unsupported");
    }
}
