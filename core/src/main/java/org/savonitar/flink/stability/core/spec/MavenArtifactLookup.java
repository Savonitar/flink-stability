package org.savonitar.flink.stability.core.spec;

import java.nio.file.Path;

@FunctionalInterface
interface MavenArtifactLookup {
    Path resolve(MavenCoordinate coordinate, boolean offline) throws MavenArtifactLookupException;
}
