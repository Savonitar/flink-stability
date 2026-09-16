package org.savonitar.flink.stability.core.artifact;

import java.util.Objects;

/** Full provenance for one connector classpath entry contributing bytes to a bundle. */
public record ConnectorBundleContribution(
        String alias,
        int closureClasspathIndex,
        ConnectorClosureLockEntry closureEntry) {

    public ConnectorBundleContribution {
        Objects.requireNonNull(alias, "alias");
        if (alias.isBlank()) {
            throw new IllegalArgumentException("alias must not be blank");
        }
        Objects.requireNonNull(closureEntry, "closureEntry");
        if (closureClasspathIndex < 0
                || closureClasspathIndex != closureEntry.classpathIndex()) {
            throw new IllegalArgumentException(
                    "closureClasspathIndex must identify the contributed lock entry");
        }
    }
}
