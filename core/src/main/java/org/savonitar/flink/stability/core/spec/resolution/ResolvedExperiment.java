package org.savonitar.flink.stability.core.spec.resolution;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Effective pair-level metadata for a differential experiment. */
public final class ResolvedExperiment {
    private final String claim;
    private final Set<String> varies;

    public ResolvedExperiment(String claim, Set<String> varies) {
        this.claim = Objects.requireNonNull(claim, "claim");
        if (claim.isBlank()) {
            throw new IllegalArgumentException("claim must not be blank");
        }
        this.varies = Collections.unmodifiableSet(
                new LinkedHashSet<>(Objects.requireNonNull(varies, "varies")));
        if (this.varies.isEmpty()) {
            throw new IllegalArgumentException("varies must not be empty");
        }
        this.varies.forEach(value -> Objects.requireNonNull(value, "varies value"));
    }

    public String claim() {
        return claim;
    }

    public Set<String> varies() {
        return varies;
    }
}
