package org.savonitar.flink.stability.core.spec;

import java.util.List;
import java.util.Objects;

/** Deterministic runtime classpath plus reproducibility and conflict evidence. */
record MavenRuntimeClosure(
        List<ResolvedMavenJar> classpath,
        List<MavenConflictDecision> conflicts,
        List<MavenPomEvidence> consultedPoms) {
    MavenRuntimeClosure {
        classpath = List.copyOf(Objects.requireNonNull(classpath, "classpath"));
        conflicts = List.copyOf(Objects.requireNonNull(conflicts, "conflicts"));
        consultedPoms = List.copyOf(Objects.requireNonNull(
                consultedPoms, "consultedPoms"));
    }
}
