package org.savonitar.flink.stability.core.spec;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** A semantic scenario plan whose local artifacts have immutable prepared snapshots. */
public final class PreparedScenarioPlan implements AutoCloseable {
    private final ResolvedScenarioPlan scenarioPlan;
    private final ArtifactWorkspace workspace;
    private final List<ResolvedArtifact> artifacts;
    private final boolean ownsWorkspace;

    PreparedScenarioPlan(
            ResolvedScenarioPlan scenarioPlan,
            ArtifactWorkspace workspace,
            List<ResolvedArtifact> artifacts,
            boolean ownsWorkspace) {
        this.scenarioPlan = Objects.requireNonNull(scenarioPlan, "scenarioPlan");
        this.workspace = Objects.requireNonNull(workspace, "workspace");
        this.artifacts = List.copyOf(Objects.requireNonNull(artifacts, "artifacts"));
        this.ownsWorkspace = ownsWorkspace;

        boolean experiment = scenarioPlan.scenario().isExperiment();
        Set<ArtifactIdentity> identities = new HashSet<>();
        for (ResolvedArtifact artifact : this.artifacts) {
            if ((!experiment && artifact.scope() != ResolutionScope.SINGLE)
                    || (experiment && artifact.scope() == ResolutionScope.SINGLE)) {
                throw new IllegalArgumentException(
                        "Artifact scope does not match the scenario topology");
            }
            if (!identities.add(new ArtifactIdentity(artifact.scope(), artifact.path()))) {
                throw new IllegalArgumentException(
                        "Duplicate artifact identity " + artifact.scope() + " " + artifact.path());
            }
        }
    }

    public ResolvedScenarioPlan scenarioPlan() {
        return scenarioPlan;
    }

    public Path artifactRoot() {
        return workspace.artifactRoot();
    }

    /** Private snapshot root; it remains valid until its owning top-level plan is closed. */
    public Path preparationRoot() {
        return workspace.preparationRoot();
    }

    public List<ResolvedArtifact> artifacts() {
        return artifacts;
    }

    /** Returns the side-specific artifact, falling back to the experiment-common identity. */
    public Optional<ResolvedArtifact> artifact(ScenarioSide side, String path) {
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(path, "path");
        scenarioPlan.scenario().side(side);
        ResolutionScope sideScope = switch (side) {
            case SINGLE -> ResolutionScope.SINGLE;
            case BASELINE -> ResolutionScope.BASELINE;
            case CANDIDATE -> ResolutionScope.CANDIDATE;
        };
        Optional<ResolvedArtifact> exact = artifacts.stream()
                .filter(artifact -> artifact.scope() == sideScope && artifact.path().equals(path))
                .findFirst();
        return exact.isPresent()
                ? exact
                : artifacts.stream()
                        .filter(artifact -> artifact.scope() == ResolutionScope.COMMON
                                && artifact.path().equals(path))
                        .findFirst();
    }

    @Override
    public void close() {
        if (ownsWorkspace) {
            workspace.close();
        }
    }

    ArtifactWorkspace workspace() {
        return workspace;
    }

    private record ArtifactIdentity(ResolutionScope scope, String path) {}
}
