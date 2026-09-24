package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;
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
    private final List<PreparedConnectorClosure> connectorClosures;
    private final boolean ownsWorkspace;

    PreparedScenarioPlan(
            ResolvedScenarioPlan scenarioPlan,
            ArtifactWorkspace workspace,
            List<ResolvedArtifact> artifacts,
            List<PreparedConnectorClosure> connectorClosures,
            boolean ownsWorkspace) {
        this.scenarioPlan = Objects.requireNonNull(scenarioPlan, "scenarioPlan");
        this.workspace = Objects.requireNonNull(workspace, "workspace");
        this.artifacts = List.copyOf(Objects.requireNonNull(artifacts, "artifacts"));
        this.connectorClosures = List.copyOf(Objects.requireNonNull(
                connectorClosures, "connectorClosures"));
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

        Set<ConnectorIdentity> connectorIdentities = new HashSet<>();
        for (PreparedConnectorClosure closure : this.connectorClosures) {
            if ((!experiment && closure.scope() != ResolutionScope.SINGLE)
                    || (experiment && closure.scope() == ResolutionScope.SINGLE)) {
                throw new IllegalArgumentException(
                        "Connector closure scope does not match the scenario topology");
            }
            if (!connectorIdentities.add(new ConnectorIdentity(
                    closure.scope(), closure.alias()))) {
                throw new IllegalArgumentException(
                        "Duplicate connector closure identity " + closure.scope()
                                + " " + closure.alias());
            }
            boolean primaryPresent = this.artifacts.stream().anyMatch(artifact ->
                    artifact.scope() == closure.primary().scope()
                            && artifact.path().equals(
                                    closure.connectorPath() + "/artifact")
                            && artifact.preparedPath().equals(
                                    closure.primary().preparedPath())
                            && artifact.sha256().equals(closure.primary().sha256()));
            if (!primaryPresent) {
                throw new IllegalArgumentException(
                        "Connector closure primary must match a declared prepared artifact");
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

    public List<PreparedConnectorClosure> connectorClosures() {
        return connectorClosures;
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

    /** Returns the side-specific connector closure, falling back to a common closure. */
    public Optional<PreparedConnectorClosure> connectorClosure(
            ScenarioSide side,
            String alias) {
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(alias, "alias");
        scenarioPlan.scenario().side(side);
        ResolutionScope sideScope = switch (side) {
            case SINGLE -> ResolutionScope.SINGLE;
            case BASELINE -> ResolutionScope.BASELINE;
            case CANDIDATE -> ResolutionScope.CANDIDATE;
        };
        Optional<PreparedConnectorClosure> exact = connectorClosures.stream()
                .filter(closure -> closure.scope() == sideScope
                        && closure.alias().equals(alias))
                .findFirst();
        return exact.isPresent()
                ? exact
                : connectorClosures.stream()
                        .filter(closure -> closure.scope() == ResolutionScope.COMMON
                                && closure.alias().equals(alias))
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

    private record ConnectorIdentity(ResolutionScope scope, String alias) {}
}
