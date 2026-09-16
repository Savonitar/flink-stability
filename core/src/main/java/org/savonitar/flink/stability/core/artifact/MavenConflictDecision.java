package org.savonitar.flink.stability.core.artifact;

import java.util.List;
import java.util.Objects;

/** Complete evidence for one Maven graph node omitted by version mediation. */
public record MavenConflictDecision(
        MavenArtifactIdentity omitted,
        MavenArtifactIdentity winner,
        Reason reason,
        int omittedDepth,
        int winnerDepth,
        int omittedOriginRootIndex,
        int winnerOriginRootIndex,
        List<MavenArtifactIdentity> omittedPath,
        List<MavenArtifactIdentity> winnerPath) {
    public enum Reason {
        NEAREST,
        FIRST_BREADTH_FIRST
    }

    public MavenConflictDecision {
        Objects.requireNonNull(omitted, "omitted");
        Objects.requireNonNull(winner, "winner");
        Objects.requireNonNull(reason, "reason");
        if (!omitted.conflictKey().equals(winner.conflictKey())) {
            throw new IllegalArgumentException("omitted and winner must have one conflict key");
        }
        if (omittedDepth < 0 || winnerDepth < 0) {
            throw new IllegalArgumentException("conflict depths must be non-negative");
        }
        if (winnerDepth > omittedDepth) {
            throw new IllegalArgumentException("A conflict winner must not be farther away");
        }
        if (reason == Reason.NEAREST && winnerDepth >= omittedDepth) {
            throw new IllegalArgumentException(
                    "NEAREST requires the winner to have a smaller depth");
        }
        if (reason == Reason.FIRST_BREADTH_FIRST && winnerDepth != omittedDepth) {
            throw new IllegalArgumentException(
                    "FIRST_BREADTH_FIRST requires equal conflict depths");
        }
        if (omittedOriginRootIndex < 0 || winnerOriginRootIndex < 0) {
            throw new IllegalArgumentException("conflict root indexes must be non-negative");
        }
        omittedPath = checkedPath(omittedPath, omitted, "omittedPath");
        winnerPath = checkedPath(winnerPath, winner, "winnerPath");
        if (omittedPath.size() != omittedDepth + 1) {
            throw new IllegalArgumentException(
                    "omittedPath length must equal omittedDepth + 1");
        }
        if (winnerPath.size() != winnerDepth + 1) {
            throw new IllegalArgumentException(
                    "winnerPath length must equal winnerDepth + 1");
        }
    }

    private static List<MavenArtifactIdentity> checkedPath(
            List<MavenArtifactIdentity> path,
            MavenArtifactIdentity endpoint,
            String name) {
        List<MavenArtifactIdentity> copy = List.copyOf(Objects.requireNonNull(path, name));
        if (copy.isEmpty() || !endpoint.equals(copy.get(copy.size() - 1))) {
            throw new IllegalArgumentException(name + " must be non-empty and end at its artifact");
        }
        return copy;
    }
}
