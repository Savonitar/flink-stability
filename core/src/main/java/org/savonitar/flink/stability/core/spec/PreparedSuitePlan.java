package org.savonitar.flink.stability.core.spec;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A complete suite plan whose entries and artifacts all passed preflight. */
public final class PreparedSuitePlan implements AutoCloseable {
    private final ResolvedSuitePlan suitePlan;
    private final ArtifactWorkspace workspace;
    private final List<PreparedSuiteEntry> entries;

    PreparedSuitePlan(
            ResolvedSuitePlan suitePlan,
            ArtifactWorkspace workspace,
            List<PreparedSuiteEntry> entries) {
        this.suitePlan = Objects.requireNonNull(suitePlan, "suitePlan");
        this.workspace = Objects.requireNonNull(workspace, "workspace");
        this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (this.entries.size() != suitePlan.entries().size()) {
            throw new IllegalArgumentException("Prepared suite must contain every resolved entry");
        }
        for (int index = 0; index < this.entries.size(); index++) {
            PreparedSuiteEntry prepared = this.entries.get(index);
            if (prepared.suiteEntry() != suitePlan.entries().get(index)
                    || prepared.scenario().workspace() != this.workspace) {
                throw new IllegalArgumentException(
                        "Prepared suite entries must preserve source order and artifact root");
            }
        }
    }

    public ResolvedSuitePlan suitePlan() {
        return suitePlan;
    }

    public Path artifactRoot() {
        return workspace.artifactRoot();
    }

    public Path preparationRoot() {
        return workspace.preparationRoot();
    }

    public List<PreparedSuiteEntry> entries() {
        return entries;
    }

    public Optional<PreparedSuiteEntry> entry(String entryId) {
        Objects.requireNonNull(entryId, "entryId");
        return entries.stream()
                .filter(entry -> entry.suiteEntry().identity().entryId().equals(entryId))
                .findFirst();
    }

    @Override
    public void close() {
        workspace.close();
    }
}
