package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.resolution.ResolvedSuiteEntry;
import java.util.Objects;

/** One resolved suite entry with its artifacts prepared for execution. */
public final class PreparedSuiteEntry {
    private final ResolvedSuiteEntry suiteEntry;
    private final PreparedScenarioPlan scenario;

    PreparedSuiteEntry(ResolvedSuiteEntry suiteEntry, PreparedScenarioPlan scenario) {
        this.suiteEntry = Objects.requireNonNull(suiteEntry, "suiteEntry");
        this.scenario = Objects.requireNonNull(scenario, "scenario");
        if (suiteEntry.scenarioPlan() != scenario.scenarioPlan()) {
            throw new IllegalArgumentException(
                    "Prepared scenario must wrap the suite entry's resolved plan");
        }
    }

    public ResolvedSuiteEntry suiteEntry() {
        return suiteEntry;
    }

    public PreparedScenarioPlan scenario() {
        return scenario;
    }
}
