package org.savonitar.flink.stability.core.spec.document;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** A deterministic, cross-file-validated collection of v1 specifications. */
public final class SpecificationCatalog {
    private final Map<String, ScenarioBundle> scenarios;
    private final Map<String, SuiteSpecification> suites;

    SpecificationCatalog(
            Map<String, ScenarioBundle> scenarios,
            Map<String, SuiteSpecification> suites) {
        this.scenarios = Collections.unmodifiableMap(new LinkedHashMap<>(scenarios));
        this.suites = Collections.unmodifiableMap(new LinkedHashMap<>(suites));
    }

    public Map<String, ScenarioBundle> scenarios() {
        return scenarios;
    }

    public Optional<ScenarioBundle> scenario(String name) {
        return Optional.ofNullable(scenarios.get(name));
    }

    public Map<String, SuiteSpecification> suites() {
        return suites;
    }

    public Optional<SuiteSpecification> suite(String name) {
        return Optional.ofNullable(suites.get(name));
    }
}
