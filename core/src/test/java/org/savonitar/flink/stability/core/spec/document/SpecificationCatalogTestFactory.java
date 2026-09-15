package org.savonitar.flink.stability.core.spec.document;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/** Test-only bridge for constructing catalogs while keeping the production constructor package-private. */
public final class SpecificationCatalogTestFactory {
    private SpecificationCatalogTestFactory() {}

    public static SpecificationCatalog catalog(
            SuiteSpecification suite, ScenarioBundle... bundles) {
        Map<String, ScenarioBundle> scenarios = new LinkedHashMap<>();
        Arrays.stream(bundles).forEach(bundle ->
                scenarios.put(bundle.scenario().name(), bundle));
        return new SpecificationCatalog(scenarios, Map.of(suite.name(), suite));
    }
}
