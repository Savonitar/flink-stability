package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.spec.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.spec.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.spec.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.spec.PreparedSuitePlan;
import org.savonitar.flink.stability.core.spec.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.ResolvedSuitePlan;
import org.savonitar.flink.stability.core.spec.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.SpecificationCatalog;
import org.savonitar.flink.stability.core.spec.SpecificationCatalogLoader;
import org.savonitar.flink.stability.core.spec.SuitePlanResolver;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/** Loads, resolves, and prepares a selected v1 target without provisioning infrastructure. */
final class SpecificationValidationService {
    private final SpecificationCatalogLoader catalogLoader;
    private final ScenarioPlanResolver scenarioResolver;
    private final SuitePlanResolver suiteResolver;
    private final ArtifactPlanResolver artifactResolver;

    SpecificationValidationService() {
        this(
                new SpecificationCatalogLoader(),
                new ScenarioPlanResolver(),
                new SuitePlanResolver(),
                new ArtifactPlanResolver());
    }

    SpecificationValidationService(
            SpecificationCatalogLoader catalogLoader,
            ScenarioPlanResolver scenarioResolver,
            SuitePlanResolver suiteResolver,
            ArtifactPlanResolver artifactResolver) {
        this.catalogLoader = Objects.requireNonNull(catalogLoader, "catalogLoader");
        this.scenarioResolver = Objects.requireNonNull(scenarioResolver, "scenarioResolver");
        this.suiteResolver = Objects.requireNonNull(suiteResolver, "suiteResolver");
        this.artifactResolver = Objects.requireNonNull(artifactResolver, "artifactResolver");
    }

    ValidationSummary validateScenario(
            Path catalogRoot,
            String scenarioName,
            Map<String, ? extends JsonNode> submitOverrides,
            ArtifactResolutionOptions artifactOptions) {
        Objects.requireNonNull(scenarioName, "scenarioName");
        SpecificationCatalog catalog = load(catalogRoot);
        var bundle = catalog.scenario(scenarioName).orElseThrow(() -> unknown(
                "scenario", scenarioName, catalog.scenarios().keySet()));
        ResolvedScenarioPlan resolved = scenarioResolver.resolve(
                bundle,
                new ResolutionRequest(Map.of(), submitOverrides));
        try (PreparedScenarioPlan prepared = artifactResolver.resolve(resolved, artifactOptions)) {
            return new ValidationSummary(
                    ValidationSummary.TargetKind.SCENARIO,
                    scenarioName,
                    1,
                    prepared.artifacts().size());
        }
    }

    ValidationSummary validateSuite(
            Path catalogRoot,
            String suiteName,
            Map<String, ? extends JsonNode> submitOverrides,
            ArtifactResolutionOptions artifactOptions) {
        Objects.requireNonNull(suiteName, "suiteName");
        SpecificationCatalog catalog = load(catalogRoot);
        if (catalog.suite(suiteName).isEmpty()) {
            throw unknown("suite", suiteName, catalog.suites().keySet());
        }
        ResolvedSuitePlan resolved = suiteResolver.resolve(catalog, suiteName, submitOverrides);
        try (PreparedSuitePlan prepared = artifactResolver.resolve(resolved, artifactOptions)) {
            int artifactCount = prepared.entries().stream()
                    .mapToInt(entry -> entry.scenario().artifacts().size())
                    .sum();
            return new ValidationSummary(
                    ValidationSummary.TargetKind.SUITE,
                    suiteName,
                    prepared.entries().size(),
                    artifactCount);
        }
    }

    private SpecificationCatalog load(Path catalogRoot) {
        return catalogLoader.load(Objects.requireNonNull(catalogRoot, "catalogRoot"));
    }

    private static UnknownSpecificationTargetException unknown(
            String kind,
            String name,
            java.util.Set<String> available) {
        return new UnknownSpecificationTargetException(
                "Catalog contains no " + kind + " '" + name + "'; available " + kind
                        + "s: " + available.stream().sorted().toList());
    }
}
