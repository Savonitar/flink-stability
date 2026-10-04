package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.artifact.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.artifact.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.artifact.PreparedSuitePlan;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedSuitePlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalog;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalogLoader;
import org.savonitar.flink.stability.core.spec.resolution.SuitePlanResolver;

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
        return validateScenario(catalogRoot, scenarioName, submitOverrides, artifactOptions, false);
    }

    ValidationSummary validateScenario(Path catalogRoot, String scenarioName,
            Map<String, ? extends JsonNode> submitOverrides, ArtifactResolutionOptions artifactOptions,
            boolean showPlan) {
        Objects.requireNonNull(scenarioName, "scenarioName");
        SpecificationCatalog catalog = load(catalogRoot);
        var bundle = catalog.scenario(scenarioName).orElseThrow(() -> unknown(
                "scenario", scenarioName, catalog.scenarios().keySet()));
        ResolvedScenarioPlan resolved = scenarioResolver.resolve(
                bundle,
                new ResolutionRequest(Map.of(), submitOverrides));
        try (PreparedScenarioPlan prepared = artifactResolver.resolve(resolved, artifactOptions)) {
            java.util.Optional<JsonNode> plan = java.util.Optional.empty();
            if (showPlan) {
                var compiler = new org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler();
                var executable = compiler.compile(resolved);
                var bound = compiler.bind(prepared, executable);
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                var rendered = mapper.createObjectNode();
                rendered.put("scenario", scenarioName);
                rendered.set("resolved", resolved.scenario().side(
                        org.savonitar.flink.stability.core.spec.resolution.ScenarioSide.SINGLE).document());
                if (executable.kafka().runtimeTarget().customConfiguration()) {
                    var kafka = rendered.putObject("kafkaRuntime");
                    kafka.put("launchType", executable.kafka().launchType());
                    if ("generic-kraft".equals(executable.kafka().launchType()))
                        kafka.put("layout", executable.kafka().layout());
                    executable.kafka().imageId().ifPresent(value -> kafka.put("imageId", value));
                    kafka.set("brokerConfig", mapper.valueToTree(executable.kafka().brokerConfig()));
                    kafka.set("launches", mapper.valueToTree(executable.kafka().runtimeTarget().resolvedLaunches()));
                }
                rendered.set("flinkConfig", mapper.valueToTree(bound.flinkRuntimeTarget().config()));
                CustomRuntimeSubjectEvidence.from(bound).ifPresent(value -> rendered.set("customRuntimeSubject", value));
                plan = java.util.Optional.of(rendered);
            }
            return new ValidationSummary(ValidationSummary.TargetKind.SCENARIO,
                    scenarioName, 1, prepared.artifacts().size(), plan);
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
