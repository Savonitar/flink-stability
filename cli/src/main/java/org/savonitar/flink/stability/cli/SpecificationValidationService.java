package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.artifact.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.artifact.PreparedSuitePlan;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedSuitePlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalog;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalogLoader;
import org.savonitar.flink.stability.core.spec.resolution.SuitePlanResolver;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

/** Loads, resolves, and prepares a selected v1 target without provisioning infrastructure. */
final class SpecificationValidationService {
    private final SpecificationCatalogLoader catalogLoader;
    private final V1ScenarioPreparationService scenarioPreparation;
    private final ExecutableScenarioPlanCompiler compiler;
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
        this.compiler = new ExecutableScenarioPlanCompiler();
        this.scenarioPreparation = new V1ScenarioPreparationService(
                catalogLoader, scenarioResolver, compiler, artifactResolver);
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
        try (var target = scenarioPreparation.prepare(
                catalogRoot, scenarioName, submitOverrides, artifactOptions)) {
            var prepared = target.owner();
            var bound = target.executionPlan();
            var resolved = prepared.scenarioPlan();
            var executable = bound.executablePlan();
            java.util.Optional<JsonNode> plan = java.util.Optional.empty();
            if (showPlan) {
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
        var executables = new ArrayList<ExecutableScenarioPlan>();
        var issues = new ArrayList<Diagnostic>();
        for (var entry : resolved.entries()) {
            try {
                executables.add(compiler.compile(entry.scenarioPlan()));
            } catch (SpecificationException failure) {
                failure.diagnostics().stream().map(issue -> issue.inSuiteEntry(entry.identity()))
                        .forEach(issues::add);
            }
        }
        if (!issues.isEmpty()) {
            throw new SpecificationException(SpecificationException.Stage.RUNNER_CAPABILITY, issues);
        }
        try (PreparedSuitePlan prepared = artifactResolver.resolve(resolved, artifactOptions)) {
            for (int index = 0; index < prepared.entries().size(); index++) {
                var entry = prepared.entries().get(index);
                try {
                    compiler.bind(entry.scenario(), executables.get(index));
                } catch (SpecificationException failure) {
                    failure.diagnostics().stream()
                            .map(issue -> issue.inSuiteEntry(entry.suiteEntry().identity()))
                            .forEach(issues::add);
                }
            }
            if (!issues.isEmpty()) {
                throw new SpecificationException(SpecificationException.Stage.RUNNER_CAPABILITY, issues);
            }
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
