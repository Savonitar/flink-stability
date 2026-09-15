package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.artifact.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.artifact.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalog;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalogLoader;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler;
import org.savonitar.flink.stability.core.execution.plan.PreparedExecutableScenarioPlan;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/** Resolves capabilities before artifacts, then binds one caller-owned prepared v1 plan. */
final class V1ScenarioPreparationService {
    private final SpecificationCatalogLoader catalogLoader;
    private final ScenarioPlanResolver scenarioResolver;
    private final ExecutableScenarioPlanCompiler compiler;
    private final ArtifactPlanResolver artifactResolver;

    V1ScenarioPreparationService() {
        this(
                new SpecificationCatalogLoader(),
                new ScenarioPlanResolver(),
                new ExecutableScenarioPlanCompiler(),
                new ArtifactPlanResolver());
    }

    V1ScenarioPreparationService(
            SpecificationCatalogLoader catalogLoader,
            ScenarioPlanResolver scenarioResolver,
            ExecutableScenarioPlanCompiler compiler,
            ArtifactPlanResolver artifactResolver) {
        this.catalogLoader = Objects.requireNonNull(catalogLoader, "catalogLoader");
        this.scenarioResolver = Objects.requireNonNull(scenarioResolver, "scenarioResolver");
        this.compiler = Objects.requireNonNull(compiler, "compiler");
        this.artifactResolver = Objects.requireNonNull(artifactResolver, "artifactResolver");
    }

    PreparedTarget prepare(
            Path catalogRoot,
            String scenarioName,
            Map<String, ? extends JsonNode> submitOverrides,
            ArtifactResolutionOptions artifactOptions) {
        Objects.requireNonNull(catalogRoot, "catalogRoot");
        Objects.requireNonNull(scenarioName, "scenarioName");
        Objects.requireNonNull(submitOverrides, "submitOverrides");
        Objects.requireNonNull(artifactOptions, "artifactOptions");

        SpecificationCatalog catalog = catalogLoader.load(catalogRoot);
        var bundle = catalog.scenario(scenarioName).orElseThrow(() ->
                new UnknownSpecificationTargetException(
                        "Catalog contains no scenario '" + scenarioName
                                + "'; available scenarios: "
                                + catalog.scenarios().keySet().stream().sorted().toList()));
        ResolvedScenarioPlan resolved = scenarioResolver.resolve(
                bundle, new ResolutionRequest(Map.of(), submitOverrides));

        // This check intentionally precedes any Maven lookup, file staging, or Docker work.
        ExecutableScenarioPlan executable = compiler.compile(resolved);
        PreparedScenarioPlan prepared = artifactResolver.resolve(resolved, artifactOptions);
        try {
            PreparedExecutableScenarioPlan bound = compiler.bind(prepared, executable);
            return new PreparedTarget(prepared, bound);
        } catch (RuntimeException failure) {
            try {
                prepared.close();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    record PreparedTarget(
            PreparedScenarioPlan owner,
            PreparedExecutableScenarioPlan executionPlan) implements AutoCloseable {
        PreparedTarget {
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(executionPlan, "executionPlan");
            if (executionPlan.preparedScenarioPlan() != owner) {
                throw new IllegalArgumentException(
                        "Execution plan must retain the same prepared owner");
            }
        }

        @Override
        public void close() {
            owner.close();
        }
    }
}
