package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.execution.V1AttemptContext;
import org.savonitar.flink.stability.core.execution.V1ScenarioExecutionResult;
import org.savonitar.flink.stability.core.execution.V1ScenarioExecutor;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionException;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.spec.document.CatalogValidationException;
import org.savonitar.flink.stability.core.spec.document.DocumentValidationException;
import org.savonitar.flink.stability.core.spec.resolution.ExpectedResultSelectionException;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightException;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioResolutionException;
import org.savonitar.flink.stability.core.execution.plan.RunnerCapabilityException;
import org.savonitar.flink.stability.testcontainers.DockerV1AttemptRuntime;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/** Runs exactly one bounded scenario through the typed v1 execution path. */
@CommandLine.Command(
        name = "run",
        mixinStandardHelpOptions = true,
        description = "Prepare and execute one bounded v1 scenario exactly once.")
public final class RunScenarioCommand implements Callable<Integer> {
    private final ScenarioPreparation preparation;
    private final AttemptContextCreation attemptContexts;
    private final V1ExecutionResultRenderer resultRenderer;
    private final ValidationDiagnosticRenderer diagnosticRenderer;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec specification;

    @CommandLine.Option(
            names = "--catalog-root",
            required = true,
            paramLabel = "DIR",
            description = "Root directory recursively containing the v1 specification catalog.")
    private Path catalogRoot;

    @CommandLine.Option(
            names = "--scenario",
            required = true,
            paramLabel = "NAME",
            description = "Run one scenario selected by meta.name.")
    private String scenario;

    @CommandLine.Option(
            names = {"-p", "--parameter"},
            paramLabel = "NAME=VALUE",
            description = "Submit-time scalar parameter override; repeat for multiple values.")
    private List<String> parameterAssignments = new ArrayList<>();

    @CommandLine.Option(
            names = "--artifact-root",
            defaultValue = ".",
            paramLabel = "DIR",
            description = "Base directory for local artifact references (default: current directory).")
    private Path artifactRoot;

    @CommandLine.Option(
            names = "--offline",
            description = "Resolve Maven artifacts from the local cache only.")
    private boolean offline;

    public RunScenarioCommand() {
        this(
                productionPreparation(),
                new V1AttemptContextFactory()::create,
                new V1ExecutionResultRenderer(),
                new ValidationDiagnosticRenderer());
    }

    RunScenarioCommand(
            ScenarioPreparation preparation,
            AttemptContextCreation attemptContexts,
            V1ExecutionResultRenderer resultRenderer,
            ValidationDiagnosticRenderer diagnosticRenderer) {
        this.preparation = Objects.requireNonNull(preparation, "preparation");
        this.attemptContexts = Objects.requireNonNull(attemptContexts, "attemptContexts");
        this.resultRenderer = Objects.requireNonNull(resultRenderer, "resultRenderer");
        this.diagnosticRenderer = Objects.requireNonNull(
                diagnosticRenderer, "diagnosticRenderer");
    }

    @Override
    public Integer call() {
        Map<String, JsonNode> overrides = CliParameterParser.parse(
                parameterAssignments,
                specification.commandLine());
        ArtifactResolutionOptions artifactOptions =
                new ArtifactResolutionOptions(artifactRoot, offline);

        try {
            V1AttemptContext context;
            V1ScenarioExecutionResult result;
            PreparedExecution prepared = preparation.prepare(
                    catalogRoot, scenario, overrides, artifactOptions);
            try {
                // Preparation (including capability and artifact checks) is complete before this
                // context can be handed to the Docker-backed executor.
                context = attemptContexts.create();
                result = Objects.requireNonNull(
                        prepared.execute(context), "scenario execution returned null");
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    prepared.close();
                } catch (RuntimeException | Error cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
            try {
                prepared.close();
            } catch (RuntimeException cleanupFailure) {
                result = result.withPreparedArtifactCleanupFailure(cleanupFailure);
            }
            specification.commandLine().getOut().println(
                    resultRenderer.render(scenario, context, result));
            return result.status() == V1ScenarioExecutionResult.Status.PASS
                    ? CommandLine.ExitCode.OK
                    : CommandLine.ExitCode.SOFTWARE;
        } catch (UnknownSpecificationTargetException failure) {
            specification.commandLine().getErr().println("error: " + failure.getMessage());
            return CommandLine.ExitCode.USAGE;
        } catch (DocumentValidationException
                | CatalogValidationException
                | ScenarioResolutionException
                | ExpectedResultSelectionException
                | ScenarioPreflightException
                | ArtifactResolutionException
                | RunnerCapabilityException failure) {
            diagnosticRenderer.render(failure, catalogRoot)
                    .forEach(specification.commandLine().getErr()::println);
            return CommandLine.ExitCode.SOFTWARE;
        } catch (IOException failure) {
            specification.commandLine().getErr().println(
                    "error: could not allocate run checkpoint storage: "
                            + failure.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        } catch (UncheckedIOException failure) {
            specification.commandLine().getErr().println(
                    "error: prepared artifact cleanup failed: " + failure.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        } catch (RuntimeException failure) {
            specification.commandLine().getErr().println(
                    "error: run failed: " + failure.getClass().getSimpleName()
                            + ": " + String.valueOf(failure.getMessage()));
            return CommandLine.ExitCode.SOFTWARE;
        }
    }

    private static ScenarioPreparation productionPreparation() {
        V1ScenarioPreparationService preparationService =
                new V1ScenarioPreparationService();
        V1ScenarioExecutor executor = new V1ScenarioExecutor(DockerV1AttemptRuntime::new);
        return (catalogRoot, scenarioName, overrides, artifactOptions) -> {
            V1ScenarioPreparationService.PreparedTarget target = preparationService.prepare(
                    catalogRoot, scenarioName, overrides, artifactOptions);
            return new PreparedExecution() {
                @Override
                public V1ScenarioExecutionResult execute(V1AttemptContext context) {
                    return executor.execute(target.executionPlan(), context);
                }

                @Override
                public void close() {
                    target.close();
                }
            };
        };
    }

    @FunctionalInterface
    interface ScenarioPreparation {
        PreparedExecution prepare(
                Path catalogRoot,
                String scenarioName,
                Map<String, ? extends JsonNode> submitOverrides,
                ArtifactResolutionOptions artifactOptions);
    }

    interface PreparedExecution extends AutoCloseable {
        V1ScenarioExecutionResult execute(V1AttemptContext context);

        @Override
        void close();
    }

    @FunctionalInterface
    interface AttemptContextCreation {
        V1AttemptContext create() throws IOException;
    }
}
