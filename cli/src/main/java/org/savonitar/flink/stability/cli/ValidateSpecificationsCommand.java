package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.artifact.ArtifactResolutionException;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.spec.document.CatalogValidationException;
import org.savonitar.flink.stability.core.spec.document.DocumentValidationException;
import org.savonitar.flink.stability.core.spec.resolution.ExpectedResultSelectionException;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightException;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioResolutionException;
import org.savonitar.flink.stability.core.spec.resolution.SuitePlanningException;
import picocli.CommandLine;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "validate",
        mixinStandardHelpOptions = true,
        description = "Validate and prepare a v1 scenario or suite without provisioning Docker.")
public final class ValidateSpecificationsCommand implements Callable<Integer> {
    private final SpecificationValidationService validationService;
    private final ValidationDiagnosticRenderer diagnosticRenderer;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec specification;

    @CommandLine.Option(
            names = "--catalog-root",
            required = true,
            paramLabel = "DIR",
            description = "Root directory recursively containing the v1 specification catalog.")
    private Path catalogRoot;

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    private Target target;

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

    public ValidateSpecificationsCommand() {
        this(new SpecificationValidationService(), new ValidationDiagnosticRenderer());
    }

    ValidateSpecificationsCommand(
            SpecificationValidationService validationService,
            ValidationDiagnosticRenderer diagnosticRenderer) {
        this.validationService = validationService;
        this.diagnosticRenderer = diagnosticRenderer;
    }

    @Override
    public Integer call() {
        var overrides = CliParameterParser.parse(
                parameterAssignments,
                specification.commandLine());
        ArtifactResolutionOptions artifactOptions =
                new ArtifactResolutionOptions(artifactRoot, offline);
        try {
            ValidationSummary summary = target.scenario != null
                    ? validationService.validateScenario(
                            catalogRoot, target.scenario, overrides, artifactOptions)
                    : validationService.validateSuite(
                            catalogRoot, target.suite, overrides, artifactOptions);
            specification.commandLine().getOut().println(success(summary));
            return CommandLine.ExitCode.OK;
        } catch (UnknownSpecificationTargetException exception) {
            specification.commandLine().getErr().println("error: " + exception.getMessage());
            return CommandLine.ExitCode.USAGE;
        } catch (UncheckedIOException exception) {
            specification.commandLine().getErr().println(
                    "error: prepared artifact cleanup failed: " + exception.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        } catch (DocumentValidationException
                | CatalogValidationException
                | ScenarioResolutionException
                | ExpectedResultSelectionException
                | ScenarioPreflightException
                | ArtifactResolutionException
                | SuitePlanningException exception) {
            diagnosticRenderer.render(exception, catalogRoot)
                    .forEach(specification.commandLine().getErr()::println);
            return CommandLine.ExitCode.SOFTWARE;
        }
    }

    private static String success(ValidationSummary summary) {
        String details = summary.kind() == ValidationSummary.TargetKind.SCENARIO
                ? summary.artifacts() + " artifact" + plural(summary.artifacts())
                : summary.entries() + " entr" + (summary.entries() == 1 ? "y" : "ies")
                        + ", " + summary.artifacts() + " artifact" + plural(summary.artifacts());
        return "valid: " + summary.kind().displayName() + " '" + summary.name()
                + "' (" + details + ")";
    }

    private static String plural(int count) {
        return count == 1 ? "" : "s";
    }

    static final class Target {
        @CommandLine.Option(names = "--scenario", paramLabel = "NAME",
                description = "Validate one scenario selected by meta.name.")
        private String scenario;

        @CommandLine.Option(names = "--suite", paramLabel = "NAME",
                description = "Validate every entry in one suite selected by meta.name.")
        private String suite;
    }
}
