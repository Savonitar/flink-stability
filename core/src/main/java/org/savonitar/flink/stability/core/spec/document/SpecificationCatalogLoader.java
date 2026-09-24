package org.savonitar.flink.stability.core.spec.document;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;

/** Recursively discovers v1 documents and validates their cross-file identities. */
public final class SpecificationCatalogLoader {
    private static final String YAML_SUFFIX = ".yaml";
    private static final String EXPECTED_SUFFIX = ".expected.yaml";

    private final SpecificationLoader documentLoader;

    public SpecificationCatalogLoader() {
        this(new SpecificationLoader());
    }

    SpecificationCatalogLoader(SpecificationLoader documentLoader) {
        this.documentLoader = documentLoader;
    }

    public SpecificationCatalog load(Path root) {
        Path normalizedRoot = root.toAbsolutePath().normalize();
        if (!Files.isDirectory(normalizedRoot, LinkOption.NOFOLLOW_LINKS)) {
            throw new SpecificationException(Stage.CATALOG, List.of(issue(normalizedRoot,
                    "catalog.root-not-directory", "$", "Discovery root must be a directory")));
        }

        List<LoadedSpecification> documents = discover(normalizedRoot);
        List<ScenarioSpecification> scenarios = specificationsOfType(documents, ScenarioSpecification.class);
        List<ExpectedResultSpecification> expectedResults =
                specificationsOfType(documents, ExpectedResultSpecification.class);
        List<SuiteSpecification> suites = specificationsOfType(documents, SuiteSpecification.class);

        List<Diagnostic> issues = new ArrayList<>();
        validateScenarioIdentities(scenarios, issues);
        validateExpectedResultIdentities(scenarios, expectedResults, issues);
        validateSuiteIdentities(suites, issues);
        validateSuiteMembership(suites, scenarios, issues);
        if (!issues.isEmpty()) {
            throw new SpecificationException(Stage.CATALOG, issues);
        }

        Map<Path, ExpectedResultSpecification> expectedByPath = expectedResults.stream()
                .collect(Collectors.toMap(ExpectedResultSpecification::source, Function.identity()));
        Map<String, ScenarioBundle> bundles = new LinkedHashMap<>();
        scenarios.stream()
                .sorted(Comparator.comparing(ScenarioSpecification::name))
                .forEach(scenario -> bundles.put(scenario.name(),
                        new ScenarioBundle(scenario, expectedByPath.get(expectedSibling(scenario.source())))));

        Map<String, SuiteSpecification> suitesByName = new LinkedHashMap<>();
        suites.stream()
                .sorted(Comparator.comparing(SuiteSpecification::name))
                .forEach(suite -> suitesByName.put(suite.name(), suite));
        return new SpecificationCatalog(bundles, suitesByName);
    }

    private List<LoadedSpecification> discover(Path root) {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths
                    .filter(path -> !Files.isSymbolicLink(path))
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .filter(SpecificationCatalogLoader::isYaml)
                    .sorted(Comparator.comparing(Path::toString))
                    .map(documentLoader::load)
                    .toList();
        } catch (UncheckedIOException exception) {
            throw catalogIoFailure(root, exception.getCause());
        } catch (IOException exception) {
            throw catalogIoFailure(root, exception);
        }
    }

    private void validateScenarioIdentities(
            List<ScenarioSpecification> scenarios, List<Diagnostic> issues) {
        scenarios.forEach(scenario -> {
            String filenameName = stripSuffix(scenario.source().getFileName().toString(), YAML_SUFFIX);
            if (!scenario.name().equals(filenameName)) {
                issues.add(issue(scenario.source(), "catalog.scenario-name-filename-mismatch", "$/meta/name",
                        "Scenario name '" + scenario.name() + "' must equal filename basename '"
                                + filenameName + "'"));
            }
        });

        groupByName(scenarios).forEach((name, duplicates) -> {
            if (duplicates.size() > 1) {
                duplicates.forEach(scenario -> issues.add(issue(scenario.source(),
                        "catalog.duplicate-scenario-name", "$/meta/name",
                        "Scenario name '" + name + "' is declared by multiple files")));
            }
        });
    }

    private void validateExpectedResultIdentities(
            List<ScenarioSpecification> scenarios,
            List<ExpectedResultSpecification> expectedResults,
            List<Diagnostic> issues) {
        Map<String, List<ScenarioSpecification>> scenariosByName = groupByName(scenarios);
        Map<Path, ExpectedResultSpecification> expectedByPath = expectedResults.stream()
                .collect(Collectors.toMap(ExpectedResultSpecification::source, Function.identity()));

        scenarios.forEach(scenario -> {
            Path siblingPath = expectedSibling(scenario.source());
            ExpectedResultSpecification sibling = expectedByPath.get(siblingPath);
            if (sibling == null) {
                issues.add(issue(scenario.source(), "catalog.expected-result-missing", "$",
                        "Scenario requires sibling " + siblingPath.getFileName()));
                return;
            }

            String target = expectedTarget(sibling);
            if (!scenario.name().equals(target)) {
                issues.add(issue(sibling.source(), "catalog.expected-result-scenario-mismatch", "$/meta/scenario",
                        "Sibling targets scenario '" + target + "' instead of '" + scenario.name() + "'"));
            }
            String requiredName = scenario.name() + ".expected";
            if (!requiredName.equals(sibling.name())) {
                issues.add(issue(sibling.source(), "catalog.expected-result-name-mismatch", "$/meta/name",
                        "Sibling name must be '" + requiredName + "'"));
            }
        });

        expectedResults.forEach(expected -> {
            String target = expectedTarget(expected);
            String requiredName = target + ".expected";
            if (!requiredName.equals(expected.name())) {
                issues.add(issue(expected.source(), "catalog.expected-result-name-mismatch", "$/meta/name",
                        "Expected-result name must be '" + requiredName + "'"));
            }
            String requiredFilename = target + EXPECTED_SUFFIX;
            if (!requiredFilename.equals(expected.source().getFileName().toString())) {
                issues.add(issue(expected.source(), "catalog.expected-result-path-mismatch", "$",
                        "Expected-result filename must be '" + requiredFilename + "'"));
            }

            List<ScenarioSpecification> targets = scenariosByName.getOrDefault(target, List.of());
            if (targets.isEmpty()) {
                issues.add(issue(expected.source(), "catalog.expected-result-orphan", "$/meta/scenario",
                        "No discovered scenario is named '" + target + "'"));
            } else if (targets.size() == 1) {
                Path requiredPath = expectedSibling(targets.getFirst().source());
                if (!expected.source().equals(requiredPath)) {
                    issues.add(issue(expected.source(), "catalog.expected-result-path-mismatch", "$",
                            "Expected result must be beside its scenario at " + requiredPath));
                }
            }
        });

        expectedResults.stream()
                .collect(Collectors.groupingBy(SpecificationCatalogLoader::expectedTarget))
                .forEach((target, duplicates) -> {
                    if (duplicates.size() > 1) {
                        duplicates.forEach(expected -> issues.add(issue(expected.source(),
                                "catalog.expected-result-duplicate-target", "$/meta/scenario",
                                "Multiple expected-result documents target scenario '" + target + "'")));
                    }
                });
    }

    private void validateSuiteIdentities(List<SuiteSpecification> suites, List<Diagnostic> issues) {
        groupByName(suites).forEach((name, duplicates) -> {
            if (duplicates.size() > 1) {
                duplicates.forEach(suite -> issues.add(issue(suite.source(), "catalog.duplicate-suite-name",
                        "$/meta/name", "Suite name '" + name + "' is declared by multiple files")));
            }
        });
    }

    private void validateSuiteMembership(
            List<SuiteSpecification> suites,
            List<ScenarioSpecification> scenarios,
            List<Diagnostic> issues) {
        Set<String> scenarioNames = scenarios.stream()
                .map(ScenarioSpecification::name)
                .collect(Collectors.toSet());
        suites.forEach(suite -> {
            List<SuiteEntryDeclaration> entries = suiteEntries(suite);
            entries.stream()
                    .filter(entry -> !scenarioNames.contains(entry.scenarioName()))
                    .forEach(entry -> issues.add(issue(
                            suite.source(),
                            "catalog.suite-scenario-not-found",
                            entry.path() + "/scenario",
                            "Suite references undiscovered scenario '"
                                    + entry.scenarioName() + "'")));

            Set<Integer> entriesMissingRequiredAlias = entries.stream()
                    .collect(Collectors.groupingBy(SuiteEntryDeclaration::scenarioName))
                    .values().stream()
                    .filter(group -> group.size() > 1)
                    .flatMap(List::stream)
                    .filter(entry -> entry.explicitId() == null)
                    .peek(entry -> issues.add(issue(
                            suite.source(),
                            "catalog.suite-entry-alias-required",
                            entry.path() + "/as",
                            "Scenario '" + entry.scenarioName()
                                    + "' appears more than once; every occurrence requires 'as'")))
                    .map(SuiteEntryDeclaration::index)
                    .collect(Collectors.toSet());

            entries.stream()
                    .filter(entry -> !entriesMissingRequiredAlias.contains(entry.index()))
                    .collect(Collectors.groupingBy(SuiteEntryDeclaration::effectiveId))
                    .values().stream()
                    .filter(group -> group.size() > 1)
                    .flatMap(List::stream)
                    .forEach(entry -> issues.add(issue(
                            suite.source(),
                            "catalog.suite-duplicate-entry-id",
                            entry.path() + (entry.explicitId() == null ? "/scenario" : "/as"),
                            "Suite entry ID '" + entry.effectiveId()
                                    + "' is used by multiple entries")));
        });
    }

    private static List<SuiteEntryDeclaration> suiteEntries(SuiteSpecification suite) {
        ArrayNode nodes = (ArrayNode) suite.document().get("scenarios");
        List<SuiteEntryDeclaration> entries = new ArrayList<>();
        for (int index = 0; index < nodes.size(); index++) {
            ObjectNode node = (ObjectNode) nodes.get(index);
            String scenarioName = node.path("scenario").textValue();
            String explicitId = node.has("as") ? node.path("as").textValue() : null;
            entries.add(new SuiteEntryDeclaration(
                    index,
                    scenarioName,
                    explicitId,
                    explicitId == null ? scenarioName : explicitId,
                    "$/scenarios/" + index));
        }
        return List.copyOf(entries);
    }

    private static <T extends LoadedSpecification> List<T> specificationsOfType(
            List<LoadedSpecification> documents, Class<T> type) {
        return documents.stream().filter(type::isInstance).map(type::cast).toList();
    }

    private static <T extends LoadedSpecification> Map<String, List<T>> groupByName(List<T> documents) {
        return documents.stream().collect(Collectors.groupingBy(LoadedSpecification::name));
    }

    private static boolean isYaml(Path path) {
        return path.getFileName().toString().endsWith(YAML_SUFFIX);
    }

    private static String expectedTarget(ExpectedResultSpecification expectedResult) {
        return expectedResult.at("/meta/scenario").textValue();
    }

    private static Path expectedSibling(Path scenarioPath) {
        String filename = scenarioPath.getFileName().toString();
        String basename = stripSuffix(filename, YAML_SUFFIX);
        return scenarioPath.resolveSibling(basename + EXPECTED_SUFFIX).toAbsolutePath().normalize();
    }

    private static String stripSuffix(String value, String suffix) {
        return value.substring(0, value.length() - suffix.length());
    }

    private static Diagnostic issue(Path source, String code, String path, String message) {
        return new Diagnostic(source, code, path, message);
    }

    private static SpecificationException catalogIoFailure(Path root, IOException exception) {
        return new SpecificationException(Stage.CATALOG, List.of(issue(root, "catalog.io-error", "$",
                exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage())));
    }

    private record SuiteEntryDeclaration(
            int index,
            String scenarioName,
            String explicitId,
            String effectiveId,
            String path) {}
}
