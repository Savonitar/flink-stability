package org.savonitar.flink.stability.core.spec.document;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.savonitar.flink.stability.core.spec.document.SpecificationAssertions.assertFailsAt;

class SpecificationCatalogLoaderTest {
    private final SpecificationCatalogLoader loader = new SpecificationCatalogLoader();

    @TempDir
    Path temporaryDirectory;

    @Test
    void recursivelyBuildsScenarioBundlesWithoutExecutingExpectedResults() throws IOException {
        Path nested = Files.createDirectories(temporaryDirectory.resolve("kafka/eos"));
        copyResource("minimal.yaml", nested.resolve("minimal.yaml"));
        copyResource("minimal.expected.yaml", nested.resolve("minimal.expected.yaml"));
        copyResource("smoke-suite.yaml", temporaryDirectory.resolve("smoke-suite.yaml"));
        Files.writeString(temporaryDirectory.resolve("notes.md"), "not a specification");

        SpecificationCatalog catalog = loader.load(temporaryDirectory);

        assertEquals(1, catalog.scenarios().size());
        ScenarioBundle bundle = catalog.scenario("minimal").orElseThrow();
        assertEquals("minimal", bundle.scenario().name());
        assertEquals("minimal.expected", bundle.expectedResult().name());
        assertEquals(1, catalog.suites().size());
        assertTrue(catalog.suite("smoke-suite").isPresent());
        assertFalse(catalog.scenario("minimal.expected").isPresent());
    }

    @Test
    void rejectsMissingExpectedResultSibling() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("minimal.yaml"));

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-missing", "minimal.yaml");
    }

    @Test
    void rejectsExpectedResultTargetMismatch() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("minimal.yaml"));
        ObjectNode expected = expectedResultFor("another-scenario");
        YamlTestDocuments.write(temporaryDirectory.resolve("minimal.expected.yaml"), expected);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-scenario-mismatch", "minimal.expected.yaml");
        assertHasIssue(exception, "catalog.expected-result-orphan", "minimal.expected.yaml");
    }

    @Test
    void rejectsExpectedResultNameMismatch() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("minimal.yaml"));
        ObjectNode expected = YamlTestDocuments.read(resource("minimal.expected.yaml"));
        ObjectNode meta = (ObjectNode) expected.required("meta");
        meta.required("name");
        meta.put("name", "wrong.expected");
        YamlTestDocuments.write(temporaryDirectory.resolve("minimal.expected.yaml"), expected);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-name-mismatch", "minimal.expected.yaml");
        assertEquals(1, countIssues(exception, "catalog.expected-result-name-mismatch",
                "minimal.expected.yaml"));
    }

    @Test
    void rejectsScenarioFilenameMismatch() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("different-name.yaml"));
        copyResource("minimal.expected.yaml", temporaryDirectory.resolve("different-name.expected.yaml"));

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.scenario-name-filename-mismatch", "different-name.yaml");
    }

    @Test
    void rejectsOrphanExpectedResult() throws IOException {
        ObjectNode expected = expectedResultFor("ghost");
        YamlTestDocuments.write(temporaryDirectory.resolve("ghost.expected.yaml"), expected);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-orphan", "ghost.expected.yaml");
    }

    @Test
    void rejectsMultipleExpectedResultsTargetingOneScenario() throws IOException {
        copyValidPair(temporaryDirectory);
        copyResource("minimal.expected.yaml", temporaryDirectory.resolve("duplicate.expected.yaml"));

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-duplicate-target", "minimal.expected.yaml");
        assertHasIssue(exception, "catalog.expected-result-duplicate-target", "duplicate.expected.yaml");
    }

    @Test
    void rejectsDuplicateGlobalScenarioNamesAcrossDirectories() throws IOException {
        Path first = Files.createDirectories(temporaryDirectory.resolve("first"));
        Path second = Files.createDirectories(temporaryDirectory.resolve("second"));
        copyValidPair(first);
        copyValidPair(second);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertEquals(2, exception.diagnostics().stream()
                .filter(issue -> issue.code().equals("catalog.duplicate-scenario-name"))
                .count());
    }

    @Test
    void rejectsDuplicateSuiteNames() throws IOException {
        copyResource("smoke-suite.yaml", temporaryDirectory.resolve("first.yaml"));
        Path nested = Files.createDirectories(temporaryDirectory.resolve("nested"));
        copyResource("smoke-suite.yaml", nested.resolve("second.yaml"));

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertEquals(2, exception.diagnostics().stream()
                .filter(issue -> issue.code().equals("catalog.duplicate-suite-name"))
                .count());
    }

    @Test
    void rejectsSuiteEntryThatReferencesUndiscoveredScenario() throws IOException {
        writeSuite("""
                  - scenario: missing-scenario
                """);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertExactSuiteIssues(exception,
                "catalog.suite-scenario-not-found@$/scenarios/0/scenario");
    }

    @Test
    void requiresExplicitAliasOnEveryOccurrenceOfRepeatedScenario() throws IOException {
        copyValidPair(temporaryDirectory);
        writeSuite("""
                  - scenario: minimal
                  - scenario: minimal
                    as: first-run
                  - scenario: minimal
                """);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertExactSuiteIssues(exception,
                "catalog.suite-entry-alias-required@$/scenarios/0/as",
                "catalog.suite-entry-alias-required@$/scenarios/2/as");
    }

    @Test
    void rejectsDuplicateExplicitSuiteEntryIds() throws IOException {
        copyValidPair(temporaryDirectory);
        copyValidPairNamed("alternate");
        writeSuite("""
                  - scenario: minimal
                    as: shared-entry
                  - scenario: alternate
                    as: shared-entry
                """);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertExactSuiteIssues(exception,
                "catalog.suite-duplicate-entry-id@$/scenarios/0/as",
                "catalog.suite-duplicate-entry-id@$/scenarios/1/as");
    }

    @Test
    void rejectsDefaultSuiteEntryIdCollidingWithExplicitIdForAnotherScenario() throws IOException {
        copyValidPair(temporaryDirectory);
        copyValidPairNamed("alternate");
        writeSuite("""
                  - scenario: minimal
                  - scenario: alternate
                    as: minimal
                """);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertExactSuiteIssues(exception,
                "catalog.suite-duplicate-entry-id@$/scenarios/0/scenario",
                "catalog.suite-duplicate-entry-id@$/scenarios/1/as");
    }

    @Test
    void acceptsRepeatedScenarioWithDistinctExplicitAliases() throws IOException {
        copyValidPair(temporaryDirectory);
        writeSuite("""
                  - scenario: minimal
                    as: first-run
                  - scenario: minimal
                    as: second-run
                """);

        SpecificationCatalog catalog = loader.load(temporaryDirectory);

        SuiteSpecification suite = catalog.suite("membership-suite").orElseThrow();
        assertEquals("first-run", suite.at("/scenarios/0/as").textValue());
        assertEquals("second-run", suite.at("/scenarios/1/as").textValue());
    }

    @Test
    void doesNotFollowSpecificationSymlinks() throws IOException {
        Path outsideDiscoveryRoot = Files.createDirectories(temporaryDirectory.resolve("outside"));
        copyValidPair(outsideDiscoveryRoot);
        Path discoveryRoot = Files.createDirectories(temporaryDirectory.resolve("discovery"));
        Files.createSymbolicLink(discoveryRoot.resolve("linked.yaml"), outsideDiscoveryRoot.resolve("minimal.yaml"));

        SpecificationCatalog catalog = loader.load(discoveryRoot);

        assertTrue(catalog.scenarios().isEmpty());
    }

    @Test
    void doesNotTraverseDirectorySymlinks() throws IOException {
        Path outsideDiscoveryRoot = Files.createDirectories(temporaryDirectory.resolve("outside"));
        copyValidPair(outsideDiscoveryRoot);
        Path discoveryRoot = Files.createDirectories(temporaryDirectory.resolve("discovery"));
        Files.createSymbolicLink(discoveryRoot.resolve("linked-directory"), outsideDiscoveryRoot);

        SpecificationCatalog catalog = loader.load(discoveryRoot);

        assertTrue(catalog.scenarios().isEmpty());
    }

    @Test
    void ignoresNonCanonicalYamlExtensions() throws IOException {
        Files.writeString(temporaryDirectory.resolve("invalid.yml"), "not: [valid");
        Files.writeString(temporaryDirectory.resolve("invalid.YAML"), "not: [valid");
        Files.writeString(temporaryDirectory.resolve("extensionless"), "not: [valid");

        SpecificationCatalog catalog = loader.load(temporaryDirectory);

        assertTrue(catalog.scenarios().isEmpty());
    }

    @Test
    void treatsSymlinkedExpectedResultAsMissing() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("minimal.yaml"));
        Path outsideDiscoveryRoot = Files.createDirectories(temporaryDirectory.resolve("outside"));
        Path realExpected = outsideDiscoveryRoot.resolve("minimal.expected.yaml");
        copyResource("minimal.expected.yaml", realExpected);
        Files.createSymbolicLink(temporaryDirectory.resolve("minimal.expected.yaml"),
                realExpected);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-missing", "minimal.yaml");
    }

    @Test
    void rejectsSymlinkDiscoveryRootWithoutFollowingIt() throws IOException {
        Path realRoot = Files.createDirectories(temporaryDirectory.resolve("real-root"));
        copyValidPair(realRoot);
        Path linkedRoot = Files.createSymbolicLink(temporaryDirectory.resolve("linked-root"), realRoot);

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(linkedRoot));

        assertHasIssue(exception, "catalog.root-not-directory", "linked-root");
    }

    @Test
    void rejectsNonDirectoryRootsWithoutFollowingSymlinks() throws IOException {
        Path file = Files.writeString(temporaryDirectory.resolve("scenario.yaml"), "format: v1\n");

        SpecificationException exception = assertFailsAt(
                Stage.CATALOG, () -> loader.load(file));

        assertHasIssue(exception, "catalog.root-not-directory", "scenario.yaml");
    }

    private void copyValidPair(Path directory) throws IOException {
        copyResource("minimal.yaml", directory.resolve("minimal.yaml"));
        copyResource("minimal.expected.yaml", directory.resolve("minimal.expected.yaml"));
    }

    private void copyValidPairNamed(String name) throws IOException {
        ObjectNode scenario = YamlTestDocuments.read(resource("minimal.yaml"));
        ObjectNode meta = (ObjectNode) scenario.required("meta");
        meta.required("name");
        meta.put("name", name);
        YamlTestDocuments.write(temporaryDirectory.resolve(name + ".yaml"), scenario);

        YamlTestDocuments.write(temporaryDirectory.resolve(name + ".expected.yaml"),
                expectedResultFor(name));
    }

    private ObjectNode expectedResultFor(String scenarioName) throws IOException {
        ObjectNode expected = YamlTestDocuments.read(resource("minimal.expected.yaml"));
        ObjectNode meta = (ObjectNode) expected.required("meta");
        meta.required("name");
        meta.required("scenario");
        meta.put("name", scenarioName + ".expected");
        meta.put("scenario", scenarioName);
        return expected;
    }

    private void writeSuite(String scenarioEntries) throws IOException {
        ObjectNode suite = YamlTestDocuments.read(resource("smoke-suite.yaml"));
        ObjectNode meta = (ObjectNode) suite.required("meta");
        meta.required("name");
        meta.put("name", "membership-suite");
        suite.required("scenarios");
        suite.set("scenarios", YamlTestDocuments.readArray(scenarioEntries));
        YamlTestDocuments.write(temporaryDirectory.resolve("membership-suite.yaml"), suite);
    }

    private void copyResource(String name, Path target) throws IOException {
        Files.copy(resource(name), target);
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static void assertHasIssue(
            SpecificationException exception, String code, String filename) {
        assertTrue(exception.diagnostics().stream()
                        .anyMatch(issue -> issue.code().equals(code)
                                && issue.source().getFileName().toString().equals(filename)),
                () -> "Expected " + code + " in " + filename + " but got " + exception.diagnostics());
    }

    private static long countIssues(
            SpecificationException exception, String code, String filename) {
        return exception.diagnostics().stream()
                .filter(issue -> issue.code().equals(code)
                        && issue.source().getFileName().toString().equals(filename))
                .count();
    }

    private static void assertExactSuiteIssues(
            SpecificationException exception, String... expectedIssues) {
        assertTrue(exception.diagnostics().stream()
                        .allMatch(issue -> issue.source().getFileName().toString()
                                .equals("membership-suite.yaml")),
                () -> "Expected only membership-suite.yaml issues but got " + exception.diagnostics());
        assertEquals(List.of(expectedIssues), exception.diagnostics().stream()
                .map(issue -> issue.code() + "@" + issue.path())
                .toList());
    }
}
