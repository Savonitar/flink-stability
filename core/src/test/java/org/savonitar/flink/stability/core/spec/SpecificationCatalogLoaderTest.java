package org.savonitar.flink.stability.core.spec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-missing", "minimal.yaml");
    }

    @Test
    void rejectsExpectedResultTargetMismatch() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("minimal.yaml"));
        String expected = resourceText("minimal.expected.yaml")
                .replace("  scenario: minimal\n", "  scenario: another-scenario\n")
                .replace("  name: minimal.expected\n", "  name: another-scenario.expected\n");
        Files.writeString(temporaryDirectory.resolve("minimal.expected.yaml"), expected);

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-scenario-mismatch", "minimal.expected.yaml");
        assertHasIssue(exception, "catalog.expected-result-orphan", "minimal.expected.yaml");
    }

    @Test
    void rejectsExpectedResultNameMismatch() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("minimal.yaml"));
        String expected = resourceText("minimal.expected.yaml")
                .replace("  name: minimal.expected\n", "  name: wrong.expected\n");
        Files.writeString(temporaryDirectory.resolve("minimal.expected.yaml"), expected);

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-name-mismatch", "minimal.expected.yaml");
        assertEquals(1, countIssues(exception, "catalog.expected-result-name-mismatch",
                "minimal.expected.yaml"));
    }

    @Test
    void rejectsScenarioFilenameMismatch() throws IOException {
        copyResource("minimal.yaml", temporaryDirectory.resolve("different-name.yaml"));
        copyResource("minimal.expected.yaml", temporaryDirectory.resolve("different-name.expected.yaml"));

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.scenario-name-filename-mismatch", "different-name.yaml");
    }

    @Test
    void rejectsOrphanExpectedResult() throws IOException {
        String expected = resourceText("minimal.expected.yaml")
                .replace("minimal.expected", "ghost.expected")
                .replace("scenario: minimal", "scenario: ghost");
        Files.writeString(temporaryDirectory.resolve("ghost.expected.yaml"), expected);

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-orphan", "ghost.expected.yaml");
    }

    @Test
    void rejectsMultipleExpectedResultsTargetingOneScenario() throws IOException {
        copyValidPair(temporaryDirectory);
        copyResource("minimal.expected.yaml", temporaryDirectory.resolve("duplicate.expected.yaml"));

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-duplicate-target", "minimal.expected.yaml");
        assertHasIssue(exception, "catalog.expected-result-duplicate-target", "duplicate.expected.yaml");
    }

    @Test
    void rejectsDuplicateGlobalScenarioNamesAcrossDirectories() throws IOException {
        Path first = Files.createDirectories(temporaryDirectory.resolve("first"));
        Path second = Files.createDirectories(temporaryDirectory.resolve("second"));
        copyValidPair(first);
        copyValidPair(second);

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertEquals(2, exception.issues().stream()
                .filter(issue -> issue.code().equals("catalog.duplicate-scenario-name"))
                .count());
    }

    @Test
    void rejectsDuplicateSuiteNames() throws IOException {
        copyResource("smoke-suite.yaml", temporaryDirectory.resolve("first.yaml"));
        Path nested = Files.createDirectories(temporaryDirectory.resolve("nested"));
        copyResource("smoke-suite.yaml", nested.resolve("second.yaml"));

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertEquals(2, exception.issues().stream()
                .filter(issue -> issue.code().equals("catalog.duplicate-suite-name"))
                .count());
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

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(temporaryDirectory));

        assertHasIssue(exception, "catalog.expected-result-missing", "minimal.yaml");
    }

    @Test
    void rejectsSymlinkDiscoveryRootWithoutFollowingIt() throws IOException {
        Path realRoot = Files.createDirectories(temporaryDirectory.resolve("real-root"));
        copyValidPair(realRoot);
        Path linkedRoot = Files.createSymbolicLink(temporaryDirectory.resolve("linked-root"), realRoot);

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(linkedRoot));

        assertHasIssue(exception, "catalog.root-not-directory", "linked-root");
    }

    @Test
    void rejectsNonDirectoryRootsWithoutFollowingSymlinks() throws IOException {
        Path file = Files.writeString(temporaryDirectory.resolve("scenario.yaml"), "format: v1\n");

        CatalogValidationException exception = assertThrows(
                CatalogValidationException.class, () -> loader.load(file));

        assertHasIssue(exception, "catalog.root-not-directory", "scenario.yaml");
    }

    private void copyValidPair(Path directory) throws IOException {
        copyResource("minimal.yaml", directory.resolve("minimal.yaml"));
        copyResource("minimal.expected.yaml", directory.resolve("minimal.expected.yaml"));
    }

    private void copyResource(String name, Path target) throws IOException {
        Files.copy(resource(name), target);
    }

    private String resourceText(String name) throws IOException {
        return Files.readString(resource(name));
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static void assertHasIssue(
            CatalogValidationException exception, String code, String filename) {
        assertTrue(exception.issues().stream()
                        .anyMatch(issue -> issue.code().equals(code)
                                && issue.source().getFileName().toString().equals(filename)),
                () -> "Expected " + code + " in " + filename + " but got " + exception.issues());
    }

    private static long countIssues(
            CatalogValidationException exception, String code, String filename) {
        return exception.issues().stream()
                .filter(issue -> issue.code().equals(code)
                        && issue.source().getFileName().toString().equals(filename))
                .count();
    }
}
