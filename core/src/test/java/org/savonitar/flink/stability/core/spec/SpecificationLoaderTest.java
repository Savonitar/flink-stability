package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpecificationLoaderTest {
    private final SpecificationLoader loader = new SpecificationLoader();

    @TempDir
    Path temporaryDirectory;

    @Test
    void dispatchesAndValidatesEveryV1DocumentKind() {
        LoadedSpecification scenario = loader.load(resource("minimal.yaml"));
        LoadedSpecification expectedResult = loader.load(resource("minimal.expected.yaml"));
        LoadedSpecification suite = loader.load(resource("smoke-suite.yaml"));

        assertInstanceOf(ScenarioSpecification.class, scenario);
        assertEquals(DocumentKind.SCENARIO, scenario.kind());
        assertEquals("minimal", scenario.name());

        assertInstanceOf(ExpectedResultSpecification.class, expectedResult);
        assertEquals(DocumentKind.EXPECTED_RESULT, expectedResult.kind());
        assertEquals("minimal.expected", expectedResult.name());

        assertInstanceOf(SuiteSpecification.class, suite);
        assertEquals(DocumentKind.SUITE, suite.kind());
        assertEquals("smoke-suite", suite.name());
    }

    @Test
    void rejectsMissingFormatBeforeKindDispatch() throws IOException {
        Path document = write("missing-format.yaml", "kind: unknown\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class, () -> loader.load(document));

        assertSingleIssue(exception, "document.missing-format", "$/format");
    }

    @Test
    void rejectsUnsupportedFormatBeforeKindDispatch() throws IOException {
        Path document = write("unsupported-format.yaml", "format: v2\nkind: unknown\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class, () -> loader.load(document));

        assertSingleIssue(exception, "document.unsupported-format", "$/format");
    }

    @Test
    void rejectsUnknownKindBeforeSchemaValidation() throws IOException {
        Path document = write("unknown-kind.yaml", "format: v1\nkind: experiment\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class, () -> loader.load(document));

        assertSingleIssue(exception, "document.unknown-kind", "$/kind");
    }

    @Test
    void rejectsUnknownFieldsThroughTheSelectedSchema() throws IOException {
        String valid = Files.readString(resource("minimal.yaml"));
        Path document = write("unknown-field.yaml", valid + "unknown_field: true\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class, () -> loader.load(document));

        assertTrue(exception.issues().stream()
                .anyMatch(issue -> issue.code().equals("schema.additional-properties")
                        && issue.path().equals("$")));
        assertTrue(exception.getMessage().contains("unknown_field"));
    }

    @Test
    void rejectsMissingScenarioDescription() throws IOException {
        String invalid = Files.readString(resource("minimal.yaml"))
                .replace("  description: Minimal structurally valid scenario used by loader tests.\n", "");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("missing-description.yaml", invalid)));

        assertHasIssue(exception, "schema.required", "$/meta");
    }

    @Test
    void rejectsUnknownExpectedResultField() throws IOException {
        String invalid = Files.readString(resource("minimal.expected.yaml")) + "unknown_field: true\n";

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("unknown-expected-field.yaml", invalid)));

        assertHasIssue(exception, "schema.additional-properties", "$");
    }

    @Test
    void rejectsOracleOnExpectedPass() throws IOException {
        String invalid = Files.readString(resource("minimal.expected.yaml"))
                .replace("  outcome: pass\n", "  outcome: pass\n  oracle: kafka.id-set\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("pass-with-oracle.expected.yaml", invalid)));

        assertHasIssue(exception, "schema.one-of", "$/default");
    }

    @Test
    void rejectsExpectedFailureWithoutOracle() {
        Path source = resource("minimal.expected.yaml");
        ObjectNode document = loader.loadExpectedResult(source).document();
        ObjectNode expectation = document.withObject("default");
        expectation.removeAll();
        expectation.put("outcome", "fail");
        expectation.put("reason", "validator.kafka.id-set.missing-ids");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateExpectedResultDocument(source, document));

        assertHasIssue(exception, "schema.one-of", "$/default");
    }

    @Test
    void rejectsExpectedFailureWithoutReason() {
        Path source = resource("minimal.expected.yaml");
        ObjectNode document = loader.loadExpectedResult(source).document();
        ObjectNode expectation = document.withObject("default");
        expectation.removeAll();
        expectation.put("outcome", "fail");
        expectation.put("oracle", "kafka.id-set");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateExpectedResultDocument(source, document));

        assertHasIssue(exception, "schema.one-of", "$/default");
    }

    @Test
    void rejectsFailingExperimentBaseline() {
        Path source = resource("minimal.expected.yaml");
        ObjectNode document = loader.loadExpectedResult(source).document();
        ObjectNode expectation = document.withObject("default");
        expectation.removeAll();
        expectation.putObject("baseline").put("outcome", "fail");
        expectation.putObject("candidate").put("outcome", "pass");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateExpectedResultDocument(source, document));

        assertHasIssue(exception, "schema.one-of", "$/default");
    }

    @Test
    void rejectsUnknownSuiteEntryField() throws IOException {
        String invalid = Files.readString(resource("smoke-suite.yaml"))
                .replace("  - scenario: minimal\n", "  - scenario: minimal\n    health_retry_limit: 2\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("unknown-suite-field.yaml", invalid)));

        assertHasIssue(exception, "schema.additional-properties", "$/scenarios/0");
    }

    @Test
    void rejectsZeroSuiteRuns() throws IOException {
        String invalid = Files.readString(resource("smoke-suite.yaml"))
                .replace("  - scenario: minimal\n", "  - scenario: minimal\n    runs: 0\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("zero-runs-suite.yaml", invalid)));

        assertHasIssue(exception, "schema.minimum", "$/scenarios/0/runs");
    }

    @Test
    void rejectsUnknownNestedNetworkFaultField() throws IOException {
        String scenario = Files.readString(resource("minimal.yaml"));
        String invalid = scenario.substring(0, scenario.indexOf("phases:")) + """
                phases:
                  - name: fault
                    steps:
                      - network_fault:
                          proxy: traffic
                          target: { cluster: main }
                          match: { api: produce, topic: output }
                          fault: { type: disconnect }
                          duration: 1s
                          heal: restore-proxy-rule
                          unknown_field: true

                """ + scenario.substring(scenario.indexOf("terminal_validations:"));

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("unknown-network-fault-field.yaml", invalid)));

        assertHasIssue(exception, "schema.additional-properties", "$/phases/0/steps/0/network_fault");
    }

    @Test
    void rejectsParameterInterpolationInScenarioLocalAliases() throws IOException {
        String invalid = Files.readString(resource("minimal.yaml"))
                .replace("alias: eos-job", "alias: ${job_alias}");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("templated-alias.yaml", invalid)));

        assertHasIssue(exception, "schema.pattern", "$/workload/jobs/0/alias");
    }

    @Test
    void localSubjectConnectorRequiresAnExplicitRuntimeDependencyDeclaration() {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        ObjectNode connector = (ObjectNode) document.at("/subject/connectors/kafka");
        connector.put("artifact", "./connector.jar");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateScenarioDocument(source, document));

        assertHasIssue(exception, "schema.required", "$/subject/connectors/kafka");
    }

    @Test
    void emptyRuntimeDependencyListExplicitlyAcceptsASelfContainedLocalConnector() {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        ObjectNode connector = (ObjectNode) document.at("/subject/connectors/kafka");
        connector.put("artifact", "./connector.jar");
        connector.putArray("runtime_dependencies");

        ScenarioSpecification scenario = loader.validateScenarioDocument(source, document);

        assertTrue(scenario.document().at(
                "/subject/connectors/kafka/runtime_dependencies").isEmpty());
    }

    @Test
    void mavenSubjectConnectorAcceptsAnExplicitRuntimeDependencyList() {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        ((ObjectNode) document.at("/subject/connectors/kafka"))
                .putArray("runtime_dependencies")
                .add("maven:org.apache.kafka:kafka-clients:4.2.0");

        ScenarioSpecification scenario = loader.validateScenarioDocument(source, document);

        assertEquals("maven:org.apache.kafka:kafka-clients:4.2.0",
                scenario.document().at(
                        "/subject/connectors/kafka/runtime_dependencies/0").textValue());
    }

    @Test
    void parameterizedConnectorDefersDependencyModeUntilResolvedValidation() {
        Path source = resource("minimal.yaml");
        ObjectNode raw = loader.loadScenario(source).document();
        raw.putObject("parameters").putObject("connector_artifact")
                .put("type", "string")
                .put("default", "./connector.jar");
        ((ObjectNode) raw.at("/subject/connectors/kafka"))
                .put("artifact", "${connector_artifact}");

        loader.validateScenarioDocument(source, raw);

        ObjectNode resolvedMaven = raw.deepCopy();
        resolvedMaven.remove("parameters");
        ((ObjectNode) resolvedMaven.at("/subject/connectors/kafka"))
                .put("artifact", "maven:org.example:connector:1.0.0");
        loader.validateResolvedScenario(source, resolvedMaven);

        ObjectNode resolved = raw.deepCopy();
        resolved.remove("parameters");
        ((ObjectNode) resolved.at("/subject/connectors/kafka"))
                .put("artifact", "./connector.jar");
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateResolvedScenario(source, resolved));

        assertHasIssue(exception, "schema.required", "$/subject/connectors/kafka");
    }

    @Test
    void runtimeDependencyReferencesMustBeUnique() {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        ObjectNode connector = (ObjectNode) document.at("/subject/connectors/kafka");
        connector.put("artifact", "./connector.jar");
        connector.putArray("runtime_dependencies")
                .add("./kafka-clients.jar")
                .add("./kafka-clients.jar");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateScenarioDocument(source, document));

        assertHasIssue(exception, "schema.unique-items",
                "$/subject/connectors/kafka/runtime_dependencies");
    }

    @Test
    void customValidatorFailureReasonsUseTheirDedicatedNamespace() {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        ObjectNode custom = document.withArray("terminal_validations").addObject();
        custom.put("type", "custom");
        custom.put("artifact", "./validator.jar");
        custom.put("timeout", "30s");
        custom.putArray("failure_reasons").add("await.job-state.timeout");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateScenarioDocument(source, document));

        assertHasIssue(exception, "schema.pattern", "$/terminal_validations/1/failure_reasons/0");
    }

    @Test
    void resolvedDocumentValidationRejectsDefinitionsAndUnresolvedTemplates() {
        Path source = resource("minimal.yaml");
        ObjectNode raw = loader.loadScenario(source).document();
        raw.putObject("parameters").putObject("brokers")
                .put("type", "integer").put("default", 1);
        ((ObjectNode) raw.at("/setup/kafka/clusters/main")).put("brokers", "${brokers}");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.validateResolvedScenario(source, raw));

        assertHasIssue(exception, "resolved.parameters-present", "$/parameters");
        assertHasIssue(exception, "resolved.unresolved-template", "$/setup/kafka/clusters/main/brokers");
    }

    @Test
    void rejectsDuplicateYamlKeysWithSourceLocation() throws IOException {
        Path document = write("duplicate-key.yaml", "format: v1\nformat: v1\nkind: scenario\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class, () -> loader.load(document));

        assertEquals("document.invalid-yaml", exception.issues().getFirst().code());
        assertTrue(exception.issues().getFirst().path().contains("line"));
    }

    @Test
    void rejectsMultipleYamlDocuments() throws IOException {
        Path document = write("multiple.yaml", "format: v1\nkind: suite\n---\nformat: v1\nkind: suite\n");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class, () -> loader.load(document));

        assertSingleIssue(exception, "document.multiple-documents", "$");
    }

    @Test
    void rejectsMissingKindBeforeSchemaValidation() throws IOException {
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("missing-kind.yaml", "format: v1\n")));

        assertSingleIssue(exception, "document.missing-kind", "$/kind");
    }

    @Test
    void rejectsNonStringFormatBeforeKindDispatch() throws IOException {
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("numeric-format.yaml", "format: 1\nkind: scenario\n")));

        assertSingleIssue(exception, "document.invalid-format", "$/format");
    }

    @Test
    void rejectsNonStringKindBeforeSchemaValidation() throws IOException {
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("numeric-kind.yaml", "format: v1\nkind: 1\n")));

        assertSingleIssue(exception, "document.invalid-kind", "$/kind");
    }

    @Test
    void rejectsEmptyDocuments() throws IOException {
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("empty.yaml", "")));

        assertSingleIssue(exception, "document.empty", "$");
    }

    @Test
    void rejectsNonObjectRoots() throws IOException {
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("array.yaml", "- item\n")));

        assertSingleIssue(exception, "document.root-not-object", "$");
    }

    @Test
    void rejectsMalformedYaml() throws IOException {
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(write("malformed.yaml", "format: [\n")));

        assertEquals("document.invalid-yaml", exception.issues().getFirst().code());
    }

    @Test
    void rejectsAValidDocumentWhenItsCallerExpectsAnotherKind() {
        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class,
                () -> loader.load(resource("minimal.expected.yaml"), DocumentKind.SCENARIO));

        assertSingleIssue(exception, "document.kind-mismatch", "$/kind");
    }

    @Test
    void doesNotExposeItsMutableJacksonTree() {
        ScenarioSpecification scenario = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode firstCopy = scenario.document();
        firstCopy.put("format", "mutated");

        assertEquals("v1", scenario.format());
        assertNotEquals(firstCopy, scenario.document());
    }

    @Test
    void rejectsMissingFilesBeforeParsing() {
        Path missing = temporaryDirectory.resolve("missing.yaml");

        DocumentValidationException exception = assertThrows(
                DocumentValidationException.class, () -> loader.load(missing));

        assertSingleIssue(exception, "document.not-found", "$");
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private Path write(String filename, String content) throws IOException {
        return Files.writeString(temporaryDirectory.resolve(filename), content);
    }

    private static void assertSingleIssue(
            DocumentValidationException exception, String code, String path) {
        assertEquals(1, exception.issues().size());
        assertEquals(code, exception.issues().getFirst().code());
        assertEquals(path, exception.issues().getFirst().path());
    }

    private static void assertHasIssue(
            DocumentValidationException exception, String code, String path) {
        assertTrue(exception.issues().stream()
                        .anyMatch(issue -> issue.code().equals(code) && issue.path().equals(path)),
                () -> "Expected " + code + " at " + path + " but got " + exception.issues());
    }
}
