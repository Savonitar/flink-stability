package org.savonitar.flink.stability.core.spec.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.savonitar.flink.stability.core.spec.document.SpecificationAssertions.assertFailsAt;

class SpecificationLoaderTest {
    private final SpecificationLoader loader = new SpecificationLoader();

    @TempDir
    Path temporaryDirectory;

    @Test
    void connectorPinsRequireLowercaseSha256AfterParameterResolution() {
        Path source = resource("minimal.yaml");
        for (var pin : List.of(JsonNodeFactory.instance.textNode("a".repeat(64)),
                JsonNodeFactory.instance.textNode("0".repeat(64)))) {
            var document = loader.loadScenario(source).document();
            ((ObjectNode) document.at("/subject/connectors/kafka")).set("sha256", pin);
            loader.validateScenarioDocument(source, document);
            loader.validateResolvedScenario(source, document);
        }
        for (var pin : List.of(JsonNodeFactory.instance.textNode("A".repeat(64)),
                JsonNodeFactory.instance.textNode("a".repeat(63)), JsonNodeFactory.instance.textNode("g".repeat(64)),
                JsonNodeFactory.instance.textNode(""), JsonNodeFactory.instance.numberNode(123),
                JsonNodeFactory.instance.nullNode())) {
            var document = loader.loadScenario(source).document();
            ((ObjectNode) document.at("/subject/connectors/kafka")).set("sha256", pin);
            assertFailsAt(Stage.DOCUMENT, () -> loader.validateScenarioDocument(source, document));
            assertFailsAt(Stage.DOCUMENT, () -> loader.validateResolvedScenario(source, document));
        }
        var parameterized = loader.loadScenario(source).document();
        ((ObjectNode) parameterized.at("/subject/connectors/kafka")).put("sha256", "${connector_hash}");
        loader.validateScenarioDocument(source, parameterized);
        assertFailsAt(Stage.DOCUMENT, () -> loader.validateResolvedScenario(source, parameterized));
    }

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
    void acceptsExplicitKafkaTransactionVersionsInRawAndResolvedDocuments() {
        Path source = resource("minimal.yaml");
        for (int version : List.of(1, 2)) {
            ObjectNode document = loader.loadScenario(source).document();
            ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                    .put("transaction_version", version);

            ScenarioSpecification raw = loader.validateScenarioDocument(source, document);
            ScenarioSpecification resolved = loader.validateResolvedScenario(source, document);

            assertEquals(version, raw.at("/setup/kafka/clusters/main/transaction_version").intValue());
            assertEquals(version, resolved.at("/setup/kafka/clusters/main/transaction_version").intValue());
        }
    }

    @Test
    void rejectsInvalidKafkaTransactionVersionsInRawAndResolvedDocuments() {
        Path source = resource("minimal.yaml");
        JsonNodeFactory nodes = JsonNodeFactory.instance;
        List<JsonNode> invalidValues = List.of(
                nodes.numberNode(0),
                nodes.numberNode(3),
                nodes.numberNode(1.5),
                nodes.textNode("1"),
                nodes.booleanNode(true),
                nodes.nullNode());
        for (JsonNode version : invalidValues) {
            ObjectNode document = loader.loadScenario(source).document();
            ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                    .set("transaction_version", version);

            SpecificationException raw = assertFailsAt(
                    Stage.DOCUMENT,
                    () -> loader.validateScenarioDocument(source, document));
            SpecificationException resolved = assertFailsAt(
                    Stage.DOCUMENT,
                    () -> loader.validateResolvedScenario(source, document));

            assertHasIssue(raw, "schema.one-of", "$/setup/kafka/clusters/main/transaction_version");
            assertHasIssue(resolved, "schema.one-of", "$/setup/kafka/clusters/main/transaction_version");
        }
    }

    @Test
    void preservesExplicitTokenProofScopesAndOmissionInRawAndResolvedDocuments() {
        Path source = resource("minimal.yaml");
        for (String scope : List.of("", "bootstrap", "submitted-job")) {
            ObjectNode document = loader.loadScenario(source).document();
            ObjectNode provider = ((ObjectNode) document.at("/setup/flink"))
                    .putObject("token_provider").put("renewal_interval", "2s");
            if (!scope.isEmpty()) provider.put("proof_scope", scope);

            for (ScenarioSpecification validated : List.of(
                    loader.validateScenarioDocument(source, document),
                    loader.validateResolvedScenario(source, document))) {
                JsonNode value = validated.at("/setup/flink/token_provider/proof_scope");
                if (scope.isEmpty()) assertTrue(value.isMissingNode());
                else assertEquals(scope, value.textValue());
            }
        }
    }

    @Test
    void rejectsUnknownOrNonStringTokenProofScopes() {
        Path source = resource("minimal.yaml");
        JsonNodeFactory nodes = JsonNodeFactory.instance;
        for (JsonNode scope : List.of(nodes.textNode(""), nodes.textNode("legacy"),
                nodes.textNode("BOOTSTRAP"), nodes.textNode("job"), nodes.textNode("submitted_job"),
                nodes.numberNode(1), nodes.booleanNode(true), nodes.nullNode(), nodes.objectNode())) {
            ObjectNode document = loader.loadScenario(source).document();
            ((ObjectNode) document.at("/setup/flink")).putObject("token_provider")
                    .put("renewal_interval", "2s").set("proof_scope", scope);
            assertFailsAt(Stage.DOCUMENT, () -> loader.validateScenarioDocument(source, document));
            assertFailsAt(Stage.DOCUMENT, () -> loader.validateResolvedScenario(source, document));
        }
    }

    @Test
    void acceptsOnlyTheImplementedExplicitRecoveryBarrier() {
        Path source = resource("minimal.yaml");
        for (String value : List.of("token-checkpoint", "checkpoint", "true")) {
            ObjectNode document = loader.loadScenario(source).document();
            ((ObjectNode) document.at("/phases/0")).putArray("steps").addObject().putObject("leader_fault")
                    .put("mode", "isolate-zookeeper").put("duration", "15s").put("timeout", "2m")
                    .put("recovery_barrier", value);
            if (value.equals("token-checkpoint")) {
                loader.validateScenarioDocument(source, document);
                loader.validateResolvedScenario(source, document);
            } else {
                assertFailsAt(Stage.DOCUMENT, () -> loader.validateScenarioDocument(source, document));
                assertFailsAt(Stage.DOCUMENT, () -> loader.validateResolvedScenario(source, document));
            }
        }
    }

    @Test
    void namedRestartsCannotUpgradeImagesOrTargetOtherComponentKinds() {
        Path source = resource("minimal.yaml");
        for (String component : List.of("taskmanager", "jobmanager", "flink", "kafka")) {
            ObjectNode document = loader.loadScenario(source).document();
            ObjectNode restart = ((ObjectNode) document.at("/phases/0"))
                    .putArray("steps").addObject().putObject("restart")
                    .put("component", component).put("name", "taskmanager-1");
            if (component.equals("taskmanager")) {
                loader.validateScenarioDocument(source, document);
                restart.put("image", "flink:2.2.1");
            }
            assertFailsAt(Stage.DOCUMENT, () -> loader.validateScenarioDocument(source, document));
        }
    }

    @Test
    void rejectsMissingFormatBeforeKindDispatch() throws IOException {
        Path document = write("missing-format.yaml", "kind: unknown\n");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(document));

        assertSingleIssue(exception, "document.missing-format", "$/format");
    }

    @Test
    void rejectsUnsupportedFormatBeforeKindDispatch() throws IOException {
        Path document = write("unsupported-format.yaml", "format: v2\nkind: unknown\n");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(document));

        assertSingleIssue(exception, "document.unsupported-format", "$/format");
    }

    @Test
    void rejectsUnknownKindBeforeSchemaValidation() throws IOException {
        Path document = write("unknown-kind.yaml", "format: v1\nkind: experiment\n");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(document));

        assertSingleIssue(exception, "document.unknown-kind", "$/kind");
    }

    @Test
    void rejectsUnknownFieldsThroughTheSelectedSchema() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("minimal.yaml"));
        invalid.put("unknown_field", true);
        Path document = write("unknown-field.yaml", invalid);

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(document));

        assertTrue(exception.diagnostics().stream()
                .anyMatch(issue -> issue.code().equals("schema.additional-properties")
                        && issue.path().equals("$")));
        assertTrue(exception.getMessage().contains("unknown_field"));
    }

    @Test
    void rejectsMissingScenarioDescription() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("minimal.yaml"));
        ObjectNode meta = (ObjectNode) invalid.required("meta");
        meta.required("description");
        meta.remove("description");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("missing-description.yaml", invalid)));

        assertHasIssue(exception, "schema.required", "$/meta");
    }

    @Test
    void rejectsUnknownExpectedResultField() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("minimal.expected.yaml"));
        invalid.put("unknown_field", true);

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("unknown-expected-field.yaml", invalid)));

        assertHasIssue(exception, "schema.additional-properties", "$");
    }

    @Test
    void rejectsOracleOnExpectedPass() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("minimal.expected.yaml"));
        ObjectNode expectation = (ObjectNode) invalid.required("default");
        assertEquals("pass", expectation.required("outcome").textValue());
        expectation.put("oracle", "kafka.id-set");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.validateExpectedResultDocument(source, document));

        assertHasIssue(exception, "schema.one-of", "$/default");
    }

    @Test
    void rejectsUnknownSuiteEntryField() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("smoke-suite.yaml"));
        ((ObjectNode) invalid.requiredAt("/scenarios/0")).put("health_retry_limit", 2);

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("unknown-suite-field.yaml", invalid)));

        assertHasIssue(exception, "schema.additional-properties", "$/scenarios/0");
    }

    @Test
    void rejectsZeroSuiteRuns() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("smoke-suite.yaml"));
        ((ObjectNode) invalid.requiredAt("/scenarios/0")).put("runs", 0);

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("zero-runs-suite.yaml", invalid)));

        assertHasIssue(exception, "schema.minimum", "$/scenarios/0/runs");
    }

    @Test
    void rejectsUnknownNestedNetworkFaultField() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("minimal.yaml"));
        invalid.required("phases");
        ObjectNode phase = invalid.putArray("phases").addObject().put("name", "fault");
        ObjectNode networkFault = phase.putArray("steps").addObject().putObject("network_fault");
        networkFault.put("proxy", "traffic");
        networkFault.putObject("target").put("cluster", "main");
        networkFault.putObject("match").put("api", "produce").put("topic", "output");
        networkFault.putObject("fault").put("type", "disconnect");
        networkFault.put("duration", "1s");
        networkFault.put("heal", "restore-proxy-rule");
        networkFault.put("unknown_field", true);

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("unknown-network-fault-field.yaml", invalid)));

        assertHasIssue(exception, "schema.additional-properties", "$/phases/0/steps/0/network_fault");
    }

    @Test
    void rejectsParameterInterpolationInScenarioLocalAliases() throws IOException {
        ObjectNode invalid = YamlTestDocuments.read(resource("minimal.yaml"));
        ObjectNode job = (ObjectNode) invalid.requiredAt("/workload/jobs/0");
        job.required("alias");
        job.put("alias", "${job_alias}");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("templated-alias.yaml", invalid)));

        assertHasIssue(exception, "schema.pattern", "$/workload/jobs/0/alias");
    }

    @Test
    void localSubjectConnectorRequiresAnExplicitRuntimeDependencyDeclaration() {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        ObjectNode connector = (ObjectNode) document.at("/subject/connectors/kafka");
        connector.put("artifact", "./connector.jar");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.validateResolvedScenario(source, raw));

        assertHasIssue(exception, "resolved.parameters-present", "$/parameters");
        assertHasIssue(exception, "resolved.unresolved-template", "$/setup/kafka/clusters/main/brokers");
    }

    @Test
    void rejectsDuplicateYamlKeysWithSourceLocation() throws IOException {
        Path document = write("duplicate-key.yaml", "format: v1\nformat: v1\nkind: scenario\n");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(document));

        assertEquals("document.invalid-yaml", exception.diagnostics().getFirst().code());
        assertTrue(exception.diagnostics().getFirst().path().contains("line"));
    }

    @Test
    void rejectsMultipleYamlDocuments() throws IOException {
        Path document = write("multiple.yaml", "format: v1\nkind: suite\n---\nformat: v1\nkind: suite\n");

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(document));

        assertSingleIssue(exception, "document.multiple-documents", "$");
    }

    @Test
    void rejectsMissingKindBeforeSchemaValidation() throws IOException {
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("missing-kind.yaml", "format: v1\n")));

        assertSingleIssue(exception, "document.missing-kind", "$/kind");
    }

    @Test
    void rejectsNonStringFormatBeforeKindDispatch() throws IOException {
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("numeric-format.yaml", "format: 1\nkind: scenario\n")));

        assertSingleIssue(exception, "document.invalid-format", "$/format");
    }

    @Test
    void rejectsNonStringKindBeforeSchemaValidation() throws IOException {
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("numeric-kind.yaml", "format: v1\nkind: 1\n")));

        assertSingleIssue(exception, "document.invalid-kind", "$/kind");
    }

    @Test
    void rejectsEmptyDocuments() throws IOException {
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("empty.yaml", "")));

        assertSingleIssue(exception, "document.empty", "$");
    }

    @Test
    void rejectsNonObjectRoots() throws IOException {
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("array.yaml", "- item\n")));

        assertSingleIssue(exception, "document.root-not-object", "$");
    }

    @Test
    void rejectsMalformedYaml() throws IOException {
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("malformed.yaml", "format: [\n")));

        assertEquals("document.invalid-yaml", exception.diagnostics().getFirst().code());
    }

    @Test
    void rejectsAValidDocumentWhenItsCallerExpectsAnotherKind() {
        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT,
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

        SpecificationException exception = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(missing));

        assertSingleIssue(exception, "document.not-found", "$");
    }

    @Test
    void countedDropFaultsAndHeldFaultsTakeDifferentTimingFields() throws IOException {
        ObjectNode dropWithDuration = networkFaultDocument("end-txn", "drop-response");
        ObjectNode drop = (ObjectNode) dropWithDuration.at("/phases/0/steps/0/network_fault");
        drop.remove(List.of("occurrences"));
        drop.put("duration", "1s");
        SpecificationException countedFailure = assertFailsAt(
                Stage.DOCUMENT, () -> loader.load(write("drop-with-duration.yaml", dropWithDuration)));
        assertHasIssue(countedFailure, "schema.required", "$/phases/0/steps/0/network_fault");
        assertHasIssue(countedFailure, "schema.not", "$/phases/0/steps/0/network_fault");

        ObjectNode heldWithOccurrences = networkFaultDocument("end-txn", "disconnect");
        ((ObjectNode) heldWithOccurrences.at("/phases/0/steps/0/network_fault"))
                .put("duration", "1s");
        SpecificationException heldFailure = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("held-with-occurrences.yaml", heldWithOccurrences)));
        assertHasIssue(heldFailure, "schema.not", "$/phases/0/steps/0/network_fault");

        ObjectNode resultOnProduce = networkFaultDocument("produce", "drop-request");
        SpecificationException resultFailure = assertFailsAt(
                Stage.DOCUMENT,
                () -> loader.load(write("result-on-produce.yaml", resultOnProduce)));
        assertHasIssue(resultFailure, "schema.not", "$/phases/0/steps/0/network_fault/match");

        loader.load(write("drop-response.yaml", networkFaultDocument("end-txn", "drop-response")));
    }

    @Test
    void rejectsEachInvalidCountedOrHeldTimingFieldIndependently() throws IOException {
        for (String missing : List.of("occurrences", "trigger_deadline")) {
            ObjectNode document = networkFaultDocument("end-txn", "drop-response");
            ((ObjectNode) document.at("/phases/0/steps/0/network_fault")).remove(missing);
            SpecificationException failure = assertFailsAt(Stage.DOCUMENT,
                    () -> loader.load(write("missing-" + missing + ".yaml", document)));
            assertHasIssue(failure, "schema.required", "$/phases/0/steps/0/network_fault");
        }
        ObjectNode counted = networkFaultDocument("end-txn", "drop-request");
        ((ObjectNode) counted.at("/phases/0/steps/0/network_fault")).put("duration", "1s");
        SpecificationException durationFailure = assertFailsAt(Stage.DOCUMENT,
                () -> loader.load(write("counted-with-duration.yaml", counted)));
        assertHasIssue(durationFailure, "schema.not", "$/phases/0/steps/0/network_fault");
        for (String extra : List.of("occurrences", "trigger_deadline")) {
            ObjectNode held = networkFaultDocument("end-txn", "disconnect");
            ObjectNode fault = (ObjectNode) held.at("/phases/0/steps/0/network_fault");
            fault.put("duration", "1s");
            fault.remove(extra.equals("occurrences") ? "trigger_deadline" : "occurrences");
            SpecificationException failure = assertFailsAt(Stage.DOCUMENT,
                    () -> loader.load(write("held-with-" + extra + ".yaml", held)));
            assertHasIssue(failure, "schema.not", "$/phases/0/steps/0/network_fault");
        }
    }

    /** A scenario whose only step is a counted network fault on commits. */
    private ObjectNode networkFaultDocument(String api, String type) throws IOException {
        ObjectNode document = YamlTestDocuments.read(resource("minimal.yaml"));
        ObjectNode fault = document.putArray("phases").addObject().put("name", "fault")
                .putArray("steps").addObject().putObject("network_fault");
        fault.put("proxy", "kafka-proxy");
        fault.putObject("target").put("cluster", "main");
        fault.putObject("match").put("api", api).put("result", "commit");
        fault.putObject("fault").put("type", type);
        fault.put("occurrences", 1);
        fault.put("trigger_deadline", "1m");
        fault.put("heal", "restore-proxy-rule");
        return document;
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

    private Path write(String filename, ObjectNode document) throws IOException {
        return YamlTestDocuments.write(temporaryDirectory.resolve(filename), document);
    }

    private static void assertSingleIssue(
            SpecificationException exception, String code, String path) {
        assertEquals(1, exception.diagnostics().size());
        assertEquals(code, exception.diagnostics().getFirst().code());
        assertEquals(path, exception.diagnostics().getFirst().path());
    }

    private static void assertHasIssue(
            SpecificationException exception, String code, String path) {
        assertTrue(exception.diagnostics().stream()
                        .anyMatch(issue -> issue.code().equals(code) && issue.path().equals(path)),
                () -> "Expected " + code + " at " + path + " but got " + exception.diagnostics());
    }
}
