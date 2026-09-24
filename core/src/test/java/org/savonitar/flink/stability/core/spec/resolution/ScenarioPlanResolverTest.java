package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.savonitar.flink.stability.core.spec.document.SpecificationAssertions.assertFailsAt;

class ScenarioPlanResolverTest {
    private final SpecificationLoader loader = new SpecificationLoader();

    @Test
    void resolvesCatalogBundleBindingsAndSelectsExpectationBeforeProvisioning() {
        ScenarioSpecification baseScenario = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode scenarioDocument = baseScenario.document();
        scenarioDocument.putObject("parameters").putObject("version")
                .put("type", "string").put("default", "2.2.0");
        ((ObjectNode) scenarioDocument.at("/setup/flink")).put("image", "flink:${version}");
        ScenarioSpecification scenario = loader.validateScenarioDocument(
                baseScenario.source(), scenarioDocument);

        ExpectedResultSpecification baseExpected = loader.loadExpectedResult(resource("minimal.expected.yaml"));
        ObjectNode expectedDocument = baseExpected.document();
        ArrayNode cases = expectedDocument.putArray("cases");
        ObjectNode caseNode = cases.addObject();
        caseNode.putObject("when").put("version", "2.2.2");
        caseNode.put("outcome", "fail");
        caseNode.put("oracle", "kafka.id-set");
        caseNode.put("reason", "validator.kafka.id-set.missing-ids");
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                baseExpected.source(), expectedDocument);

        ResolvedScenarioPlan plan = new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected),
                new ResolutionRequest(
                        Map.of("version", TextNode.valueOf("2.2.1")),
                        Map.of("version", TextNode.valueOf("2.2.2"))));

        assertEquals("flink:2.2.2", plan.scenario().side(ScenarioSide.SINGLE)
                .document().at("/setup/flink/image").textValue());
        assertEquals(ParameterSource.SUBMIT_OVERRIDE,
                plan.scenario().commonEffectiveParameters().get("version").source());
        assertEquals(ExpectationSelectionKind.CASE, plan.selectedExpectation().kind());
        assertEquals("$/cases/0", plan.selectedExpectation().originPointer());
    }

    @Test
    void rejectsSemanticReferenceFailuresBeforeReturningAPlan() {
        ScenarioSpecification baseScenario = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode document = baseScenario.document();
        ((ObjectNode) document.at("/workload/jobs/0/source")).put("topic", "missing");
        ScenarioSpecification scenario = loader.validateScenarioDocument(
                baseScenario.source(), document);

        assertFailsAt(
                Stage.PREFLIGHT,
                () -> new ScenarioPlanResolver().resolve(
                        new ScenarioBundle(
                                scenario,
                                loader.loadExpectedResult(resource("minimal.expected.yaml"))),
                        ResolutionRequest.none()));
    }

    @Test
    void reportsBaseTopologyOnceBeforeProbingDormantCases() {
        ScenarioSpecification baseScenario = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode scenarioDocument = baseScenario.document();
        scenarioDocument.putObject("parameters").putObject("profile")
                .put("type", "string").put("default", "default");
        ((ObjectNode) scenarioDocument.at("/workload/jobs/0"))
                .withArray("connectors").add(TextNode.valueOf("missing"));
        ScenarioSpecification scenario = loader.validateScenarioDocument(
                baseScenario.source(), scenarioDocument);

        ExpectedResultSpecification baseExpected =
                loader.loadExpectedResult(resource("minimal.expected.yaml"));
        ObjectNode expectedDocument = baseExpected.document();
        ArrayNode cases = expectedDocument.putArray("cases");
        addFailCase(cases, "first");
        addFailCase(cases, "second");
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                baseExpected.source(), expectedDocument);

        SpecificationException exception = assertFailsAt(
                Stage.PREFLIGHT,
                () -> new ScenarioPlanResolver().resolve(
                        new ScenarioBundle(scenario, expected), ResolutionRequest.none()));

        assertEquals(1, exception.diagnostics().size());
        Diagnostic issue = exception.diagnostics().getFirst();
        assertEquals("preflight.reference.connector-not-found", issue.code());
        assertEquals("$/workload/jobs/0/connectors/1", issue.path());
        assertEquals(scenario.source(), issue.source());
    }

    @Test
    void canonicalResolverRejectsUnknownExpectedFailureReason() {
        ExpectedResultSpecification baseExpected =
                loader.loadExpectedResult(resource("minimal.expected.yaml"));
        ObjectNode document = baseExpected.document();
        ObjectNode expectation = document.withObject("default");
        expectation.removeAll();
        expectation.put("outcome", "fail");
        expectation.put("oracle", "kafka.id-set");
        expectation.put("reason", "validator.kafka.id-set.unknown");
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                baseExpected.source(), document);

        SpecificationException exception = assertFailsAt(
                Stage.PREFLIGHT,
                () -> new ScenarioPlanResolver().resolve(
                        new ScenarioBundle(
                                loader.loadScenario(resource("minimal.yaml")), expected),
                        ResolutionRequest.none()));

        assertTrue(exception.diagnostics().stream().anyMatch(issue ->
                issue.code().equals("preflight.expectation.reason-not-declared")
                        && issue.path().equals("$/default/reason")));
    }

    private static void addFailCase(ArrayNode cases, String profile) {
        ObjectNode caseNode = cases.addObject();
        caseNode.putObject("when").put("profile", profile);
        caseNode.put("outcome", "fail");
        caseNode.put("oracle", "kafka.id-set");
        caseNode.put("reason", "validator.kafka.id-set.missing-ids");
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
