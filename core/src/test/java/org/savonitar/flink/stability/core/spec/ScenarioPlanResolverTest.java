package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScenarioPlanResolverTest {
    private final SpecificationLoader loader = new SpecificationLoader();

    @Test
    void resolvesCatalogBundleBindingsAndSelectsExpectationBeforeProvisioning() {
        ScenarioSpecification baseScenario = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode scenarioDocument = baseScenario.document();
        scenarioDocument.putObject("parameters").putObject("version")
                .put("type", "string").put("default", "1.19");
        ((ObjectNode) scenarioDocument.at("/setup/flink")).put("image", "flink:${version}");
        ScenarioSpecification scenario = loader.validateScenarioDocument(
                baseScenario.source(), scenarioDocument);

        ExpectedResultSpecification baseExpected = loader.loadExpectedResult(resource("minimal.expected.yaml"));
        ObjectNode expectedDocument = baseExpected.document();
        ArrayNode cases = expectedDocument.putArray("cases");
        ObjectNode caseNode = cases.addObject();
        caseNode.putObject("when").put("version", "1.21");
        caseNode.put("outcome", "fail");
        caseNode.put("oracle", "kafka.id-set");
        caseNode.put("reason", "validator.kafka.id-set.missing-ids");
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                baseExpected.source(), expectedDocument);

        ResolvedScenarioPlan plan = new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected),
                new ResolutionRequest(
                        Map.of("version", TextNode.valueOf("1.20")),
                        Map.of("version", TextNode.valueOf("1.21"))));

        assertEquals("flink:1.21", plan.scenario().side(ScenarioSide.SINGLE)
                .document().at("/setup/flink/image").textValue());
        assertEquals(ParameterSource.SUBMIT_OVERRIDE,
                plan.scenario().commonEffectiveParameters().get("version").source());
        assertEquals(ExpectationSelectionKind.CASE, plan.selectedExpectation().kind());
        assertEquals("$/cases/0", plan.selectedExpectation().originPointer());
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
