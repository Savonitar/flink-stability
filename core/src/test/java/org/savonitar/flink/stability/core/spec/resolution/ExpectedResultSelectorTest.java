package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpectedResultSelectorTest {
    private final SpecificationLoader loader = new SpecificationLoader();
    private final ScenarioParameterResolver parameterResolver = new ScenarioParameterResolver(loader);
    private final ExpectedResultSelector selector = new ExpectedResultSelector(parameterResolver);

    @Test
    void selectsPlainDefaultWhenThereAreNoCases() {
        ScenarioBundle bundle = bundle(plainScenario(document -> {}), expected(document -> {}));
        ResolvedScenario resolved = parameterResolver.resolve(bundle.scenario(), ResolutionRequest.none());

        ResolvedScenarioPlan plan = selector.select(bundle, resolved);

        assertEquals(ExpectationSelectionKind.DEFAULT, plan.selectedExpectation().kind());
        assertTrue(plan.selectedExpectation().caseIndex().isEmpty());
        assertEquals("$/default", plan.selectedExpectation().originPointer());
        assertEquals("pass", plan.expectationFor(ScenarioSide.SINGLE).path("outcome").textValue());
    }

    @Test
    void selectsMatchingCaseAndFallsBackToDefaultWhenItDoesNotMatch() {
        ScenarioSpecification scenario = parameterizedPlainScenario("color", "string", TextNode.valueOf("blue"));
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("color", TextNode.valueOf("red"))));
        ScenarioBundle bundle = bundle(scenario, expected);

        ResolvedScenarioPlan matched = selector.select(bundle, parameterResolver.resolve(
                scenario,
                new ResolutionRequest(Map.of("color", TextNode.valueOf("red")), Map.of())));
        assertEquals(ExpectationSelectionKind.CASE, matched.selectedExpectation().kind());
        assertEquals(0, matched.selectedExpectation().caseIndex().orElseThrow());
        assertEquals("$/cases/0", matched.selectedExpectation().originPointer());
        assertEquals("fail", matched.expectationFor(ScenarioSide.SINGLE).path("outcome").textValue());

        ResolvedScenarioPlan fallback = selector.select(bundle, parameterResolver.resolve(
                scenario,
                new ResolutionRequest(Map.of("color", TextNode.valueOf("green")), Map.of())));
        assertEquals(ExpectationSelectionKind.DEFAULT, fallback.selectedExpectation().kind());
    }

    @Test
    void submitOverrideWinsBeforeCaseSelection() {
        ScenarioSpecification scenario = parameterizedPlainScenario("color", "string", TextNode.valueOf("blue"));
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("color", TextNode.valueOf("red"))));

        ResolvedScenarioPlan plan = new ScenarioPlanResolver(parameterResolver).resolve(
                bundle(scenario, expected),
                new ResolutionRequest(
                        Map.of("color", TextNode.valueOf("green")),
                        Map.of("color", TextNode.valueOf("red"))));

        assertEquals(ExpectationSelectionKind.CASE, plan.selectedExpectation().kind());
        assertEquals(ParameterSource.SUBMIT_OVERRIDE,
                plan.scenario().commonEffectiveParameters().get("color").source());
    }

    @Test
    void matchingUsesTypedIntegerBooleanStringAndDurationValues() {
        ScenarioSpecification scenario = plainScenario(document -> {
            parameter(document, "count", "integer", IntNode.valueOf(1));
            parameter(document, "enabled", "boolean", BooleanNode.FALSE);
            parameter(document, "label", "string", TextNode.valueOf("old"));
            parameter(document, "timeout", "duration", TextNode.valueOf("1s"));
        });
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(document, Map.of(
                "count", IntNode.valueOf(2),
                "enabled", BooleanNode.TRUE,
                "label", TextNode.valueOf("new"),
                "timeout", TextNode.valueOf("60s"))));

        ResolvedScenarioPlan plan = new ScenarioPlanResolver(parameterResolver).resolve(
                bundle(scenario, expected),
                new ResolutionRequest(Map.of(
                        "count", IntNode.valueOf(2),
                        "enabled", BooleanNode.TRUE,
                        "label", TextNode.valueOf("new"),
                        "timeout", TextNode.valueOf("60s")), Map.of()));

        assertEquals(ExpectationSelectionKind.CASE, plan.selectedExpectation().kind());
        assertTrue(plan.selectedExpectation().conditions().get("count").isIntegralNumber());
        assertTrue(plan.selectedExpectation().conditions().get("enabled").isBoolean());
    }

    @Test
    void integerMatchingIsIndependentOfJacksonNodeWidth() {
        ScenarioSpecification scenario = parameterizedPlainScenario(
                "count", "integer", IntNode.valueOf(1));
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("count", IntNode.valueOf(2))));

        ResolvedScenarioPlan plan = new ScenarioPlanResolver(parameterResolver).resolve(
                bundle(scenario, expected),
                new ResolutionRequest(Map.of("count", LongNode.valueOf(2L)), Map.of()));

        assertEquals(ExpectationSelectionKind.CASE, plan.selectedExpectation().kind());
    }

    @Test
    void selectsOneExperimentExpectationFromPairCommonParameters() {
        ScenarioSpecification scenario = experimentScenario();
        ExpectedResultSpecification expected = experimentExpected(document -> {
            ObjectNode caseNode = document.putArray("cases").addObject();
            caseNode.putObject("when").put("strategy", "POOLING");
            caseNode.putObject("baseline").put("outcome", "pass");
            caseNode.putObject("candidate").put("outcome", "pass");
        });

        ResolvedScenarioPlan plan = new ScenarioPlanResolver(parameterResolver).resolve(
                bundle(scenario, expected),
                new ResolutionRequest(Map.of("strategy", TextNode.valueOf("POOLING")), Map.of()));

        assertEquals(ExpectationSelectionKind.CASE, plan.selectedExpectation().kind());
        assertEquals("pass", plan.expectationFor(ScenarioSide.BASELINE).path("outcome").textValue());
        assertEquals("pass", plan.expectationFor(ScenarioSide.CANDIDATE).path("outcome").textValue());
        assertFalse(plan.scenario().commonEffectiveParameters().containsKey("backend"));
    }

    @Test
    void rejectsDefaultShapeThatDoesNotMatchScenarioKind() {
        ScenarioSpecification plain = plainScenario(document -> {});
        ExpectedResultSpecification experimentShape = experimentExpected(document -> {});
        ExpectedResultSelectionException plainException = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(plain, experimentShape),
                        parameterResolver.resolve(plain, ResolutionRequest.none())));
        assertHasIssue(plainException, "expectation.shape-mismatch", "$/default");

        ScenarioSpecification experiment = experimentScenario();
        ExpectedResultSpecification plainShape = expected(document -> {});
        ExpectedResultSelectionException experimentException = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(experiment, plainShape),
                        parameterResolver.resolve(experiment, ResolutionRequest.none())));
        assertHasIssue(experimentException, "expectation.shape-mismatch", "$/default");
    }

    @Test
    void validatesTheShapeOfUnselectedCases() {
        ScenarioSpecification scenario = parameterizedPlainScenario("color", "string", TextNode.valueOf("blue"));
        ExpectedResultSpecification expected = expected(document -> {
            ObjectNode caseNode = document.putArray("cases").addObject();
            caseNode.putObject("when").put("color", "red");
            caseNode.putObject("baseline").put("outcome", "pass");
            caseNode.putObject("candidate").put("outcome", "pass");
        });

        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));

        assertHasIssue(exception, "expectation.shape-mismatch", "$/cases/0");
    }

    @Test
    void rejectsUnknownAndVariedCaseParameters() {
        ScenarioSpecification plain = parameterizedPlainScenario("known", "string", TextNode.valueOf("value"));
        ExpectedResultSpecification unknown = expected(document -> addPlainFailCase(
                document, Map.of("unknown", TextNode.valueOf("value"))));
        ExpectedResultSelectionException unknownException = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(plain, unknown),
                        parameterResolver.resolve(plain, ResolutionRequest.none())));
        assertHasIssue(unknownException, "expectation.case-unknown-parameter", "$/cases/0/when/unknown");

        ScenarioSpecification experiment = experimentScenario();
        ExpectedResultSpecification varied = experimentExpected(document -> {
            ObjectNode caseNode = document.putArray("cases").addObject();
            caseNode.putObject("when").put("backend", "rocksdb");
            caseNode.putObject("baseline").put("outcome", "pass");
            caseNode.putObject("candidate").put("outcome", "pass");
        });
        ExpectedResultSelectionException variedException = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(experiment, varied),
                        parameterResolver.resolve(experiment, ResolutionRequest.none())));
        assertHasIssue(variedException, "expectation.case-varied-parameter", "$/cases/0/when/backend");
    }

    @Test
    void rejectsWrongTypesInvalidDurationsAndOutOfRangeLiterals() {
        assertInvalidLiteral("count", "integer", IntNode.valueOf(1), TextNode.valueOf("1"));
        assertInvalidLiteral("timeout", "duration", TextNode.valueOf("1s"), TextNode.valueOf("1m30s"));

        ScenarioSpecification bounded = plainScenario(document -> {
            ObjectNode declaration = parameter(document, "count", "integer", IntNode.valueOf(2));
            declaration.put("min", 1).put("max", 3);
        });
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("count", IntNode.valueOf(4))));
        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(bounded, expected),
                        parameterResolver.resolve(bounded, ResolutionRequest.none())));
        assertHasIssue(exception, "expectation.case-value-invalid", "$/cases/0/when/count");
    }

    @Test
    void rejectsUnsupportedCapabilityInAnUnselectedCase() {
        ScenarioSpecification scenario = plainScenario(document -> {
            parameter(document, "backend", "string", TextNode.valueOf("rocksdb"));
            job(document).put("state_backend", "${backend}");
        });
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("backend", TextNode.valueOf("forst"))));

        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));

        assertHasIssue(exception, "expectation.case-value-invalid", "$/cases/0/when");
        assertTrue(exception.getMessage().contains("capability.state-backend.unsupported"));
    }

    @Test
    void rejectsUnselectedCaseThatProducesAnInvalidResolvedShape() {
        ScenarioSpecification scenario = plainScenario(document -> {
            parameter(document, "guarantee", "string", TextNode.valueOf("EXACTLY_ONCE"));
            job(document).withObject("sink").put("delivery_guarantee", "${guarantee}");
        });
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("guarantee", TextNode.valueOf("AT_LEAST_ONCE"))));

        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));

        assertHasIssue(exception, "expectation.case-value-invalid", "$/cases/0/when");
        assertTrue(exception.getMessage().contains("schema."));
        assertTrue(exception.getMessage().contains("Resolved document"));
    }

    @Test
    void rejectsUnselectedCaseThatBreaksResolvedTopology() {
        ScenarioSpecification scenario = plainScenario(document -> {
            parameter(document, "source_topic", "string", TextNode.valueOf("input"));
            job(document).withObject("source").put("topic", "${source_topic}");
        });
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("source_topic", TextNode.valueOf("missing"))));

        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));

        assertHasIssue(exception, "expectation.case-value-invalid", "$/cases/0/when");
        assertTrue(exception.getMessage().contains("preflight.reference.kafka-topic-not-found"));
    }

    @Test
    void internalCaseProbesDoNotTurnScenarioDefaultsIntoSuiteOverrides() {
        ScenarioSpecification scenario = plainScenario(document -> {
            parameter(document, "run_count", "integer", IntNode.valueOf(2));
            parameter(document, "profile", "string", TextNode.valueOf("blue"));
            document.put("runs", "${run_count}");
        });
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("profile", TextNode.valueOf("red"))));

        ResolvedScenarioPlan plan = selector.select(
                bundle(scenario, expected),
                parameterResolver.resolve(scenario, ResolutionRequest.none()));

        assertEquals(ExpectationSelectionKind.DEFAULT, plan.selectedExpectation().kind());
        assertEquals(2, plan.scenario().side(ScenarioSide.SINGLE)
                .document().path("runs").intValue());
    }

    @Test
    void rejectsSameSubsetAndDisjointKeyOverlaps() {
        ScenarioSpecification scenario = plainScenario(document -> {
            parameter(document, "a", "integer", IntNode.valueOf(3));
            parameter(document, "b", "integer", IntNode.valueOf(3));
        });
        ExpectedResultSpecification expected = expected(document -> {
            addPlainFailCase(document, Map.of("a", IntNode.valueOf(1)));
            addPlainFailCase(document, Map.of("a", IntNode.valueOf(1), "b", IntNode.valueOf(2)));
            addPlainFailCase(document, Map.of("b", IntNode.valueOf(1)));
        });

        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));

        assertHasIssue(exception, "expectation.case-overlap", "$/cases/1/when");
        assertHasIssue(exception, "expectation.case-overlap", "$/cases/2/when");
    }

    @Test
    void overlapDiagnosticListsAllEarlierCompatibleCases() {
        ScenarioSpecification scenario = plainScenario(document -> {
            parameter(document, "a", "integer", IntNode.valueOf(3));
            parameter(document, "b", "integer", IntNode.valueOf(3));
        });
        ExpectedResultSpecification expected = expected(document -> {
            addPlainFailCase(document, Map.of("a", IntNode.valueOf(1)));
            addPlainFailCase(document, Map.of("a", IntNode.valueOf(2)));
            addPlainFailCase(document, Map.of("b", IntNode.valueOf(1)));
        });

        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));

        ExpectationIssue issue = exception.issues().stream()
                .filter(candidate -> candidate.path().equals("$/cases/2/when"))
                .findFirst().orElseThrow();
        assertTrue(issue.message().contains("[0, 1]"));
    }

    @Test
    void contradictoryCasesDoNotOverlapAndMayShareAReplacement() {
        ScenarioSpecification scenario = parameterizedPlainScenario("a", "integer", IntNode.valueOf(3));
        ExpectedResultSpecification expected = expected(document -> {
            addPlainFailCase(document, Map.of("a", IntNode.valueOf(1)));
            addPlainFailCase(document, Map.of("a", IntNode.valueOf(2)));
        });

        ResolvedScenarioPlan plan = selector.select(bundle(scenario, expected),
                parameterResolver.resolve(scenario, ResolutionRequest.none()));

        assertEquals(ExpectationSelectionKind.DEFAULT, plan.selectedExpectation().kind());
    }

    @Test
    void rejectsCaseReplacementEqualToDefaultRegardlessOfFieldOrder() {
        ScenarioSpecification scenario = parameterizedPlainScenario("a", "integer", IntNode.valueOf(1));
        ExpectedResultSpecification expected = expected(document -> {
            ObjectNode defaultNode = document.withObject("default");
            defaultNode.removeAll();
            defaultNode.put("outcome", "fail");
            defaultNode.put("oracle", "kafka.id-set");
            defaultNode.put("reason", "validator.kafka.id-set.missing-ids");
            ObjectNode caseNode = document.putArray("cases").addObject();
            caseNode.putObject("when").put("a", 2);
            caseNode.put("reason", "validator.kafka.id-set.missing-ids");
            caseNode.put("outcome", "fail");
            caseNode.put("oracle", "kafka.id-set");
        });

        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));

        assertHasIssue(exception, "expectation.case-redundant", "$/cases/0");
    }

    @Test
    void selectedExpectationIsDefensiveAndRejectsIncompatibleSides() {
        ScenarioSpecification scenario = parameterizedPlainScenario("color", "string", TextNode.valueOf("red"));
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of("color", TextNode.valueOf("red"))));
        ResolvedScenarioPlan plan = selector.select(bundle(scenario, expected),
                parameterResolver.resolve(scenario, ResolutionRequest.none()));
        SelectedExpectation selected = plan.selectedExpectation();

        assertThrows(UnsupportedOperationException.class,
                () -> selected.conditions().put("other", TextNode.valueOf("value")));
        ObjectNode first = selected.expectation();
        first.put("outcome", "mutated");
        assertEquals("fail", selected.expectation().path("outcome").textValue());
        assertThrows(IllegalArgumentException.class,
                () -> selected.expectationFor(ScenarioSide.BASELINE));
        assertFalse(selected.expectation().has("when"));
    }

    @Test
    void rejectsResolvedScenarioFromAnotherBundleAsProgrammerMisuse() {
        ScenarioSpecification bundled = plainScenario(document -> {});
        ScenarioSpecification other = loader.validateScenarioDocument(
                resource("minimal.yaml").resolveSibling("other.yaml"), bundled.document());
        ResolvedScenario resolvedOther = parameterResolver.resolve(other, ResolutionRequest.none());

        assertThrows(IllegalArgumentException.class,
                () -> selector.select(bundle(bundled, expected(document -> {})), resolvedOther));
    }

    private void assertInvalidLiteral(String name, String type, JsonNode defaultValue, JsonNode invalidValue) {
        ScenarioSpecification scenario = parameterizedPlainScenario(name, type, defaultValue);
        ExpectedResultSpecification expected = expected(document -> addPlainFailCase(
                document, Map.of(name, invalidValue)));
        ExpectedResultSelectionException exception = assertThrows(
                ExpectedResultSelectionException.class,
                () -> selector.select(bundle(scenario, expected),
                        parameterResolver.resolve(scenario, ResolutionRequest.none())));
        assertHasIssue(exception, "expectation.case-value-invalid", "$/cases/0/when/" + name);
    }

    private ScenarioSpecification parameterizedPlainScenario(
            String name, String type, JsonNode defaultValue) {
        return plainScenario(document -> parameter(document, name, type, defaultValue));
    }

    private ScenarioSpecification plainScenario(Consumer<ObjectNode> changes) {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        changes.accept(document);
        return loader.validateScenarioDocument(source, document);
    }

    private ScenarioSpecification experimentScenario() {
        return plainScenario(document -> {
            parameter(document, "backend", "string", TextNode.valueOf("rocksdb"));
            parameter(document, "strategy", "string", TextNode.valueOf("INCREMENTING"));
            job(document).put("state_backend", "${backend}");
            job(document).withObject("sink").put("transaction_id_naming_strategy", "${strategy}");
            ObjectNode experiment = document.putObject("experiment");
            experiment.put("claim", "EOS holds regardless of state backend.");
            experiment.putArray("varies").add("backend");
            experiment.putObject("baseline").put("backend", "rocksdb");
            experiment.putObject("candidate").put("backend", "hashmap");
        });
    }

    private ExpectedResultSpecification expected(Consumer<ObjectNode> changes) {
        Path source = resource("minimal.expected.yaml");
        ObjectNode document = loader.loadExpectedResult(source).document();
        changes.accept(document);
        return loader.validateExpectedResultDocument(source, document);
    }

    private ExpectedResultSpecification experimentExpected(Consumer<ObjectNode> changes) {
        return expected(document -> {
            ObjectNode defaultNode = document.withObject("default");
            defaultNode.removeAll();
            defaultNode.putObject("baseline").put("outcome", "pass");
            ObjectNode candidate = defaultNode.putObject("candidate");
            candidate.put("outcome", "fail");
            candidate.put("oracle", "kafka.id-set");
            candidate.put("reason", "validator.kafka.id-set.missing-ids");
            changes.accept(document);
        });
    }

    private static ObjectNode parameter(
            ObjectNode document, String name, String type, JsonNode defaultValue) {
        ObjectNode parameters = document.get("parameters") instanceof ObjectNode existing
                ? existing
                : document.putObject("parameters");
        ObjectNode declaration = parameters.putObject(name);
        declaration.put("type", type);
        declaration.set("default", defaultValue);
        return declaration;
    }

    private static void addPlainFailCase(ObjectNode document, Map<String, JsonNode> conditions) {
        ArrayNode cases = document.get("cases") instanceof ArrayNode existing
                ? existing
                : document.putArray("cases");
        ObjectNode caseNode = cases.addObject();
        ObjectNode when = caseNode.putObject("when");
        conditions.forEach(when::set);
        caseNode.put("outcome", "fail");
        caseNode.put("oracle", "kafka.id-set");
        caseNode.put("reason", "validator.kafka.id-set.missing-ids");
    }

    private static ObjectNode job(ObjectNode document) {
        return (ObjectNode) document.at("/workload/jobs/0");
    }

    private static ScenarioBundle bundle(
            ScenarioSpecification scenario, ExpectedResultSpecification expected) {
        return new ScenarioBundle(scenario, expected);
    }

    private static void assertHasIssue(
            ExpectedResultSelectionException exception, String code, String path) {
        assertTrue(exception.issues().stream()
                        .anyMatch(issue -> issue.code().equals(code) && issue.path().equals(path)),
                () -> "Expected " + code + " at " + path + " but got " + exception.issues());
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
