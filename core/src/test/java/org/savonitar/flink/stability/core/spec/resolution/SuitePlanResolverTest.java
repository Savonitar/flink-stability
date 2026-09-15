package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalog;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalogTestFactory;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.spec.document.SuiteSpecification;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuitePlanResolverTest {
    private static final BigInteger ONE = BigInteger.ONE;

    private final SpecificationLoader loader = new SpecificationLoader();
    private final SuitePlanResolver resolver = new SuitePlanResolver();

    @Test
    void preservesSourceOrderAndBuildsStableEntryIdentity() {
        ScenarioBundle alpha = plainBundle("alpha", ONE, document -> {}, document -> {});
        ScenarioBundle beta = plainBundle("beta", ONE, document -> {}, document -> {});
        SuiteSpecification suite = suite("ordered-suite", scenarios -> {
            entry(scenarios, "beta").put("as", "beta-first");
            entry(scenarios, "alpha");
        });

        ResolvedSuitePlan plan = resolver.resolve(catalog(suite, alpha, beta), suite.name());

        assertSame(suite, plan.specification());
        assertEquals(List.of("beta-first", "alpha"), plan.entries().stream()
                .map(entry -> entry.identity().entryId())
                .toList());

        SuiteEntryIdentity first = plan.entries().getFirst().identity();
        assertEquals(suite.source(), first.suiteSource());
        assertEquals("ordered-suite", first.suiteName());
        assertEquals(0, first.entryIndex());
        assertEquals("$/scenarios/0", first.entryPointer());
        assertEquals("beta-first", first.entryId());
        assertEquals("beta", first.scenarioName());

        SuiteEntryIdentity second = plan.entries().get(1).identity();
        assertEquals(1, second.entryIndex());
        assertEquals("$/scenarios/1", second.entryPointer());
        assertEquals("alpha", second.entryId());
        assertEquals("alpha", second.scenarioName());
        assertSame(plan.entries().getFirst(), plan.entry("beta-first").orElseThrow());
        assertTrue(plan.entry("missing").isEmpty());
    }

    @Test
    void suiteBindingsSelectExpectationsAndGlobalSubmitOverridesWin() {
        ScenarioBundle bundle = profileBundle("parameterized");
        SuiteSpecification suite = suite("parameter-suite", scenarios -> entry(scenarios, "parameterized")
                .putObject("parameters").put("profile", "red"));
        SpecificationCatalog catalog = catalog(suite, bundle);

        ResolvedSuiteEntry suiteBound = resolver.resolve(catalog, suite.name())
                .entry("parameterized").orElseThrow();
        assertEquals(ExpectationSelectionKind.CASE,
                suiteBound.scenarioPlan().selectedExpectation().kind());
        assertEquals("$/cases/0",
                suiteBound.scenarioPlan().selectedExpectation().originPointer());
        assertEquals("red", effectiveValue(suiteBound, "profile").textValue());
        assertEquals(ParameterSource.SUITE_BINDING,
                effectiveParameter(suiteBound, "profile").source());

        ResolvedSuiteEntry submitOverridden = resolver.resolve(
                        catalog,
                        suite.name(),
                        Map.of("profile", TextNode.valueOf("green")))
                .entry("parameterized").orElseThrow();
        assertEquals(ExpectationSelectionKind.DEFAULT,
                submitOverridden.scenarioPlan().selectedExpectation().kind());
        assertEquals("green", effectiveValue(submitOverridden, "profile").textValue());
        assertEquals(ParameterSource.SUBMIT_OVERRIDE,
                effectiveParameter(submitOverridden, "profile").source());
    }

    @Test
    void globalSubmitOverridesRejectEveryEntryThatDoesNotDeclareTheKey() {
        ScenarioBundle parameterized = profileBundle("parameterized-global");
        ScenarioBundle concrete = plainBundle(
                "concrete-global", ONE, document -> {}, document -> {});
        SuiteSpecification suite = suite("strict-global-suite", scenarios -> {
            entry(scenarios, "parameterized-global");
            entry(scenarios, "concrete-global");
        });

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> resolver.resolve(
                        catalog(suite, parameterized, concrete),
                        suite.name(),
                        Map.of("profile", TextNode.valueOf("red"))));

        assertEquals(1, exception.issues().size());
        SuitePlanningIssue issue = exception.issues().getFirst();
        assertEquals(1, issue.entry().entryIndex());
        assertEquals("concrete-global", issue.entry().entryId());
        assertEquals("parameter.unknown-submit-override", issue.code());
        assertEquals("$/submit-overrides/profile", issue.path());
        assertEquals(concrete.scenario().source(), issue.source());
    }

    @Test
    void laterInvalidEntryPreventsReturningAPlanForTheValidPrefix() {
        ScenarioBundle valid = plainBundle(
                "valid-prefix", ONE, document -> {}, document -> {});
        ScenarioBundle invalid = plainBundle(
                "invalid-tail",
                ONE,
                document -> ((ObjectNode) document.at("/workload/jobs/0/source"))
                        .put("topic", "missing"),
                document -> {});
        SuiteSpecification suite = suite("no-partial-suite", scenarios -> {
            entry(scenarios, "valid-prefix");
            entry(scenarios, "invalid-tail");
        });

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> resolver.resolve(catalog(suite, valid, invalid), suite.name()));

        assertTrue(exception.issues().stream().allMatch(issue ->
                issue.entry().entryIndex() == 1
                        && issue.entry().entryId().equals("invalid-tail")));
        assertTrue(exception.issues().stream().anyMatch(issue ->
                issue.code().equals("preflight.reference.kafka-topic-not-found")));
    }

    @Test
    void calculatesPlainAndExperimentRunPoliciesWithoutIndependentBaselineSlots() {
        ScenarioBundle plain = plainBundle("plain-runs", BigInteger.valueOf(3),
                document -> {}, document -> {});
        ScenarioBundle experiment = experimentBundle("experiment-runs", BigInteger.valueOf(7));
        SuiteSpecification suite = suite("run-policy-suite", scenarios -> {
            entry(scenarios, "plain-runs").put("runs", 5);
            entry(scenarios, "experiment-runs").put("runs", 2);
        });

        ResolvedSuitePlan plan = resolver.resolve(catalog(suite, plain, experiment), suite.name());
        ResolvedSuiteEntry plainEntry = plan.entry("plain-runs").orElseThrow();
        assertEquals(BigInteger.valueOf(3), plainEntry.declaredRuns());
        assertEquals(BigInteger.valueOf(5), plainEntry.suiteRunsOverride().orElseThrow());
        assertEquals(BigInteger.valueOf(5), plainEntry.effectiveRuns());
        assertEquals(ScenarioSide.SINGLE, plainEntry.repeatedSide());

        ResolvedSuiteEntry experimentEntry = plan.entry("experiment-runs").orElseThrow();
        assertEquals(BigInteger.valueOf(7), experimentEntry.declaredRuns());
        assertEquals(BigInteger.valueOf(2), experimentEntry.suiteRunsOverride().orElseThrow());
        assertEquals(BigInteger.valueOf(2), experimentEntry.effectiveRuns());
        assertEquals(ScenarioSide.CANDIDATE, experimentEntry.repeatedSide());

        List<SuiteRunSlot> experimentSlots = slots(experimentEntry.requiredCleanRunSlots());
        assertEquals(List.of(ONE, BigInteger.TWO), experimentSlots.stream()
                .map(SuiteRunSlot::ordinal)
                .toList());
        assertTrue(experimentSlots.stream().allMatch(slot -> slot.side() == ScenarioSide.CANDIDATE));
        assertTrue(experimentSlots.stream().noneMatch(slot -> slot.side() == ScenarioSide.BASELINE));
    }

    @Test
    void usesTheCandidateResolvedRunsWhenExperimentSidesDiffer() {
        ScenarioBundle experiment = plainBundle("side-runs", ONE, document -> {
            parameter(document, "run_count", "integer", BigIntegerNode.valueOf(ONE));
            document.put("runs", "${run_count}");
            ObjectNode definition = document.putObject("experiment");
            definition.put("claim", "Candidate K is the experiment repetition count.");
            definition.putArray("varies").add("run_count");
            definition.putObject("baseline").put("run_count", 2);
            definition.putObject("candidate").put("run_count", 4);
        }, document -> {
            ObjectNode expectation = document.withObject("default");
            expectation.removeAll();
            expectation.putObject("baseline").put("outcome", "pass");
            expectation.putObject("candidate").put("outcome", "pass");
        });
        SuiteSpecification suite = suite(
                "side-runs-suite", scenarios -> entry(scenarios, "side-runs"));

        ResolvedSuiteEntry entry = resolver.resolve(
                        catalog(suite, experiment), suite.name())
                .entry("side-runs").orElseThrow();

        assertEquals(BigInteger.valueOf(2), entry.scenarioPlan().scenario()
                .side(ScenarioSide.BASELINE).document().path("runs").bigIntegerValue());
        assertEquals(BigInteger.valueOf(4), entry.declaredRuns());
        assertEquals(BigInteger.valueOf(4), entry.effectiveRuns());
        assertTrue(slots(entry.requiredCleanRunSlots()).stream()
                .allMatch(slot -> slot.side() == ScenarioSide.CANDIDATE));
    }

    @Test
    void rejectsExperimentHealthBudgetMismatchThroughTheSuiteBoundary() {
        ScenarioBundle experiment = plainBundle("side-health", ONE, document -> {
            ObjectNode retry = parameter(
                    document, "retry_count", "integer", BigIntegerNode.valueOf(ONE));
            retry.put("min", 0);
            document.put("health_retry_limit", "${retry_count}");
            ObjectNode definition = document.putObject("experiment");
            definition.put("claim", "One invocation has one shared retry budget.");
            definition.putArray("varies").add("retry_count");
            definition.putObject("baseline").put("retry_count", 1);
            definition.putObject("candidate").put("retry_count", 2);
        }, document -> {
            ObjectNode expectation = document.withObject("default");
            expectation.removeAll();
            expectation.putObject("baseline").put("outcome", "pass");
            expectation.putObject("candidate").put("outcome", "pass");
        });
        SuiteSpecification suite = suite(
                "side-health-suite", scenarios -> entry(scenarios, "side-health"));

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> resolver.resolve(catalog(suite, experiment), suite.name()));

        SuitePlanningIssue issue = exception.issues().stream()
                .filter(candidate -> candidate.code().equals(
                        "preflight.invocation.health-retry-limit-side-mismatch"))
                .findFirst().orElseThrow();
        assertEquals("side-health", issue.entry().entryId());
        assertEquals(experiment.scenario().source(), issue.source());
        assertEquals(ResolutionScope.COMMON, issue.scope());
        assertEquals("$/health_retry_limit", issue.path());
    }

    @Test
    void aggregatesSlotsInEntryMajorOrderAcrossRepeatedScenarioAliases() {
        ScenarioBundle repeated = plainBundle("repeated", ONE,
                document -> {}, document -> {});
        SuiteSpecification suite = suite("alias-suite", scenarios -> {
            entry(scenarios, "repeated").put("as", "short-run").put("runs", 2);
            entry(scenarios, "repeated").put("as", "long-run").put("runs", 3);
        });

        ResolvedSuitePlan plan = resolver.resolve(catalog(suite, repeated), suite.name());
        List<SuiteRunSlot> slots = slots(plan.requiredCleanRunSlots());

        assertEquals(List.of("short-run", "short-run", "long-run", "long-run", "long-run"),
                slots.stream().map(slot -> slot.entry().entryId()).toList());
        assertEquals(List.of(ONE, BigInteger.TWO, ONE, BigInteger.TWO, BigInteger.valueOf(3)),
                slots.stream().map(SuiteRunSlot::ordinal).toList());
    }

    @Test
    void runSlotIteratorsHonorEntryTransitionsAndExhaustion() {
        ScenarioBundle bundle = plainBundle("iterator-runs", ONE,
                document -> {}, document -> {});
        SuiteSpecification suite = suite("iterator-suite", scenarios -> {
            entry(scenarios, "iterator-runs").put("as", "first");
            entry(scenarios, "iterator-runs").put("as", "second");
        });
        Iterator<SuiteRunSlot> slots = resolver.resolve(
                        catalog(suite, bundle), suite.name())
                .requiredCleanRunSlots().iterator();

        assertTrue(slots.hasNext());
        assertEquals("first", slots.next().entry().entryId());
        assertTrue(slots.hasNext());
        assertEquals("second", slots.next().entry().entryId());
        assertFalse(slots.hasNext());
        assertFalse(slots.hasNext());
        assertThrows(NoSuchElementException.class, slots::next);
    }

    @Test
    void keepsArbitrarilyLargeRunCountsExactAndLazy() {
        BigInteger huge = new BigInteger("922337203685477580812345678901234567890");
        ScenarioBundle bundle = plainBundle("huge-runs", ONE,
                document -> {}, document -> {});
        SuiteSpecification suite = suite("huge-suite", scenarios ->
                entry(scenarios, "huge-runs").set(
                        "runs", BigIntegerNode.valueOf(huge)));

        ResolvedSuitePlan plan = resolver.resolve(catalog(suite, bundle), suite.name());
        ResolvedSuiteEntry resolved = plan.entry("huge-runs").orElseThrow();

        assertEquals(ONE, resolved.declaredRuns());
        assertEquals(huge, resolved.suiteRunsOverride().orElseThrow());
        assertEquals(huge, resolved.effectiveRuns());
        assertEquals(1, plan.entries().size());

        Iterator<SuiteRunSlot> first = plan.requiredCleanRunSlots().iterator();
        assertEquals(ONE, first.next().ordinal());
        assertEquals(BigInteger.TWO, first.next().ordinal());
        assertTrue(first.hasNext());

        Iterator<SuiteRunSlot> fresh = plan.requiredCleanRunSlots().iterator();
        assertEquals(ONE, fresh.next().ordinal());
    }

    @Test
    void aggregatesEntryFailuresAndRebasesSuiteBindingLocationsExactly() {
        ScenarioBundle bundle = plainBundle("invalid-bindings", ONE,
                document -> {}, document -> {});
        SuiteSpecification suite = suite("invalid-binding-suite", scenarios -> {
            entry(scenarios, "invalid-bindings").put("as", "first")
                    .putObject("parameters").put("first_unknown", "value");
            entry(scenarios, "invalid-bindings").put("as", "second")
                    .putObject("parameters").put("second_unknown", "value");
        });

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> resolver.resolve(catalog(suite, bundle), suite.name()));

        assertEquals(2, exception.issues().size());
        assertIssue(exception.issues().getFirst(), suite, 0, "first",
                "parameter.unknown-suite-binding",
                "$/scenarios/0/parameters/first_unknown");
        assertIssue(exception.issues().get(1), suite, 1, "second",
                "parameter.unknown-suite-binding",
                "$/scenarios/1/parameters/second_unknown");
        assertThrows(UnsupportedOperationException.class, () -> exception.issues().clear());
        assertTrue(exception.getMessage().contains("first_unknown"));
        assertTrue(exception.getMessage().contains("second_unknown"));
    }

    @Test
    void rejectsIndirectSuiteControlOfRunsAndHealthAtTheRealBindingPaths() {
        ScenarioBundle bundle = plainBundle("protected-policy", ONE, document -> {
            parameter(document, "run_count", "integer", BigIntegerNode.valueOf(ONE));
            parameter(document, "retry_count", "integer", BigIntegerNode.valueOf(ONE));
            document.put("runs", "${run_count}");
            document.put("health_retry_limit", "${retry_count}");
        }, document -> {});
        SuiteSpecification suite = suite("protected-policy-suite", scenarios -> {
            ObjectNode parameters = entry(scenarios, "protected-policy")
                    .putObject("parameters");
            parameters.put("run_count", 2);
            parameters.put("retry_count", 2);
        });

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> resolver.resolve(catalog(suite, bundle), suite.name()));

        assertEquals(List.of(
                        "parameter.suite-binding-controls-health-retry-limit"
                                + "@$/scenarios/0/parameters/retry_count",
                        "parameter.suite-binding-controls-runs"
                                + "@$/scenarios/0/parameters/run_count"),
                exception.issues().stream()
                        .map(issue -> issue.code() + "@" + issue.path())
                        .sorted()
                        .toList());
        assertTrue(exception.issues().stream()
                .allMatch(issue -> issue.source().equals(suite.source())));
    }

    @Test
    void preservesScenarioAndExpectedResultSourceIdentities() {
        ScenarioBundle bundle = profileBundle("source-preserved");
        SuiteSpecification suite = suite("source-suite", scenarios -> entry(scenarios, "source-preserved"));

        ResolvedSuiteEntry entry = resolver.resolve(catalog(suite, bundle), suite.name())
                .entry("source-preserved").orElseThrow();

        assertSame(bundle.scenario(), entry.scenarioPlan().scenario().template());
        assertEquals(bundle.scenario().source(),
                entry.scenarioPlan().scenario().template().source());
        assertSame(bundle.expectedResult(),
                entry.scenarioPlan().selectedExpectation().specification());
        assertEquals(bundle.expectedResult().source(),
                entry.scenarioPlan().selectedExpectation().specification().source());
    }

    @Test
    void preservesOriginalScenarioAndExpectedDiagnosticLocations() {
        ScenarioBundle invalidTopology = plainBundle(
                "invalid-topology",
                ONE,
                document -> ((ObjectNode) document.at("/workload/jobs/0/source"))
                        .put("topic", "missing"),
                document -> {});
        ScenarioBundle invalidExpectation = plainBundle(
                "invalid-expectation",
                ONE,
                document -> {},
                document -> addPlainFailCase(
                        document, Map.of("unknown", TextNode.valueOf("value"))));
        SuiteSpecification suite = suite("diagnostic-source-suite", scenarios -> {
            entry(scenarios, "invalid-topology");
            entry(scenarios, "invalid-expectation");
        });

        SuitePlanningException exception = assertThrows(
                SuitePlanningException.class,
                () -> resolver.resolve(
                        catalog(suite, invalidTopology, invalidExpectation), suite.name()));

        SuitePlanningIssue topology = exception.issues().stream()
                .filter(issue -> issue.code().equals(
                        "preflight.reference.kafka-topic-not-found"))
                .findFirst().orElseThrow();
        assertEquals("invalid-topology", topology.entry().entryId());
        assertEquals(invalidTopology.scenario().source(), topology.source());
        assertEquals("$/workload/jobs/0/source/topic", topology.path());

        SuitePlanningIssue expectation = exception.issues().stream()
                .filter(issue -> issue.code().equals(
                        "expectation.case-unknown-parameter"))
                .findFirst().orElseThrow();
        assertEquals("invalid-expectation", expectation.entry().entryId());
        assertEquals(invalidExpectation.expectedResult().source(), expectation.source());
        assertEquals("$/cases/0/when/unknown", expectation.path());
    }

    @Test
    void returnedPlansAreImmutableAndReplanningIsDeterministic() {
        ScenarioBundle bundle = profileBundle("deterministic");
        SuiteSpecification suite = suite("deterministic-suite", scenarios -> entry(scenarios, "deterministic")
                .putObject("parameters").put("profile", "red"));
        SpecificationCatalog catalog = catalog(suite, bundle);

        ResolvedSuitePlan first = resolver.resolve(catalog, suite.name());
        ResolvedSuitePlan second = resolver.resolve(catalog, suite.name());

        assertNotSame(first, second);
        assertEquals(planSignature(first), planSignature(second));
        assertEquals(slots(first.requiredCleanRunSlots()), slots(second.requiredCleanRunSlots()));
        assertThrows(UnsupportedOperationException.class, () -> first.entries().clear());

        ObjectNode returnedSuiteDocument = first.specification().document();
        returnedSuiteDocument.withArray("scenarios").removeAll();
        assertEquals(1, first.specification().at("/scenarios").size());

        ObjectNode returnedScenarioDocument = first.entries().getFirst().scenarioPlan()
                .scenario().side(ScenarioSide.SINGLE).document();
        returnedScenarioDocument.put("runs", 999);
        assertEquals(ONE, first.entries().getFirst().declaredRuns());
        assertEquals(ONE, first.entries().getFirst().scenarioPlan().scenario()
                .side(ScenarioSide.SINGLE).document().path("runs").bigIntegerValue());

        ObjectNode returnedExpectation = first.entries().getFirst().scenarioPlan()
                .selectedExpectation().expectation();
        returnedExpectation.put("outcome", "mutated");
        assertEquals("fail", first.entries().getFirst().scenarioPlan()
                .selectedExpectation().expectation().path("outcome").textValue());
    }

    private ScenarioBundle profileBundle(String name) {
        return plainBundle(name, ONE, document -> {
            parameter(document, "profile", "string", TextNode.valueOf("blue"));
            document.withObject("setup").withObject("flink")
                    .put("image", "flink:2.2.0-${profile}");
        }, document -> addPlainFailCase(
                document, Map.of("profile", TextNode.valueOf("red"))));
    }

    private ScenarioBundle experimentBundle(String name, BigInteger runs) {
        return plainBundle(name, runs, document -> {
            parameter(document, "backend", "string", TextNode.valueOf("rocksdb"));
            ((ObjectNode) document.at("/workload/jobs/0")).put("state_backend", "${backend}");
            ObjectNode experiment = document.putObject("experiment");
            experiment.put("claim", "EOS holds for both state backends.");
            experiment.putArray("varies").add("backend");
            experiment.putObject("baseline").put("backend", "rocksdb");
            experiment.putObject("candidate").put("backend", "hashmap");
        }, document -> {
            ObjectNode expectation = document.withObject("default");
            expectation.removeAll();
            expectation.putObject("baseline").put("outcome", "pass");
            expectation.putObject("candidate").put("outcome", "pass");
        });
    }

    private ScenarioBundle plainBundle(
            String name,
            BigInteger runs,
            Consumer<ObjectNode> scenarioChanges,
            Consumer<ObjectNode> expectedChanges) {
        Path scenarioSource = resource("minimal.yaml").resolveSibling(name + ".yaml");
        ObjectNode scenarioDocument = loader.loadScenario(resource("minimal.yaml")).document();
        scenarioDocument.withObject("meta").put("name", name);
        scenarioDocument.set("runs", BigIntegerNode.valueOf(runs));
        scenarioChanges.accept(scenarioDocument);
        ScenarioSpecification scenario = loader.validateScenarioDocument(
                scenarioSource, scenarioDocument);

        Path expectedSource = scenarioSource.resolveSibling(name + ".expected.yaml");
        ObjectNode expectedDocument = loader.loadExpectedResult(resource("minimal.expected.yaml")).document();
        expectedDocument.withObject("meta")
                .put("name", name + ".expected")
                .put("scenario", name);
        expectedChanges.accept(expectedDocument);
        ExpectedResultSpecification expected = loader.validateExpectedResultDocument(
                expectedSource, expectedDocument);
        return new ScenarioBundle(scenario, expected);
    }

    private SuiteSpecification suite(String name, Consumer<ArrayNode> entries) {
        ObjectNode document = loader.loadSuite(resource("smoke-suite.yaml")).document();
        document.withObject("meta").put("name", name);
        ArrayNode scenarios = document.withArray("scenarios");
        scenarios.removeAll();
        entries.accept(scenarios);
        Path source = resource("smoke-suite.yaml").resolveSibling(name + ".yaml");
        return loader.validateSuiteDocument(source, document);
    }

    private static ObjectNode entry(ArrayNode scenarios, String scenarioName) {
        return scenarios.addObject().put("scenario", scenarioName);
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

    private static SpecificationCatalog catalog(
            SuiteSpecification suite, ScenarioBundle... bundles) {
        return SpecificationCatalogTestFactory.catalog(suite, bundles);
    }

    private static EffectiveParameter effectiveParameter(
            ResolvedSuiteEntry entry, String parameterName) {
        return entry.scenarioPlan().scenario().commonEffectiveParameters().get(parameterName);
    }

    private static JsonNode effectiveValue(
            ResolvedSuiteEntry entry, String parameterName) {
        return effectiveParameter(entry, parameterName).value();
    }

    private static List<SuiteRunSlot> slots(Iterable<SuiteRunSlot> iterable) {
        List<SuiteRunSlot> slots = new ArrayList<>();
        iterable.forEach(slots::add);
        return List.copyOf(slots);
    }

    private static List<String> planSignature(ResolvedSuitePlan plan) {
        return plan.entries().stream()
                .map(entry -> entry.identity() + "|" + entry.declaredRuns()
                        + "|" + entry.suiteRunsOverride() + "|" + entry.effectiveRuns()
                        + "|" + entry.repeatedSide() + "|"
                        + entry.scenarioPlan().selectedExpectation().originPointer())
                .toList();
    }

    private static void assertIssue(
            SuitePlanningIssue issue,
            SuiteSpecification suite,
            int index,
            String entryId,
            String code,
            String path) {
        assertEquals(suite.source(), issue.source());
        assertEquals(suite.source(), issue.entry().suiteSource());
        assertEquals(suite.name(), issue.entry().suiteName());
        assertEquals(index, issue.entry().entryIndex());
        assertEquals(entryId, issue.entry().entryId());
        assertEquals(ResolutionScope.COMMON, issue.scope());
        assertEquals(code, issue.code());
        assertEquals(path, issue.path());
        assertFalse(issue.message().isBlank());
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
