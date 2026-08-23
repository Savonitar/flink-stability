package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScenarioParameterResolverTest {
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    private final SpecificationLoader loader = new SpecificationLoader();
    private final ScenarioParameterResolver resolver = new ScenarioParameterResolver(loader);

    @Test
    void materializesEveryV1DefaultWithoutMutatingTheTemplate() {
        ScenarioSpecification scenario = scenario(document -> {});

        ResolvedSide resolved = resolver.resolve(scenario, ResolutionRequest.none())
                .side(ScenarioSide.SINGLE);
        ObjectNode document = resolved.document();

        assertEquals(1, document.path("health_retry_limit").intValue());
        assertEquals("kraft", document.at("/setup/kafka/clusters/main/mode").textValue());
        assertEquals(1, document.at("/setup/flink/jobmanagers").intValue());
        assertEquals(1, document.at("/setup/flink/taskmanagers").intValue());
        assertEquals("auto", document.at("/workload/jobs/0/start").textValue());
        assertEquals(Set.of(
                        "$/health_retry_limit",
                        "$/setup/kafka/clusters/main/mode",
                        "$/setup/flink/jobmanagers",
                        "$/setup/flink/taskmanagers",
                        "$/workload/jobs/0/start"),
                resolved.appliedDefaults().stream().map(AppliedDefault::path).collect(Collectors.toSet()));

        assertFalse(scenario.document().has("health_retry_limit"));
        assertTrue(scenario.at("/setup/kafka/clusters/main/mode").isMissingNode());
        assertTrue(scenario.at("/setup/flink/jobmanagers").isMissingNode());
        assertTrue(scenario.at("/workload/jobs/0/start").isMissingNode());
    }

    @Test
    void preservesExplicitValuesInsteadOfApplyingDefaults() {
        ScenarioSpecification scenario = scenario(document -> {
            document.put("health_retry_limit", 3);
            kafkaCluster(document).put("mode", "kraft");
            flink(document).put("jobmanagers", 1).put("taskmanagers", 3);
            job(document).put("start", "manual");
        });

        ResolvedSide resolved = resolver.resolve(scenario, ResolutionRequest.none())
                .side(ScenarioSide.SINGLE);

        assertEquals(3, resolved.document().path("health_retry_limit").intValue());
        assertEquals(1, resolved.document().at("/setup/flink/jobmanagers").intValue());
        assertEquals(3, resolved.document().at("/setup/flink/taskmanagers").intValue());
        assertEquals("manual", resolved.document().at("/workload/jobs/0/start").textValue());
        assertTrue(resolved.appliedDefaults().isEmpty());
    }

    @Test
    void appliesDefaultThenSuiteThenSubmitPrecedenceAndRecordsTheWinner() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "flink_version", "string", TextNode.valueOf("1.19"));
            flink(document).put("image", "flink:${flink_version}");
        });

        ResolvedScenario fromDefault = resolver.resolve(scenario, ResolutionRequest.none());
        assertEffective(fromDefault, "flink_version", "1.19", ParameterSource.SCENARIO_DEFAULT);
        assertEquals("flink:1.19", fromDefault.side(ScenarioSide.SINGLE)
                .document().at("/setup/flink/image").textValue());

        ResolvedScenario fromSuite = resolver.resolve(scenario, new ResolutionRequest(
                Map.of("flink_version", TextNode.valueOf("1.20")), Map.of()));
        assertEffective(fromSuite, "flink_version", "1.20", ParameterSource.SUITE_BINDING);

        ResolvedScenario fromSubmit = resolver.resolve(scenario, new ResolutionRequest(
                Map.of("flink_version", TextNode.valueOf("1.20")),
                Map.of("flink_version", TextNode.valueOf("1.21"))));
        assertEffective(fromSubmit, "flink_version", "1.21", ParameterSource.SUBMIT_OVERRIDE);
        assertEquals("flink:1.21", fromSubmit.side(ScenarioSide.SINGLE)
                .document().at("/setup/flink/image").textValue());
    }

    @Test
    void rejectsIndirectSuiteAndSubmitControlOfInvocationPolicyFields() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "run_count", "integer", IntNode.valueOf(1));
            parameterWithDefault(document, "retry_count", "integer", IntNode.valueOf(1));
            document.put("runs", "${run_count}");
            document.put("health_retry_limit", "${retry_count}");
        });

        ResolvedSide fromScenarioDefaults = resolver.resolve(
                scenario, ResolutionRequest.none()).side(ScenarioSide.SINGLE);
        assertEquals(1, fromScenarioDefaults.document().path("runs").intValue());
        assertEquals(1, fromScenarioDefaults.document()
                .path("health_retry_limit").intValue());

        ScenarioResolutionException suiteRuns = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of("run_count", IntNode.valueOf(2)), Map.of())));
        assertHasIssue(
                suiteRuns,
                "parameter.suite-binding-controls-runs",
                ResolutionScope.COMMON,
                "$/suite-bindings/run_count");

        ScenarioResolutionException submitRuns = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of(), Map.of("run_count", IntNode.valueOf(2)))));
        assertHasIssue(
                submitRuns,
                "parameter.submit-override-controls-runs",
                ResolutionScope.COMMON,
                "$/submit-overrides/run_count");

        ScenarioResolutionException suiteHealth = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of("retry_count", IntNode.valueOf(2)), Map.of())));
        assertHasIssue(
                suiteHealth,
                "parameter.suite-binding-controls-health-retry-limit",
                ResolutionScope.COMMON,
                "$/suite-bindings/retry_count");

        ScenarioResolutionException submitHealth = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of(), Map.of("retry_count", IntNode.valueOf(2)))));
        assertHasIssue(
                submitHealth,
                "parameter.submit-override-controls-health-retry-limit",
                ResolutionScope.COMMON,
                "$/submit-overrides/retry_count");

        ScenarioResolutionException bothRunBindings = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of("run_count", IntNode.valueOf(2)),
                        Map.of("run_count", IntNode.valueOf(3)))));
        assertHasIssue(
                bothRunBindings,
                "parameter.suite-binding-controls-runs",
                ResolutionScope.COMMON,
                "$/suite-bindings/run_count");
        assertHasIssue(
                bothRunBindings,
                "parameter.submit-override-controls-runs",
                ResolutionScope.COMMON,
                "$/submit-overrides/run_count");
    }

    @Test
    void preservesWholeScalarIntegerAndBooleanTypesAcrossNumericFields() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "broker_count", "integer", IntNode.valueOf(2));
            parameterWithDefault(document, "partition_count", "integer", IntNode.valueOf(3));
            parameterWithDefault(document, "parallelism", "integer", IntNode.valueOf(4));
            parameterWithDefault(document, "ttl_enabled", "boolean", BooleanNode.TRUE);
            kafkaCluster(document).put("brokers", "${broker_count}");
            inputTopic(document).put("partitions", "${partition_count}");
            job(document).put("parallelism", "${parallelism}");
            ObjectNode ttl = job(document).putObject("state_ttl");
            ttl.put("enabled", "${ttl_enabled}");
            ttl.put("ttl", "10s");
        });

        ObjectNode document = resolver.resolve(scenario, ResolutionRequest.none())
                .side(ScenarioSide.SINGLE).document();

        assertTrue(document.at("/setup/kafka/clusters/main/brokers").isIntegralNumber());
        assertEquals(2, document.at("/setup/kafka/clusters/main/brokers").intValue());
        assertEquals(3, document.at("/setup/kafka/clusters/main/topics/0/partitions").intValue());
        assertEquals(4, document.at("/workload/jobs/0/parallelism").intValue());
        assertTrue(document.at("/workload/jobs/0/state_ttl/enabled").isBoolean());
        assertTrue(document.at("/workload/jobs/0/state_ttl/enabled").booleanValue());
    }

    @Test
    void rendersEmbeddedPrimitiveReferencesAsTextAndRecordsProvenance() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "major", "integer", IntNode.valueOf(20));
            parameterWithDefault(document, "stable", "boolean", BooleanNode.TRUE);
            flink(document).put("image", "flink:${major}.${major}-stable-${stable}");
        });

        ResolvedSide side = resolver.resolve(scenario, ResolutionRequest.none()).side(ScenarioSide.SINGLE);

        assertEquals("flink:20.20-stable-true", side.document().at("/setup/flink/image").textValue());
        assertEquals(Set.of("major", "stable"),
                side.interpolationProvenance().get("$/setup/flink/image"));
    }

    @Test
    void acceptsAValidEmbeddedKafkaTopicAndRejectsAnInvalidResolvedTopic() {
        ScenarioSpecification valid = scenario(document -> {
            parameterWithDefault(document, "suffix", "string", TextNode.valueOf("blue"));
            inputTopic(document).put("name", "input-${suffix}");
            job(document).withObject("source").put("topic", "input-${suffix}");
        });

        assertEquals("input-blue", resolver.resolve(valid, ResolutionRequest.none())
                .side(ScenarioSide.SINGLE).document()
                .at("/setup/kafka/clusters/main/topics/0/name").textValue());

        ScenarioSpecification invalid = scenario(document -> {
            parameterWithDefault(document, "suffix", "string", TextNode.valueOf("/bad"));
            inputTopic(document).put("name", "input-${suffix}");
        });
        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(invalid, ResolutionRequest.none()));

        assertHasIssue(exception, "schema.one-of", ResolutionScope.SINGLE,
                "$/setup/kafka/clusters/main/topics/0/name");
    }

    @Test
    void validatesResolvedIntegerBoundsThroughTheScenarioSchema() {
        assertResolvedPositiveIntegerViolation(
                "$/setup/kafka/clusters/main/brokers",
                document -> kafkaCluster(document).put("brokers", "${count}"));
        assertResolvedPositiveIntegerViolation(
                "$/setup/kafka/clusters/main/topics/0/partitions",
                document -> inputTopic(document).put("partitions", "${count}"));
        assertResolvedPositiveIntegerViolation(
                "$/workload/jobs/0/parallelism",
                document -> job(document).put("parallelism", "${count}"));
    }

    @Test
    void validatesResolvedConditionalShapesThroughTheScenarioSchema() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "ttl_enabled", "boolean", BooleanNode.FALSE);
            ObjectNode ttl = job(document).putObject("state_ttl");
            ttl.put("enabled", "${ttl_enabled}");
            ttl.put("ttl", "10s");
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "schema.one-of", ResolutionScope.SINGLE,
                "$/workload/jobs/0/state_ttl");
    }

    @Test
    void rejectsUnsupportedCapabilitiesBeforeResolvedSchemaValidation() {
        ScenarioSpecification stateBackend = scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("forst"));
            job(document).put("state_backend", "${backend}");
        });
        ScenarioResolutionException backendException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(stateBackend, ResolutionRequest.none()));
        assertHasIssue(backendException, "capability.state-backend.unsupported", ResolutionScope.SINGLE,
                "$/workload/jobs/0/state_backend");
        assertTrue(backendException.issues().stream().noneMatch(issue -> issue.code().startsWith("schema.")));

        ScenarioSpecification kafkaMode = scenario(document ->
                kafkaCluster(document).put("mode", "zookeeper"));
        ScenarioResolutionException kafkaException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(kafkaMode, ResolutionRequest.none()));
        assertHasIssue(kafkaException, "capability.kafka-mode.unsupported", ResolutionScope.SINGLE,
                "$/setup/kafka/clusters/main/mode");

        ScenarioSpecification multipleJobManagers = scenario(document ->
                flink(document).put("jobmanagers", 2));
        ScenarioResolutionException jobManagerException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(multipleJobManagers, ResolutionRequest.none()));
        assertHasIssue(jobManagerException, "capability.multiple-jobmanagers.unsupported", ResolutionScope.SINGLE,
                "$/setup/flink/jobmanagers");
    }

    @Test
    void reportsUnsupportedExperimentCapabilitiesAgainstTheAffectedSide() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("rocksdb"));
            job(document).put("state_backend", "${backend}");
            experiment(document, "backend", TextNode.valueOf("rocksdb"), TextNode.valueOf("forst"));
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "capability.state-backend.unsupported", ResolutionScope.CANDIDATE,
                "$/workload/jobs/0/state_backend");
        assertFalse(exception.issues().stream().anyMatch(issue -> issue.scope() == ResolutionScope.BASELINE));
    }

    @Test
    void validatesSinkAndRestoreCapabilityRegistries() {
        ScenarioSpecification guarantee = scenario(document ->
                job(document).withObject("sink").put("delivery_guarantee", "MAYBE"));
        ScenarioResolutionException guaranteeException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(guarantee, ResolutionRequest.none()));
        assertHasIssue(guaranteeException, "capability.delivery-guarantee.unsupported", ResolutionScope.SINGLE,
                "$/workload/jobs/0/sink/delivery_guarantee");

        ScenarioSpecification naming = scenario(document ->
                job(document).withObject("sink").put("transaction_id_naming_strategy", "RANDOM"));
        ScenarioResolutionException namingException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(naming, ResolutionRequest.none()));
        assertHasIssue(namingException, "capability.transaction-id-naming-strategy.unsupported",
                ResolutionScope.SINGLE, "$/workload/jobs/0/sink/transaction_id_naming_strategy");

        ScenarioSpecification restore = scenario(document -> {
            ArrayNode phases = document.putArray("phases");
            ObjectNode phase = phases.addObject();
            phase.put("name", "restore");
            ObjectNode restoreBody = phase.putArray("steps").addObject().putObject("restore");
            restoreBody.put("job", "eos-job");
            restoreBody.put("from", "latest-checkpoint");
            restoreBody.put("mode", "take-ownership");
        });
        ScenarioResolutionException restoreException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(restore, ResolutionRequest.none()));
        assertHasIssue(restoreException, "capability.restore-mode.unsupported", ResolutionScope.SINGLE,
                "$/phases/0/steps/0/restore/mode");
    }

    @Test
    void rejectsUnknownSubmitAndSuiteBindings() {
        ScenarioSpecification scenario = scenario(document ->
                parameterWithDefault(document, "known", "string", TextNode.valueOf("value")));

        ScenarioResolutionException submitException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of(), Map.of("unknown", TextNode.valueOf("value")))));
        assertHasIssue(submitException, "parameter.unknown-submit-override", ResolutionScope.COMMON,
                "$/submit-overrides/unknown");
        assertTrue(submitException.getMessage().contains("known"));

        ScenarioResolutionException suiteException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of("unknown", TextNode.valueOf("value")), Map.of())));
        assertHasIssue(suiteException, "parameter.unknown-suite-binding", ResolutionScope.COMMON,
                "$/suite-bindings/unknown");
    }

    @Test
    void validatesEveryBindingEvenWhenAValidHigherPrecedenceValueExists() {
        ScenarioSpecification scenario = scenario(document -> {
            ObjectNode declaration = parameterWithDefault(
                    document, "count", "integer", IntNode.valueOf(1));
            declaration.put("min", 1).put("max", 5);
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of("count", TextNode.valueOf("wrong")),
                        Map.of("count", IntNode.valueOf(3)))));

        assertHasIssue(exception, "parameter.type-mismatch", ResolutionScope.COMMON,
                "$/suite-bindings/count");

        ScenarioResolutionException boundsException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of("count", IntNode.valueOf(0)),
                        Map.of("count", IntNode.valueOf(3)))));
        assertHasIssue(boundsException, "parameter.out-of-range", ResolutionScope.COMMON,
                "$/suite-bindings/count");
    }

    @Test
    void rejectsInvalidDurationsAndRecursiveParameterValues() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "timeout", "duration", TextNode.valueOf("10s"));
            parameterWithDefault(document, "label", "string", TextNode.valueOf("plain"));
        });

        ScenarioResolutionException durationException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of(), Map.of("timeout", TextNode.valueOf("1m30s")))));
        assertHasIssue(durationException, "parameter.invalid-duration", ResolutionScope.COMMON,
                "$/submit-overrides/timeout");

        ScenarioResolutionException recursiveException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of(), Map.of("label", TextNode.valueOf("${label}")))));
        assertHasIssue(recursiveException, "parameter.recursive-expansion", ResolutionScope.COMMON,
                "$/submit-overrides/label");
    }

    @Test
    void rejectsTemplatesInParameterDeclarationsAndMapKeys() {
        ScenarioSpecification recursiveDefault = scenario(document -> {
            parameterWithDefault(document, "first", "string", TextNode.valueOf("${second}"));
            parameterWithDefault(document, "second", "string", TextNode.valueOf("value"));
        });
        ScenarioResolutionException declarationException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(recursiveDefault, ResolutionRequest.none()));
        assertHasIssue(declarationException, "parameter.recursive-expansion", ResolutionScope.COMMON,
                "$/parameters/first/default");

        ScenarioSpecification mapKey = scenario(document -> {
            parameterWithDefault(document, "config_key", "string", TextNode.valueOf("key"));
            flink(document).putObject("config").put("${config_key}", "value");
        });
        ScenarioResolutionException keyException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(mapKey, ResolutionRequest.none()));
        assertHasIssue(keyException, "parameter.reference-in-map-key", ResolutionScope.SINGLE,
                "$/setup/flink/config/${config_key}");
    }

    @Test
    void rejectsAnUndeclaredOrUnresolvedBodyReference() {
        ScenarioSpecification scenario = scenario(document ->
                kafkaCluster(document).put("brokers", "${missing}"));

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "parameter.unknown-reference", ResolutionScope.SINGLE,
                "$/setup/kafka/clusters/main/brokers");
    }

    @Test
    void rejectsMissingRequiredParameterValues() {
        ScenarioSpecification scenario = scenario(document -> {
            ObjectNode declaration = parameters(document).putObject("record_count");
            declaration.put("type", "integer").put("required", true).put("min", 1);
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "parameter.required-value-missing", ResolutionScope.SINGLE,
                "$/parameters/record_count");
    }

    @Test
    void rejectsIncoherentDeclarationBoundsAndOutOfRangeDefaults() {
        ScenarioSpecification scenario = scenario(document -> {
            ObjectNode declaration = parameterWithDefault(
                    document, "count", "integer", IntNode.valueOf(5));
            declaration.put("min", 10).put("max", 3);
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "parameter.invalid-bounds", ResolutionScope.COMMON,
                "$/parameters/count");
        assertHasIssue(exception, "parameter.out-of-range", ResolutionScope.COMMON,
                "$/parameters/count/default");
    }

    @Test
    void resolvesExperimentSidesOnlyThroughDeclaredVariedParameters() {
        ScenarioSpecification scenario = experimentScenario();

        ResolvedScenario resolved = resolver.resolve(scenario, ResolutionRequest.none());

        assertTrue(resolved.isExperiment());
        assertEquals(2, resolved.sides().size());
        ResolvedSide baseline = resolved.side(ScenarioSide.BASELINE);
        ResolvedSide candidate = resolved.side(ScenarioSide.CANDIDATE);
        assertEquals("rocksdb", baseline.document().at("/workload/jobs/0/state_backend").textValue());
        assertEquals("hashmap", candidate.document().at("/workload/jobs/0/state_backend").textValue());
        assertEquals(ParameterSource.EXPERIMENT_BASELINE,
                baseline.effectiveParameters().get("backend").source());
        assertEquals(ParameterSource.EXPERIMENT_CANDIDATE,
                candidate.effectiveParameters().get("backend").source());
        assertFalse(resolved.commonEffectiveParameters().containsKey("backend"));
        assertEquals("The declared dimension is the only difference.",
                resolved.experiment().orElseThrow().claim());
        assertFalse(baseline.document().has("parameters"));
        assertFalse(baseline.document().has("experiment"));
        assertEquals(Set.of("backend"),
                baseline.interpolationProvenance().get("$/workload/jobs/0/state_backend"));
        assertEquals(baseline.appliedDefaults().stream().map(AppliedDefault::path).collect(Collectors.toSet()),
                candidate.appliedDefaults().stream().map(AppliedDefault::path).collect(Collectors.toSet()));
    }

    @Test
    void resolvesExperimentClaimFromCommonParametersAndRejectsVariedReferences() {
        ScenarioSpecification commonClaim = scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("rocksdb"));
            parameterWithDefault(document, "version", "string", TextNode.valueOf("1.20"));
            job(document).put("state_backend", "${backend}");
            experiment(document, "backend", TextNode.valueOf("rocksdb"), TextNode.valueOf("hashmap"))
                    .put("claim", "EOS holds on Flink ${version} for both backends.");
        });
        ResolvedScenario resolved = resolver.resolve(commonClaim, ResolutionRequest.none());
        assertEquals("EOS holds on Flink 1.20 for both backends.",
                resolved.experiment().orElseThrow().claim());
        assertTrue(resolved.commonEffectiveParameters().containsKey("version"));

        ScenarioSpecification variedClaim = scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("rocksdb"));
            job(document).put("state_backend", "${backend}");
            experiment(document, "backend", TextNode.valueOf("rocksdb"), TextNode.valueOf("hashmap"))
                    .put("claim", "EOS holds for ${backend}.");
        });
        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(variedClaim, ResolutionRequest.none()));
        assertHasIssue(exception, "parameter.varied-reference-in-experiment-claim", ResolutionScope.COMMON,
                "$/experiment/claim");
    }

    @Test
    void reportsBlankResolvedExperimentClaimsWithSourceContext() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("rocksdb"));
            parameterWithDefault(document, "label", "string", TextNode.valueOf("   "));
            job(document).put("state_backend", "${backend}");
            experiment(document, "backend", TextNode.valueOf("rocksdb"), TextNode.valueOf("hashmap"))
                    .put("claim", "${label}");
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "experiment.claim-blank", ResolutionScope.COMMON,
                "$/experiment/claim");
    }

    @Test
    void acceptsRequiredVariedValuesFromBothSidesAndRejectsAMissingSideValue() {
        ScenarioSpecification complete = scenario(document -> {
            ObjectNode backend = parameters(document).putObject("backend");
            backend.put("type", "string").put("required", true);
            job(document).put("state_backend", "${backend}");
            experiment(document, "backend", TextNode.valueOf("rocksdb"), TextNode.valueOf("hashmap"));
        });
        assertEquals("hashmap", resolver.resolve(complete, ResolutionRequest.none())
                .side(ScenarioSide.CANDIDATE).document().at("/workload/jobs/0/state_backend").textValue());

        ScenarioSpecification incomplete = scenario(document -> {
            ObjectNode backend = parameters(document).putObject("backend");
            backend.put("type", "string").put("required", true);
            parameterWithDefault(document, "other", "string", TextNode.valueOf("default"));
            job(document).put("state_backend", "${backend}");
            ObjectNode experiment = experiment(
                    document, "backend", TextNode.valueOf("rocksdb"), TextNode.valueOf("hashmap"));
            experiment.withArray("varies").add("other");
            experiment.withObject("candidate").remove("backend");
            experiment.withObject("candidate").put("other", "candidate");
        });
        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(incomplete, ResolutionRequest.none()));
        assertHasIssue(exception, "parameter.required-value-missing", ResolutionScope.CANDIDATE,
                "$/parameters/backend");
    }

    @Test
    void validatesExperimentSideTypesAndBounds() {
        ScenarioSpecification scenario = scenario(document -> {
            ObjectNode count = parameterWithDefault(document, "count", "integer", IntNode.valueOf(1));
            count.put("min", 1).put("max", 5);
            inputTopic(document).withObject("input_source").put("total", "${count}");
            experiment(document, "count", IntNode.valueOf(2), IntNode.valueOf(6));
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "parameter.out-of-range", ResolutionScope.CANDIDATE,
                "$/experiment/candidate/count");
    }

    @Test
    void driftValidatorChecksParametersAndRenderedDocumentsIndependently() {
        Path source = resource("minimal.yaml");
        ObjectNode baseline = NODES.objectNode().put("runs", 1).put("backend", "rocksdb");
        ObjectNode candidate = NODES.objectNode().put("runs", 2).put("backend", "hashmap");
        Map<String, EffectiveParameter> baselineParameters = Map.of(
                "backend", new EffectiveParameter(TextNode.valueOf("rocksdb"),
                        ParameterSource.EXPERIMENT_BASELINE),
                "unreferenced", new EffectiveParameter(IntNode.valueOf(1), ParameterSource.SCENARIO_DEFAULT));
        Map<String, EffectiveParameter> candidateParameters = Map.of(
                "backend", new EffectiveParameter(TextNode.valueOf("hashmap"),
                        ParameterSource.EXPERIMENT_CANDIDATE),
                "unreferenced", new EffectiveParameter(IntNode.valueOf(2), ParameterSource.SCENARIO_DEFAULT));
        List<ResolutionIssue> issues = new ExperimentDriftValidator().validate(
                source,
                Set.of("backend"),
                baselineParameters,
                candidateParameters,
                baseline,
                candidate,
                Map.of("$/backend", Set.of("backend")),
                Map.of("$/backend", Set.of("backend")));

        assertTrue(issues.stream().anyMatch(issue -> issue.path().equals("$/parameters/unreferenced")));
        assertTrue(issues.stream().anyMatch(issue -> issue.path().equals("$/runs")));
        assertFalse(issues.stream().anyMatch(issue -> issue.path().equals("$/backend")));
    }

    @Test
    void resolvedScenarioEnforcesPlainAndExperimentSideTopology() {
        ResolvedScenario plain = resolver.resolve(scenario(document -> {}), ResolutionRequest.none());
        ResolvedScenario experiment = resolver.resolve(experimentScenario(), ResolutionRequest.none());
        ResolvedSide single = plain.side(ScenarioSide.SINGLE);
        ResolvedSide baseline = experiment.side(ScenarioSide.BASELINE);
        ResolvedSide candidate = experiment.side(ScenarioSide.CANDIDATE);
        ResolvedExperiment metadata = experiment.experiment().orElseThrow();

        assertThrows(IllegalArgumentException.class,
                () -> new ResolvedScenario(plain.template(), Map.of(), null, List.of(baseline)));
        assertThrows(IllegalArgumentException.class,
                () -> new ResolvedScenario(experiment.template(), Map.of(), metadata,
                        List.of(candidate, candidate)));
        assertThrows(IllegalArgumentException.class,
                () -> new ResolvedScenario(experiment.template(),
                        Map.of("backend", candidate.effectiveParameters().get("backend")), metadata,
                        List.of(baseline, candidate)));
        assertEquals(ScenarioSide.SINGLE, single.side());
    }

    @Test
    void rejectsSuiteAndSubmitBindingsForExperimentVaries() {
        ScenarioSpecification scenario = experimentScenario();

        ScenarioResolutionException suiteException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of("backend", TextNode.valueOf("rocksdb")), Map.of())));
        assertHasIssue(suiteException, "parameter.varied-suite-binding", ResolutionScope.COMMON,
                "$/suite-bindings/backend");

        ScenarioResolutionException submitException = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, new ResolutionRequest(
                        Map.of(), Map.of("backend", TextNode.valueOf("rocksdb")))));
        assertHasIssue(submitException, "parameter.varied-submit-override", ResolutionScope.COMMON,
                "$/submit-overrides/backend");
    }

    @Test
    void rejectsExperimentSideKeysOutsideVaries() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("rocksdb"));
            parameterWithDefault(document, "other", "integer", IntNode.valueOf(1));
            job(document).put("state_backend", "${backend}");
            ObjectNode experiment = experiment(document, "backend", TextNode.valueOf("rocksdb"),
                    TextNode.valueOf("hashmap"));
            experiment.withObject("baseline").put("other", 2);
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "parameter.side-key-not-varied", ResolutionScope.BASELINE,
                "$/experiment/baseline/other");
    }

    @Test
    void rejectsUndeclaredExperimentVaries() {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("rocksdb"));
            job(document).put("state_backend", "${backend}");
            experiment(document, "unknown", TextNode.valueOf("one"), TextNode.valueOf("two"));
        });

        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertHasIssue(exception, "parameter.unknown-varies", ResolutionScope.COMMON,
                "$/experiment/varies/0");
    }

    @Test
    void returnsDefensiveCopiesOfDocumentsRequestsAndEffectiveValues() {
        ObjectNode mutableOverride = NODES.objectNode().put("nested", "original");
        ResolutionRequest request = new ResolutionRequest(Map.of("value", mutableOverride), Map.of());
        mutableOverride.put("nested", "mutated");
        assertEquals("original", request.suiteBindings().get("value").path("nested").textValue());
        JsonNode returnedRequestValue = request.suiteBindings().get("value");
        ((ObjectNode) returnedRequestValue).put("nested", "also-mutated");
        assertEquals("original", request.suiteBindings().get("value").path("nested").textValue());

        ScenarioSpecification scenario = scenario(document ->
                parameterWithDefault(document, "version", "string", TextNode.valueOf("1.20")));
        ResolvedScenario resolved = resolver.resolve(scenario, ResolutionRequest.none());
        ObjectNode firstDocument = resolved.side(ScenarioSide.SINGLE).document();
        firstDocument.put("format", "mutated");
        assertEquals("v1", resolved.side(ScenarioSide.SINGLE).document().path("format").textValue());

        assertThrows(UnsupportedOperationException.class, () -> resolved.commonEffectiveParameters()
                .put("other", new EffectiveParameter(TextNode.valueOf("value"), ParameterSource.SUBMIT_OVERRIDE)));

        ObjectNode mutableEffectiveValue = NODES.objectNode().put("nested", "original");
        EffectiveParameter effectiveParameter = new EffectiveParameter(
                mutableEffectiveValue, ParameterSource.SCENARIO_DEFAULT);
        mutableEffectiveValue.put("nested", "mutated");
        ObjectNode returnedEffectiveValue = (ObjectNode) effectiveParameter.value();
        returnedEffectiveValue.put("nested", "also-mutated");
        assertEquals("original", effectiveParameter.value().path("nested").textValue());
    }

    private ScenarioSpecification experimentScenario() {
        return scenario(document -> {
            parameterWithDefault(document, "backend", "string", TextNode.valueOf("rocksdb"));
            job(document).put("state_backend", "${backend}");
            experiment(document, "backend", TextNode.valueOf("rocksdb"), TextNode.valueOf("hashmap"));
        });
    }

    private void assertResolvedPositiveIntegerViolation(
            String path, Consumer<ObjectNode> useParameter) {
        ScenarioSpecification scenario = scenario(document -> {
            parameterWithDefault(document, "count", "integer", IntNode.valueOf(0));
            useParameter.accept(document);
        });
        ScenarioResolutionException exception = assertThrows(ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));
        assertHasIssue(exception, "schema.one-of", ResolutionScope.SINGLE, path);
    }

    private ScenarioSpecification scenario(Consumer<ObjectNode> changes) {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        changes.accept(document);
        return loader.validateScenarioDocument(source, document);
    }

    private static ObjectNode parameters(ObjectNode document) {
        JsonNode existing = document.get("parameters");
        return existing instanceof ObjectNode object ? object : document.putObject("parameters");
    }

    private static ObjectNode parameterWithDefault(
            ObjectNode document, String name, String type, JsonNode value) {
        ObjectNode declaration = parameters(document).putObject(name);
        declaration.put("type", type);
        declaration.set("default", value);
        return declaration;
    }

    private static ObjectNode experiment(
            ObjectNode document,
            String varies,
            JsonNode baseline,
            JsonNode candidate) {
        ObjectNode experiment = document.putObject("experiment");
        experiment.put("claim", "The declared dimension is the only difference.");
        experiment.putArray("varies").add(varies);
        experiment.putObject("baseline").set(varies, baseline);
        experiment.putObject("candidate").set(varies, candidate);
        return experiment;
    }

    private static ObjectNode kafkaCluster(ObjectNode document) {
        return (ObjectNode) document.at("/setup/kafka/clusters/main");
    }

    private static ObjectNode inputTopic(ObjectNode document) {
        return (ObjectNode) document.at("/setup/kafka/clusters/main/topics/0");
    }

    private static ObjectNode flink(ObjectNode document) {
        return (ObjectNode) document.at("/setup/flink");
    }

    private static ObjectNode job(ObjectNode document) {
        return (ObjectNode) document.at("/workload/jobs/0");
    }

    private void assertEffective(
            ResolvedScenario scenario, String name, String value, ParameterSource source) {
        EffectiveParameter parameter = scenario.commonEffectiveParameters().get(name);
        assertEquals(value, parameter.value().textValue());
        assertEquals(source, parameter.source());
    }

    private static void assertHasIssue(
            ScenarioResolutionException exception,
            String code,
            ResolutionScope scope,
            String path) {
        assertTrue(exception.issues().stream().anyMatch(issue ->
                        issue.code().equals(code) && issue.scope() == scope && issue.path().equals(path)),
                () -> "Expected " + code + " [" + scope + "] at " + path
                        + " but got " + exception.issues());
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
