package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScenarioPreflightValidatorTest {
    private static final String ID_SET_REASON = "validator.kafka.id-set.missing-ids";
    private static final String TRANSACTION_REASON =
            "validator.kafka.transaction.ongoing-after-timeout";

    private final SpecificationLoader loader = new SpecificationLoader();
    private final ScenarioParameterResolver parameterResolver =
            new ScenarioParameterResolver(loader);
    private final ExpectedResultSelector expectationSelector =
            new ExpectedResultSelector(parameterResolver);
    private final ScenarioPreflightValidator validator = new ScenarioPreflightValidator();

    @Test
    void acceptsTheResolvedMinimalScenarioAndReturnsTheSamePlan() {
        ResolvedScenarioPlan plan = plan(scenario(document -> {}), expected(document -> {}));

        assertSame(plan, validator.validate(plan));
        assertEquals("pass", plan.expectationFor(ScenarioSide.SINGLE)
                .path("outcome").textValue());
    }

    @Test
    void exposesDeterministicallySortedImmutableDiagnostics() {
        ScenarioPreflightException exception = reject(document -> {
            topic(document, 0).put("replication_factor", 2);
            job(document).withArray("connectors").set(0, text("missing"));
        });

        Comparator<PreflightIssue> order = Comparator
                .comparing((PreflightIssue issue) -> issue.source().toString())
                .thenComparing(PreflightIssue::scope)
                .thenComparing(PreflightIssue::path)
                .thenComparing(PreflightIssue::code)
                .thenComparing(PreflightIssue::message);
        ArrayList<PreflightIssue> sorted = new ArrayList<>(exception.issues());
        sorted.sort(order);

        assertEquals(sorted, exception.issues());
        assertThrows(UnsupportedOperationException.class,
                () -> exception.issues().add(exception.issues().getFirst()));
    }

    @Test
    void rejectsReplicationFactorGreaterThanTheClusterBrokerCount() {
        ScenarioPreflightException exception = reject(
                document -> topic(document, 0).put("replication_factor", 2));

        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.kafka.replication-exceeds-brokers",
                "$/setup/kafka/clusters/main/topics/0/replication_factor");
    }

    @Test
    void rejectsDuplicateTopicNamesAndJobAliases() {
        ScenarioPreflightException duplicateTopic = reject(document ->
                topics(document).add(topic(document, 1).deepCopy()));
        assertHasIssue(
                duplicateTopic,
                ResolutionScope.SINGLE,
                "preflight.topic.duplicate-name",
                "$/setup/kafka/clusters/main/topics/2/name");

        ScenarioPreflightException duplicateJob = reject(document ->
                jobs(document).add(job(document).deepCopy()));
        assertHasIssue(
                duplicateJob,
                ResolutionScope.COMMON,
                "preflight.job.duplicate-alias",
                "$/workload/jobs/1/alias");
    }

    @Test
    void rejectsMissingSourceSinkConnectorAndProxyReferences() {
        ScenarioPreflightException sourceCluster = reject(document ->
                job(document).withObject("source").put("cluster", "missing"));
        assertHasIssue(
                sourceCluster,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-cluster-not-found",
                "$/workload/jobs/0/source/cluster");

        ScenarioPreflightException sourceTopic = reject(document ->
                job(document).withObject("source").put("topic", "missing"));
        assertHasIssue(
                sourceTopic,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-topic-not-found",
                "$/workload/jobs/0/source/topic");

        ScenarioPreflightException sinkCluster = reject(document ->
                job(document).withObject("sink").put("cluster", "missing"));
        assertHasIssue(
                sinkCluster,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-cluster-not-found",
                "$/workload/jobs/0/sink/cluster");

        ScenarioPreflightException sinkTopic = reject(document ->
                job(document).withObject("sink").put("topic", "missing"));
        assertHasIssue(
                sinkTopic,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-topic-not-found",
                "$/workload/jobs/0/sink/topic");

        ScenarioPreflightException connector = reject(document ->
                job(document).withArray("connectors").set(0, text("missing")));
        assertHasIssue(
                connector,
                ResolutionScope.SINGLE,
                "preflight.reference.connector-not-found",
                "$/workload/jobs/0/connectors/0");

        ScenarioPreflightException proxy = reject(document ->
                job(document).withObject("source").put("connect_via_proxy", "missing"));
        assertHasIssue(
                proxy,
                ResolutionScope.SINGLE,
                "preflight.reference.proxy-not-found",
                "$/workload/jobs/0/source/connect_via_proxy");

        ScenarioPreflightException inputProxy = reject(document ->
                topic(document, 0).withObject("input_source")
                        .put("connect_via_proxy", "missing"));
        assertHasIssue(
                inputProxy,
                ResolutionScope.SINGLE,
                "preflight.reference.proxy-not-found",
                "$/setup/kafka/clusters/main/topics/0/input_source/connect_via_proxy");
    }

    @Test
    void rejectsEveryDeclaredConnectorThatNoWorkloadJobReferences() {
        ScenarioPreflightException exception = reject(document -> {
            ObjectNode beta = document.withObject("subject")
                    .withObject("connectors")
                    .putObject("unused-beta");
            beta.put("artifact", "unused-beta.jar");
            beta.putArray("runtime_dependencies");
        });

        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.reference.connector-unreferenced",
                "$/subject/connectors/unused-beta");
    }

    @Test
    void validatesStatefulFlinkRestartImageInheritanceIncludingNestedLoops() {
        ScenarioPreflightException taskmanagerAmbiguous = reject(document -> {
            document.withObject("setup").withObject("flink").put("taskmanagers", 2);
            ArrayNode steps = ((ObjectNode) document.at("/phases/0")).putArray("steps");
            restart(steps, "taskmanager", "flink:2.2.1");
            restart(steps, "flink", null);
        });
        assertHasIssue(
                taskmanagerAmbiguous,
                ResolutionScope.SINGLE,
                "preflight.restart.taskmanager-image-ambiguous",
                "$/phases/0/steps/0/restart/image");
        assertTrue(taskmanagerAmbiguous.issues().stream().noneMatch(issue -> issue.code().equals(
                "preflight.restart.flink-image-inheritance-ambiguous")));

        ScenarioPreflightException divergent = reject(document -> {
            ArrayNode steps = ((ObjectNode) document.at("/phases/0")).putArray("steps");
            restart(steps, "jobmanager", "flink:2.2.1");
            restart(steps, "flink", null);
        });
        PreflightIssue divergence = assertHasIssue(
                divergent,
                ResolutionScope.SINGLE,
                "preflight.restart.flink-image-inheritance-ambiguous",
                "$/phases/0/steps/1/restart");
        assertTrue(divergence.message().contains("jobmanager-1=flink:2.2.1"));
        assertTrue(divergence.message().contains("taskmanagers(1)=flink:2.2.0"));
        assertTrue(divergence.message().contains("desired targets differ"));

        ScenarioPreflightException repeated = reject(document -> {
            ArrayNode steps = ((ObjectNode) document.at("/phases/0")).putArray("steps");
            ObjectNode loop = steps.addObject().putObject("loop");
            loop.put("times", 2);
            ArrayNode nested = loop.putArray("steps");
            restart(nested, "flink", null);
            restart(nested, "jobmanager", "flink:2.2.1");
        });
        assertHasIssue(
                repeated,
                ResolutionScope.SINGLE,
                "preflight.restart.flink-image-inheritance-ambiguous",
                "$/phases/0/steps/0/loop/steps/0/restart");

        ScenarioPreflightException taskmanagerRetained = reject(document -> {
            ArrayNode steps = ((ObjectNode) document.at("/phases/0")).putArray("steps");
            restart(steps, "taskmanager", "flink:2.2.1");
            restart(steps, "taskmanager", null);
            restart(steps, "flink", null);
        });
        assertHasIssue(
                taskmanagerRetained,
                ResolutionScope.SINGLE,
                "preflight.restart.flink-image-inheritance-ambiguous",
                "$/phases/0/steps/2/restart");

        ScenarioPreflightException jobmanagerRetained = reject(document -> {
            ArrayNode steps = ((ObjectNode) document.at("/phases/0")).putArray("steps");
            restart(steps, "jobmanager", "flink:2.2.1");
            restart(steps, "jobmanager", null);
            restart(steps, "flink", null);
        });
        assertHasIssue(
                jobmanagerRetained,
                ResolutionScope.SINGLE,
                "preflight.restart.flink-image-inheritance-ambiguous",
                "$/phases/0/steps/2/restart");

        assertDoesNotThrow(() -> validator.validate(plan(
                scenario(document -> {
                    ArrayNode steps = ((ObjectNode) document.at("/phases/0"))
                            .putArray("steps");
                    restart(steps, "jobmanager", "flink:2.2.1");
                    restart(steps, "flink", "flink:2.2.2");
                    restart(steps, "flink", null);
                    restart(steps, "taskmanager", "flink:2.2.1");
                }),
                expected(document -> {}))));
    }

    @Test
    void rejectsProxyRoutesAndManagedBootstrapAcrossKafkaClusters() {
        ScenarioPreflightException endpointMismatch = reject(document -> {
            addKafkaCluster(document, "other");
            addManagedProxy(document, "other-proxy", "other", "other");
            job(document).withObject("sink")
                    .put("connect_via_proxy", "other-proxy");
        });
        assertHasIssue(
                endpointMismatch,
                ResolutionScope.SINGLE,
                "preflight.proxy.endpoint-cluster-mismatch",
                "$/workload/jobs/0/sink/connect_via_proxy");

        ScenarioPreflightException bootstrapMismatch = reject(document -> {
            addKafkaCluster(document, "other");
            addManagedProxy(document, "main-proxy", "main", "other");
        });
        assertHasIssue(
                bootstrapMismatch,
                ResolutionScope.SINGLE,
                "preflight.proxy.bootstrap-cluster-mismatch",
                "$/setup/proxies/main-proxy/bootstrap/cluster");
    }

    @Test
    void rejectsAmbiguousOrIncompatibleInputContracts() {
        ScenarioPreflightException multipleSources = reject(document ->
                topic(document, 1).set(
                        "input_source",
                        topic(document, 0).get("input_source").deepCopy()));
        assertHasIssue(
                multipleSources,
                ResolutionScope.SINGLE,
                "preflight.input.multiple-sources",
                "$/setup/kafka/clusters/main/topics/0/input_source");
        assertHasIssue(
                multipleSources,
                ResolutionScope.SINGLE,
                "preflight.input.multiple-sources",
                "$/setup/kafka/clusters/main/topics/1/input_source");
        assertTrue(multipleSources.issues().stream()
                .noneMatch(issue -> issue.code().equals("preflight.input.manifest-not-available")));

        ScenarioPreflightException generatedWithoutIdSet = reject(document -> {
            ArrayNode terminal = document.putArray("terminal_validations");
            addRecordCountValidator(terminal, "main", "output");
        });
        assertHasIssue(
                generatedWithoutIdSet,
                ResolutionScope.SINGLE,
                "preflight.input.built-in-validator-required",
                "$/setup/kafka/clusters/main/topics/0/input_source");

        ScenarioPreflightException customWithBuiltIn = reject(document -> {
            ObjectNode input = topic(document, 0).withObject("input_source");
            input.removeAll();
            input.put("mode", "custom");
            input.put("artifact", "./custom-input.jar");
            input.put("timeout", "30s");
        });
        assertHasIssue(
                customWithBuiltIn,
                ResolutionScope.SINGLE,
                "preflight.input.custom-validator-required",
                "$/setup/kafka/clusters/main/topics/0/input_source");
        assertHasIssue(
                customWithBuiltIn,
                ResolutionScope.SINGLE,
                "preflight.input.custom-built-in-conflict",
                "$/terminal_validations/0");
    }

    @Test
    void rejectsMissingInputManifestAndValidatorLineageMismatch() {
        ScenarioPreflightException missingManifest = reject(document ->
                topic(document, 0).remove("input_source"));
        assertHasIssue(
                missingManifest,
                ResolutionScope.SINGLE,
                "preflight.input.manifest-not-available",
                "$/terminal_validations/0/expected");

        ScenarioPreflightException detachedOutput = reject(document -> {
            topics(document).addObject()
                    .put("name", "detached-output")
                    .put("partitions", 1)
                    .put("replication_factor", 1);
            terminal(document, 0).put("topic", "detached-output");
        });
        assertHasIssue(
                detachedOutput,
                ResolutionScope.SINGLE,
                "preflight.input.validator-source-mismatch",
                "$/terminal_validations/0");

        ScenarioPreflightException sharedOutput = reject(document -> {
            topics(document).addObject()
                    .put("name", "other-input")
                    .put("partitions", 1)
                    .put("replication_factor", 1);
            ObjectNode secondJob = job(document).deepCopy();
            secondJob.put("alias", "second-job");
            secondJob.withObject("source").put("topic", "other-input");
            jobs(document).add(secondJob);
        });
        PreflightIssue sharedOutputIssue = assertHasIssue(
                sharedOutput,
                ResolutionScope.SINGLE,
                "preflight.input.validator-source-mismatch",
                "$/terminal_validations/0");
        assertTrue(sharedOutputIssue.message().contains("[eos-job, second-job]"));
    }

    @Test
    void rejectsBrokenTerminalValidatorReferences() {
        ScenarioPreflightException topic = reject(document ->
                addRecordCountValidator(
                        (ArrayNode) document.get("terminal_validations"),
                        "main",
                        "missing"));
        assertHasIssue(
                topic,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-topic-not-found",
                "$/terminal_validations/1/topic");

        ScenarioPreflightException transactionCluster = reject(document ->
                addTransactionValidator(
                        (ArrayNode) document.get("terminal_validations"),
                        "missing",
                        "minimal"));
        assertHasIssue(
                transactionCluster,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-cluster-not-found",
                "$/terminal_validations/1/cluster");

        ScenarioPreflightException transactionSink = reject(document ->
                addTransactionValidator(
                        (ArrayNode) document.get("terminal_validations"),
                        "main",
                        "other-prefix"));
        assertHasIssue(
                transactionSink,
                ResolutionScope.SINGLE,
                "preflight.validator.transactional-sink-not-found",
                "$/terminal_validations/1/transactional_id_prefix");

        ScenarioPreflightException job = reject(document ->
                addLogValidator(
                        (ArrayNode) document.get("terminal_validations"),
                        "missing"));
        assertHasIssue(
                job,
                ResolutionScope.COMMON,
                "preflight.reference.job-not-found",
                "$/terminal_validations/1/job");
    }

    @Test
    void rejectsDuplicateTerminalValidatorTypes() {
        ScenarioPreflightException exception = reject(document ->
                ((ArrayNode) document.get("terminal_validations"))
                        .add(terminal(document, 0).deepCopy()));

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.terminal.duplicate-type",
                "$/terminal_validations/1/type");
    }

    @Test
    void rejectsExpectedFailureOracleDeclaredOnlyAsAnInlineValidation() {
        ScenarioPreflightException exception = reject(
                document -> {
                    ObjectNode inline = ((ArrayNode) document.at("/phases/0/steps"))
                            .addObject().putObject("validate");
                    inline.put("type", "flink.log-match");
                    inline.put("job", "eos-job");
                    inline.put("pattern", "ERROR");
                },
                document -> setPlainFailure(
                        document,
                        "flink.log-match",
                        "validator.flink.log-match.pattern-found"));

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.expectation.oracle-not-terminal",
                "$/default/oracle");
    }

    @Test
    void rejectsUnknownAndOracleMismatchedFailureReasons() {
        ScenarioPreflightException unknown = reject(
                document -> {},
                document -> setPlainFailure(
                        document,
                        "kafka.id-set",
                        "validator.kafka.id-set.unknown"));
        assertHasIssue(
                unknown,
                ResolutionScope.COMMON,
                "preflight.expectation.reason-not-declared",
                "$/default/reason");

        ScenarioPreflightException mismatched = reject(
                document -> addTransactionValidator(
                        (ArrayNode) document.get("terminal_validations"),
                        "main",
                        "minimal"),
                document -> setPlainFailure(document, "kafka.id-set", TRANSACTION_REASON));
        assertHasIssue(
                mismatched,
                ResolutionScope.COMMON,
                "preflight.expectation.reason-not-declared",
                "$/default/reason");
    }

    @Test
    void acceptsBothBuiltInOracleAndRegisteredReasonPairs() {
        ResolvedScenarioPlan idSet = plan(
                scenario(document -> {}),
                expected(document -> setPlainFailure(document, "kafka.id-set", ID_SET_REASON)));
        assertDoesNotThrow(() -> validator.validate(idSet));

        ResolvedScenarioPlan transactions = plan(
                scenario(document -> addTransactionValidator(
                        (ArrayNode) document.get("terminal_validations"),
                        "main",
                        "minimal")),
                expected(document -> setPlainFailure(
                        document,
                        "kafka.no-hanging-transactions",
                        TRANSACTION_REASON)));
        assertDoesNotThrow(() -> validator.validate(transactions));
    }

    @Test
    void acceptsDeclaredCustomFailureReasonAndRejectsAnUndeclaredOne() {
        Consumer<ObjectNode> customValidator = document -> {
            ObjectNode custom = ((ArrayNode) document.get("terminal_validations")).addObject();
            custom.put("type", "custom");
            custom.put("artifact", "./custom-validator.jar");
            custom.put("timeout", "30s");
            custom.putArray("failure_reasons").add("validator.custom.output-mismatch");
        };

        ResolvedScenarioPlan declared = plan(
                scenario(customValidator),
                expected(document -> setPlainFailure(
                        document,
                        "custom",
                        "validator.custom.output-mismatch")));
        assertDoesNotThrow(() -> validator.validate(declared));

        ScenarioPreflightException undeclared = reject(
                customValidator,
                document -> setPlainFailure(
                        document,
                        "custom",
                        "validator.custom.other-failure"));
        assertHasIssue(
                undeclared,
                ResolutionScope.COMMON,
                "preflight.expectation.reason-not-declared",
                "$/default/reason");
    }

    @Test
    void validatesFailureContractsInDormantExpectedResultCases() {
        ScenarioSpecification scenario = scenario(document -> {
            ObjectNode declaration = document.putObject("parameters").putObject("profile");
            declaration.put("type", "string");
            declaration.put("default", "active");
        });
        ExpectedResultSpecification expected = expected(document -> {
            ObjectNode dormant = document.putArray("cases").addObject();
            dormant.putObject("when").put("profile", "dormant");
            dormant.put("outcome", "fail");
            dormant.put("oracle", "kafka.id-set");
            dormant.put("reason", "validator.kafka.id-set.unknown");
        });
        ResolvedScenarioPlan plan = plan(scenario, expected);
        assertEquals(ExpectationSelectionKind.DEFAULT, plan.selectedExpectation().kind());

        ScenarioPreflightException exception = assertThrows(
                ScenarioPreflightException.class,
                () -> validator.validate(plan));

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.expectation.reason-not-declared",
                "$/cases/0/reason");
    }

    @Test
    void reportsExperimentTopologyFailuresAgainstOnlyTheAffectedSide() {
        ScenarioSpecification experiment = scenario(document -> {
            ObjectNode brokers = document.putObject("parameters").putObject("brokers");
            brokers.put("type", "integer");
            brokers.put("default", 2);
            brokers.put("min", 1);
            brokers.put("max", 2);
            ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                    .put("brokers", "${brokers}");
            topic(document, 0).put("replication_factor", 2);
            topic(document, 1).put("replication_factor", 2);

            ObjectNode definition = document.putObject("experiment");
            definition.put("claim", "Both sides preserve the Kafka result.");
            definition.putArray("varies").add("brokers");
            definition.putObject("baseline").put("brokers", 2);
            definition.putObject("candidate").put("brokers", 1);
        });
        ExpectedResultSpecification expectation = expected(document -> {
            ObjectNode defaultNode = document.withObject("default");
            defaultNode.removeAll();
            defaultNode.putObject("baseline").put("outcome", "pass");
            defaultNode.putObject("candidate").put("outcome", "pass");
        });

        ScenarioPreflightException exception = assertThrows(
                ScenarioPreflightException.class,
                () -> validator.validate(plan(experiment, expectation)));

        assertHasIssue(
                exception,
                ResolutionScope.CANDIDATE,
                "preflight.kafka.replication-exceeds-brokers",
                "$/setup/kafka/clusters/main/topics/0/replication_factor");
        assertTrue(exception.issues().stream()
                .filter(issue -> issue.code().equals(
                        "preflight.kafka.replication-exceeds-brokers"))
                .allMatch(issue -> issue.scope() == ResolutionScope.CANDIDATE));
    }

    @Test
    void rejectsDifferentHealthRetryBudgetsAcrossExperimentSides() {
        ScenarioSpecification experiment = scenario(document -> {
            ObjectNode retryLimit = document.putObject("parameters")
                    .putObject("retry_limit");
            retryLimit.put("type", "integer");
            retryLimit.put("default", 1);
            retryLimit.put("min", 0);
            retryLimit.put("max", 2);
            document.put("health_retry_limit", "${retry_limit}");

            ObjectNode definition = document.putObject("experiment");
            definition.put("claim", "Both sides use one invocation health budget.");
            definition.putArray("varies").add("retry_limit");
            definition.putObject("baseline").put("retry_limit", 1);
            definition.putObject("candidate").put("retry_limit", 2);
        });
        ExpectedResultSpecification expectation = expected(document -> {
            ObjectNode defaultNode = document.withObject("default");
            defaultNode.removeAll();
            defaultNode.putObject("baseline").put("outcome", "pass");
            defaultNode.putObject("candidate").put("outcome", "pass");
        });

        ScenarioPreflightException exception = assertThrows(
                ScenarioPreflightException.class,
                () -> validator.validate(plan(experiment, expectation)));

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.invocation.health-retry-limit-side-mismatch",
                "$/health_retry_limit");
    }

    private ScenarioPreflightException reject(Consumer<ObjectNode> scenarioChanges) {
        return reject(scenarioChanges, document -> {});
    }

    private ScenarioPreflightException reject(
            Consumer<ObjectNode> scenarioChanges,
            Consumer<ObjectNode> expectedChanges) {
        ResolvedScenarioPlan plan = plan(scenario(scenarioChanges), expected(expectedChanges));
        return assertThrows(ScenarioPreflightException.class, () -> validator.validate(plan));
    }

    private ResolvedScenarioPlan plan(
            ScenarioSpecification scenario,
            ExpectedResultSpecification expected) {
        ScenarioBundle bundle = new ScenarioBundle(scenario, expected);
        ResolvedScenario resolved = parameterResolver.resolve(scenario, ResolutionRequest.none());
        return expectationSelector.select(bundle, resolved);
    }

    private ScenarioSpecification scenario(Consumer<ObjectNode> changes) {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        changes.accept(document);
        return loader.validateScenarioDocument(source, document);
    }

    private ExpectedResultSpecification expected(Consumer<ObjectNode> changes) {
        Path source = resource("minimal.expected.yaml");
        ObjectNode document = loader.loadExpectedResult(source).document();
        changes.accept(document);
        return loader.validateExpectedResultDocument(source, document);
    }

    private static void setPlainFailure(ObjectNode document, String oracle, String reason) {
        ObjectNode defaultNode = document.withObject("default");
        defaultNode.removeAll();
        defaultNode.put("outcome", "fail");
        defaultNode.put("oracle", oracle);
        defaultNode.put("reason", reason);
    }

    private static void addKafkaCluster(ObjectNode document, String name) {
        ObjectNode main = (ObjectNode) document.at("/setup/kafka/clusters/main");
        ObjectNode cluster = main.deepCopy();
        ArrayNode clusterTopics = (ArrayNode) cluster.get("topics");
        clusterTopics.forEach(topic -> ((ObjectNode) topic).remove("input_source"));
        ((ObjectNode) document.at("/setup/kafka/clusters")).set(name, cluster);
    }

    private static void addManagedProxy(
            ObjectNode document,
            String alias,
            String cluster,
            String bootstrapCluster) {
        ObjectNode proxy = document.withObject("setup").withObject("proxies").putObject(alias);
        proxy.put("type", "kroxylicious");
        proxy.put("cluster", cluster);
        proxy.put("listen", "127.0.0.1:19092");
        proxy.putObject("bootstrap").put("cluster", bootstrapCluster);
    }

    private static void addRecordCountValidator(
            ArrayNode validators, String cluster, String topic) {
        ObjectNode terminal = validators.addObject();
        terminal.put("type", "kafka.record-count");
        terminal.put("cluster", cluster);
        terminal.put("topic", topic);
        terminal.put("expected_records", 10);
    }

    private static void addTransactionValidator(
            ArrayNode validators, String cluster, String prefix) {
        ObjectNode terminal = validators.addObject();
        terminal.put("type", "kafka.no-hanging-transactions");
        terminal.put("cluster", cluster);
        terminal.put("transactional_id_prefix", prefix);
        terminal.put("stabilization_timeout", "30s");
    }

    private static void addLogValidator(ArrayNode validators, String job) {
        ObjectNode terminal = validators.addObject();
        terminal.put("type", "flink.log-match");
        terminal.put("job", job);
        terminal.put("pattern", "ERROR");
    }

    private static JsonNode text(String value) {
        return com.fasterxml.jackson.databind.node.TextNode.valueOf(value);
    }

    private static void restart(ArrayNode steps, String component, String image) {
        ObjectNode restart = steps.addObject().putObject("restart");
        restart.put("component", component);
        if (image != null) {
            restart.put("image", image);
        }
    }

    private static ArrayNode topics(ObjectNode document) {
        return (ArrayNode) document.at("/setup/kafka/clusters/main/topics");
    }

    private static ObjectNode topic(ObjectNode document, int index) {
        return (ObjectNode) document.at("/setup/kafka/clusters/main/topics/" + index);
    }

    private static ArrayNode jobs(ObjectNode document) {
        return (ArrayNode) document.at("/workload/jobs");
    }

    private static ObjectNode job(ObjectNode document) {
        return (ObjectNode) document.at("/workload/jobs/0");
    }

    private static ObjectNode terminal(ObjectNode document, int index) {
        return (ObjectNode) document.at("/terminal_validations/" + index);
    }

    private static PreflightIssue assertHasIssue(
            ScenarioPreflightException exception,
            ResolutionScope scope,
            String code,
            String path) {
        return exception.issues().stream()
                .filter(issue -> issue.scope() == scope
                        && issue.code().equals(code)
                        && issue.path().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Expected " + scope + ":" + code + " at " + path
                                + " but got " + exception.issues()));
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
