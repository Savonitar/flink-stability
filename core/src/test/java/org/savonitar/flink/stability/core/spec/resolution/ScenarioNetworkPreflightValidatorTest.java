package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.savonitar.flink.stability.core.spec.document.SpecificationAssertions.assertFailsAt;

class ScenarioNetworkPreflightValidatorTest {
    private static final String NETWORK_PATH = "$/phases/0/steps/0/network_fault";
    private static final Set<String> TRANSACTION_APIS = Set.of(
            "init-producer-id",
            "add-partitions-to-txn",
            "add-offsets-to-txn",
            "txn-offset-commit",
            "end-txn");

    private final SpecificationLoader loader = new SpecificationLoader();
    private final ScenarioParameterResolver parameterResolver =
            new ScenarioParameterResolver(loader);
    private final ExpectedResultSelector expectationSelector =
            new ExpectedResultSelector(parameterResolver);
    private final ScenarioPreflightValidator validator = new ScenarioPreflightValidator();

    @Test
    void rejectsDuplicateResolvedProxyListenAddresses() {
        SpecificationException exception = reject(document -> {
            addManagedProxy(document, "first-proxy", "main", "127.0.0.1:19092");
            addManagedProxy(document, "second-proxy", "main", "127.0.0.1:19092");
        });

        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.proxy.duplicate-listen",
                "$/setup/proxies/second-proxy/listen");
    }

    @Test
    void comparesProxyListenerHostsUsingDnsIdentity() {
        SpecificationException exception = reject(document -> {
            addManagedProxy(document, "first-proxy", "main", "KAFKA-PROXY.:19092");
            addManagedProxy(document, "second-proxy", "main", "kafka-proxy:19092");
        });

        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.proxy.duplicate-listen",
                "$/setup/proxies/second-proxy/listen");
    }

    @Test
    void rejectsAnExplicitBootstrapThatCyclesToTheProxyListenAddress() {
        SpecificationException exception = reject(document ->
                addAddressProxy(
                        document,
                        "kafka-proxy",
                        "main",
                        "127.0.0.1:19092",
                        "127.0.0.1:19092"));

        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.proxy.bootstrap-listen-cycle",
                "$/setup/proxies/kafka-proxy/bootstrap/address");
    }

    @Test
    void comparesBootstrapAndListenerHostsUsingDnsIdentity() {
        SpecificationException exception = reject(document ->
                addAddressProxy(
                        document,
                        "kafka-proxy",
                        "main",
                        "KAFKA-PROXY.:19092",
                        "kafka-proxy:19092"));

        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.proxy.bootstrap-listen-cycle",
                "$/setup/proxies/kafka-proxy/bootstrap/address");
    }

    @Test
    void acceptsAnExplicitBootstrapAsALogicalRouteForItsDeclaredCluster() {
        assertAccepts(document -> {
            addAddressProxy(
                    document,
                    "kafka-proxy",
                    "main",
                    "127.0.0.1:19092",
                    "broker:9092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "produce",
                    "output",
                    null);
        });
    }

    @Test
    void rejectsMissingProxyBeforeEndpointMatching() {
        SpecificationException exception = reject(document -> addNetworkFault(
                replaceSteps(document),
                "missing-proxy",
                "main",
                null,
                "produce",
                "output",
                null));

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.reference.proxy-not-found",
                NETWORK_PATH + "/proxy");
        assertEquals(1, exception.diagnostics().stream()
                .filter(issue -> issue.path().startsWith(NETWORK_PATH))
                .count());
    }

    @Test
    void rejectsMissingOrMismatchedTargetClustersBeforeEndpointMatching() {
        SpecificationException missing = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "missing",
                    null,
                    "produce",
                    null,
                    null);
        });
        assertHasIssue(
                missing,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-cluster-not-found",
                NETWORK_PATH + "/target/cluster");

        SpecificationException mismatched = reject(document -> {
            addKafkaCluster(document, "other");
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "other",
                    null,
                    "produce",
                    "output",
                    null);
        });
        assertHasIssue(
                mismatched,
                ResolutionScope.SINGLE,
                "preflight.network.proxy-target-cluster-mismatch",
                NETWORK_PATH + "/target/cluster");
    }

    @Test
    void validatesTheOptionalBrokerAgainstTheTargetClusterInventory() {
        assertAccepts(document -> {
            kafkaCluster(document, "main").put("brokers", 2);
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    "broker-2",
                    "produce",
                    "output",
                    null);
        });

        SpecificationException exception = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    "broker-2",
                    "produce",
                    "output",
                    null);
        });
        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.network.broker-not-found",
                NETWORK_PATH + "/target/broker");
    }

    @Test
    void matchesProduceSinkAndInputFetchSourceAndMetadataEndpoints() {
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "produce",
                    "output",
                    null);
        });
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            inputSource(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "produce",
                    "input",
                    null);
        });
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            source(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "fetch",
                    "input",
                    null);
        });
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            source(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "metadata",
                    null,
                    null);
        });
    }

    @Test
    void distinguishesDirectWrongProxyMissingSelectorAndUnusedProxyRoutes() {
        SpecificationException direct = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "produce",
                    "output",
                    null);
        });
        assertHasIssue(
                direct,
                ResolutionScope.SINGLE,
                "preflight.network.endpoint-bypasses-proxy",
                NETWORK_PATH + "/match");

        SpecificationException wrongProxy = reject(document -> {
            addManagedProxy(document, "fault-proxy", "main", "127.0.0.1:19092");
            addManagedProxy(document, "other-proxy", "main", "127.0.0.1:29092");
            sink(document).put("connect_via_proxy", "other-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "fault-proxy",
                    "main",
                    null,
                    "produce",
                    "output",
                    null);
        });
        assertHasIssue(
                wrongProxy,
                ResolutionScope.SINGLE,
                "preflight.network.endpoint-bypasses-proxy",
                NETWORK_PATH + "/match");

        SpecificationException missingSelector = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            source(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "fetch",
                    "output",
                    null);
        });
        assertHasIssue(
                missingSelector,
                ResolutionScope.SINGLE,
                "preflight.network.endpoint-not-found",
                NETWORK_PATH + "/match");

        SpecificationException unusedProxy = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "fetch",
                    "output",
                    null);
        });
        assertHasIssue(
                unusedProxy,
                ResolutionScope.SINGLE,
                "preflight.network.proxy-has-no-routed-endpoint",
                NETWORK_PATH + "/proxy");
    }

    @ParameterizedTest(name = "matches routed exactly-once sink for {0}")
    @MethodSource("transactionApis")
    void matchesEveryTransactionApiAgainstARoutedExactlyOnceSink(String api) {
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    api,
                    null,
                    "minimal");
        });
    }

    @ParameterizedTest(name = "rejects topic selector for {0}")
    @MethodSource("topiclessTransactionApis")
    void rejectsTopicsForTransactionApisWithoutSafelyMatchableTopicFields(String api) {
        SpecificationException exception = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    api,
                    "output",
                    "minimal");
        });

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.network.topic-selector-unsupported",
                NETWORK_PATH + "/match/topic");
    }

    @Test
    void bindsAddPartitionsToSinkTopicAndTxnOffsetCommitToSourceTopic() {
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "add-partitions-to-txn",
                    "output",
                    "minimal");
        });
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "txn-offset-commit",
                    "input",
                    "minimal");
        });

        SpecificationException wrongSinkTopic = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "add-partitions-to-txn",
                    "input",
                    "minimal");
        });
        assertHasIssue(
                wrongSinkTopic,
                ResolutionScope.SINGLE,
                "preflight.network.endpoint-not-found",
                NETWORK_PATH + "/match");

        SpecificationException wrongSourceTopic = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "txn-offset-commit",
                    "output",
                    "minimal");
        });
        assertHasIssue(
                wrongSourceTopic,
                ResolutionScope.SINGLE,
                "preflight.network.endpoint-not-found",
                NETWORK_PATH + "/match");
    }

    @Test
    void restrictsTransactionPrefixesAndTransactionApisToExactlyOnceSinks() {
        SpecificationException prefixOnProduce = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "produce",
                    "output",
                    "minimal");
        });
        assertHasIssue(
                prefixOnProduce,
                ResolutionScope.COMMON,
                "preflight.network.transactional-prefix-unsupported",
                NETWORK_PATH + "/match/transactional_id_prefix");

        SpecificationException wrongPrefix = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "end-txn",
                    null,
                    "other-prefix");
        });
        assertHasIssue(
                wrongPrefix,
                ResolutionScope.SINGLE,
                "preflight.network.endpoint-not-found",
                NETWORK_PATH + "/match");

        SpecificationException nonTransactionalSink = reject(document -> {
            ObjectNode sink = sink(document);
            sink.put("delivery_guarantee", "AT_LEAST_ONCE");
            sink.remove("transactional_id_prefix");
            sink.remove("transaction_id_naming_strategy");
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink.put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    "end-txn",
                    null,
                    null);
        });
        assertHasIssue(
                nonTransactionalSink,
                ResolutionScope.SINGLE,
                "preflight.network.endpoint-not-found",
                NETWORK_PATH + "/match");
    }

    @ParameterizedTest(name = "accepts safe error {1} for {0}")
    @MethodSource("allowedErrorPairs")
    void acceptsEveryRegisteredSafeErrorResponsePair(String api, String error) {
        assertAccepts(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            routeForApi(document, api, "kafka-proxy");
            ObjectNode networkFault = addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    api,
                    null,
                    null);
            setErrorResponse(networkFault, error);
        });
    }

    @ParameterizedTest(name = "rejects unsafe error {1} for {0}")
    @MethodSource("unsafeErrorPairs")
    void rejectsUnregisteredErrorResponsePairs(String api, String error) {
        SpecificationException exception = reject(document -> {
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            routeForApi(document, api, "kafka-proxy");
            ObjectNode networkFault = addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    null,
                    api,
                    null,
                    null);
            setErrorResponse(networkFault, error);
        });

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.network.error-response-unsupported",
                NETWORK_PATH + "/fault/error");
    }

    @Test
    void validatesNetworkFaultsAtTheirExactNestedLoopPath() {
        SpecificationException exception = reject(document -> {
            ObjectNode loop = replaceSteps(document).addObject().putObject("loop");
            loop.put("times", 1);
            addNetworkFault(
                    loop.putArray("steps"),
                    "missing-proxy",
                    "main",
                    null,
                    "produce",
                    "output",
                    null);
        });

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.reference.proxy-not-found",
                "$/phases/0/steps/0/loop/steps/0/network_fault/proxy");
    }

    @Test
    void keepsTheSchemaApiEnumInParityWithTheRuntimeRegistry() throws IOException {
        try (InputStream stream = getClass().getResourceAsStream(
                "/schema/scenario-v1.schema.json")) {
            assertNotNull(stream);
            JsonNode schema = new ObjectMapper().readTree(stream);
            TreeSet<String> schemaApis = new TreeSet<>();
            schema.at("/$defs/networkMatch/properties/api/enum")
                    .forEach(value -> schemaApis.add(value.textValue()));

            assertEquals(new TreeSet<>(KafkaFaultApiRegistry.apis()), schemaApis);
        }
    }

    @Test
    void reportsParameterizedBrokerInventoryFailureOnlyForTheCandidateSide() {
        SpecificationException exception = reject(document -> {
            addBrokerCountParameter(document, 2);
            ObjectNode experiment = document.putObject("experiment");
            experiment.put("claim", "Both broker topologies reach the declared network target.");
            experiment.putArray("varies").add("broker_count");
            experiment.putObject("baseline").put("broker_count", 2);
            experiment.putObject("candidate").put("broker_count", 1);

            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    "broker-2",
                    "produce",
                    "output",
                    null);
        });

        assertHasIssue(
                exception,
                ResolutionScope.CANDIDATE,
                "preflight.network.broker-not-found",
                NETWORK_PATH + "/target/broker");
        assertTrue(exception.diagnostics().stream()
                .filter(issue -> issue.code().equals("preflight.network.broker-not-found"))
                .allMatch(issue -> issue.scope() == ResolutionScope.CANDIDATE));
    }

    @Test
    void rejectsADormantExpectedCaseWhoseParametersBreakTheNetworkTarget() {
        ScenarioSpecification scenario = scenario(document -> {
            addBrokerCountParameter(document, 2);
            addManagedProxy(document, "kafka-proxy", "main", "127.0.0.1:19092");
            sink(document).put("connect_via_proxy", "kafka-proxy");
            addNetworkFault(
                    replaceSteps(document),
                    "kafka-proxy",
                    "main",
                    "broker-2",
                    "produce",
                    "output",
                    null);
        });
        ExpectedResultSpecification expected = expected(document -> {
            ObjectNode dormant = document.putArray("cases").addObject();
            dormant.putObject("when").put("broker_count", 1);
            dormant.put("outcome", "fail");
            dormant.put("oracle", "kafka.id-set");
            dormant.put("reason", "validator.kafka.id-set.missing-ids");
        });
        ResolvedScenario resolved = parameterResolver.resolve(
                scenario, ResolutionRequest.none());

        SpecificationException exception = assertFailsAt(
                Stage.EXPECTATION,
                () -> expectationSelector.select(new ScenarioBundle(scenario, expected), resolved));

        Diagnostic issue = exception.diagnostics().stream()
                .filter(candidate -> candidate.code().equals("expectation.case-value-invalid")
                        && candidate.path().equals("$/cases/0/when"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Expected dormant network preflight issue but got " + exception.diagnostics()));
        assertTrue(issue.message().contains("preflight.network.broker-not-found"));
    }

    private static Stream<String> transactionApis() {
        return TRANSACTION_APIS.stream().sorted();
    }

    private static Stream<String> topiclessTransactionApis() {
        return Stream.of("init-producer-id", "add-offsets-to-txn", "end-txn");
    }

    private static Stream<Arguments> allowedErrorPairs() {
        return KafkaFaultApiRegistry.apis().stream()
                .sorted()
                .flatMap(api -> KafkaFaultApiRegistry.profile(api).allowedErrors().stream()
                        .sorted()
                        .map(error -> Arguments.of(api, error)));
    }

    private static Stream<Arguments> unsafeErrorPairs() {
        return Stream.of(
                Arguments.of("metadata", "request-timed-out"),
                Arguments.of("produce", "coordinator-not-available"),
                Arguments.of("fetch", "coordinator-not-available"),
                Arguments.of("init-producer-id", "not-leader-or-follower"),
                Arguments.of("end-txn", "not-leader-or-follower"),
                Arguments.of("produce", "unknown-error"));
    }

    private SpecificationException reject(Consumer<ObjectNode> changes) {
        ResolvedScenario scenario = resolve(changes);
        return assertFailsAt(
                Stage.PREFLIGHT,
                () -> validator.validateScenario(scenario));
    }

    private void assertAccepts(Consumer<ObjectNode> changes) {
        assertDoesNotThrow(() -> validator.validateScenario(resolve(changes)));
    }

    private ResolvedScenario resolve(Consumer<ObjectNode> changes) {
        ScenarioSpecification scenario = scenario(changes);
        return parameterResolver.resolve(scenario, ResolutionRequest.none());
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

    private static ObjectNode addNetworkFault(
            ArrayNode steps,
            String proxy,
            String cluster,
            String broker,
            String api,
            String topic,
            String transactionalIdPrefix) {
        ObjectNode networkFault = steps.addObject().putObject("network_fault");
        networkFault.put("proxy", proxy);
        ObjectNode target = networkFault.putObject("target");
        target.put("cluster", cluster);
        if (broker != null) {
            target.put("broker", broker);
        }
        ObjectNode match = networkFault.putObject("match");
        match.put("api", api);
        if (topic != null) {
            match.put("topic", topic);
        }
        if (transactionalIdPrefix != null) {
            match.put("transactional_id_prefix", transactionalIdPrefix);
        }
        networkFault.putObject("fault").put("type", "disconnect");
        networkFault.put("duration", "1s");
        networkFault.put("heal", "restore-proxy-rule");
        return networkFault;
    }

    private static void setErrorResponse(ObjectNode networkFault, String error) {
        ObjectNode fault = networkFault.withObject("fault");
        fault.removeAll();
        fault.put("type", "error-response");
        fault.put("error", error);
    }

    private static void addManagedProxy(
            ObjectNode document,
            String alias,
            String cluster,
            String listen) {
        ObjectNode proxy = document.withObject("setup").withObject("proxies").putObject(alias);
        proxy.put("type", "kroxylicious");
        proxy.put("cluster", cluster);
        proxy.put("listen", listen);
        proxy.putObject("bootstrap").put("cluster", cluster);
    }

    private static void addAddressProxy(
            ObjectNode document,
            String alias,
            String cluster,
            String listen,
            String bootstrapAddress) {
        ObjectNode proxy = document.withObject("setup").withObject("proxies").putObject(alias);
        proxy.put("type", "kroxylicious");
        proxy.put("cluster", cluster);
        proxy.put("listen", listen);
        proxy.putObject("bootstrap").put("address", bootstrapAddress);
    }

    private static void routeForApi(ObjectNode document, String api, String proxy) {
        if (api.equals("fetch")) {
            source(document).put("connect_via_proxy", proxy);
        } else {
            sink(document).put("connect_via_proxy", proxy);
        }
    }

    private static void addKafkaCluster(ObjectNode document, String name) {
        ObjectNode cluster = kafkaCluster(document, "main").deepCopy();
        ((ArrayNode) cluster.get("topics"))
                .forEach(topic -> ((ObjectNode) topic).remove("input_source"));
        ((ObjectNode) document.at("/setup/kafka/clusters")).set(name, cluster);
    }

    private static void addBrokerCountParameter(ObjectNode document, int defaultValue) {
        ObjectNode declaration = document.putObject("parameters").putObject("broker_count");
        declaration.put("type", "integer");
        declaration.put("default", defaultValue);
        declaration.put("min", 1);
        declaration.put("max", 2);
        kafkaCluster(document, "main").put("brokers", "${broker_count}");
    }

    private static ArrayNode replaceSteps(ObjectNode document) {
        return ((ObjectNode) document.at("/phases/0")).putArray("steps");
    }

    private static ObjectNode kafkaCluster(ObjectNode document, String name) {
        return (ObjectNode) document.at("/setup/kafka/clusters/" + name);
    }

    private static ObjectNode source(ObjectNode document) {
        return (ObjectNode) document.at("/workload/jobs/0/source");
    }

    private static ObjectNode sink(ObjectNode document) {
        return (ObjectNode) document.at("/workload/jobs/0/sink");
    }

    private static ObjectNode inputSource(ObjectNode document) {
        return (ObjectNode) document.at(
                "/setup/kafka/clusters/main/topics/0/input_source");
    }

    private static Diagnostic assertHasIssue(
            SpecificationException exception,
            ResolutionScope scope,
            String code,
            String path) {
        return exception.diagnostics().stream()
                .filter(issue -> issue.scope() == scope
                        && issue.code().equals(code)
                        && issue.path().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Expected " + scope + ":" + code + " at " + path
                                + " but got " + exception.diagnostics()));
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
