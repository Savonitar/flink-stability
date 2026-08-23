package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Validates the resolved declaration graph and expected oracle before Docker exists. */
public final class ScenarioPreflightValidator {
    public ResolvedScenario validateScenario(ResolvedScenario scenario) {
        Objects.requireNonNull(scenario, "scenario");
        List<PreflightIssue> issues = validateResolvedScenario(scenario);
        if (!issues.isEmpty()) {
            throw new ScenarioPreflightException(issues);
        }
        return scenario;
    }

    public ResolvedScenarioPlan validate(ResolvedScenarioPlan plan) {
        Objects.requireNonNull(plan, "plan");
        List<PreflightIssue> issues = validateResolvedScenario(plan.scenario());
        List<SideIndex> sideIndexes = indexResolvedScenario(plan.scenario(), new ArrayList<>());

        validateExpectedContract(plan, sideIndexes, issues);
        if (!issues.isEmpty()) {
            throw new ScenarioPreflightException(issues);
        }
        return plan;
    }

    private static List<PreflightIssue> validateResolvedScenario(ResolvedScenario scenario) {
        List<PreflightIssue> issues = new ArrayList<>();
        List<SideIndex> indexes = indexResolvedScenario(scenario, issues);
        Path source = scenario.template().source();
        for (int index = 0; index < scenario.sides().size(); index++) {
            validateSide(
                    source,
                    scopeOf(scenario.sides().get(index).side()),
                    indexes.get(index),
                    issues);
        }
        return issues;
    }

    private static List<SideIndex> indexResolvedScenario(
            ResolvedScenario scenario, List<PreflightIssue> issues) {
        Path source = scenario.template().source();
        List<SideIndex> indexes = new ArrayList<>();
        for (ResolvedSide side : scenario.sides()) {
            indexes.add(indexSide(source, scopeOf(side.side()), side.document(), issues));
        }
        return indexes;
    }

    static List<PreflightIssue> validateResolvedSide(
            Path source, ResolutionScope scope, ObjectNode document) {
        List<PreflightIssue> issues = new ArrayList<>();
        SideIndex index = indexSide(source, scope, document, issues);
        validateSide(source, scope, index, issues);
        return List.copyOf(issues);
    }

    private static SideIndex indexSide(
            Path source,
            ResolutionScope scope,
            ObjectNode document,
            List<PreflightIssue> issues) {
        Map<String, ClusterIndex> clusters = new LinkedHashMap<>();
        List<InputEndpoint> inputs = new ArrayList<>();
        ObjectNode clusterNodes = objectAt(document, "/setup/kafka/clusters");
        clusterNodes.fields().forEachRemaining(clusterEntry -> {
            String clusterName = clusterEntry.getKey();
            ObjectNode cluster = (ObjectNode) clusterEntry.getValue();
            BigInteger brokers = cluster.path("brokers").bigIntegerValue();
            Map<String, String> topics = new LinkedHashMap<>();
            ArrayNode topicNodes = (ArrayNode) cluster.get("topics");
            for (int topicIndex = 0; topicIndex < topicNodes.size(); topicIndex++) {
                ObjectNode topic = (ObjectNode) topicNodes.get(topicIndex);
                String topicName = topic.path("name").textValue();
                String topicPath = "$/setup/kafka/clusters/" + pointer(clusterName)
                        + "/topics/" + topicIndex;
                String earlierPath = topics.putIfAbsent(topicName, topicPath);
                if (earlierPath != null) {
                    issues.add(issue(source, scope, "preflight.topic.duplicate-name",
                            topicPath + "/name",
                            "Topic '" + topicName + "' duplicates " + earlierPath + "/name"));
                }
                BigInteger replication = topic.path("replication_factor").bigIntegerValue();
                if (replication.compareTo(brokers) > 0) {
                    issues.add(issue(source, scope,
                            "preflight.kafka.replication-exceeds-brokers",
                            topicPath + "/replication_factor",
                            "Replication factor " + replication + " exceeds cluster '"
                                    + clusterName + "' broker count " + brokers));
                }
                if (topic.get("input_source") instanceof ObjectNode input) {
                    inputs.add(new InputEndpoint(
                            clusterName,
                            topicName,
                            input.path("mode").textValue(),
                            optionalText(input, "connect_via_proxy"),
                            topicPath + "/input_source"));
                }
            }
            clusters.put(clusterName, new ClusterIndex(brokers, Map.copyOf(topics)));
        });

        Map<String, ProxyIndex> proxies = new LinkedHashMap<>();
        JsonNode proxyNodes = document.at("/setup/proxies");
        if (proxyNodes instanceof ObjectNode proxyObject) {
            proxyObject.fields().forEachRemaining(entry -> {
                ObjectNode proxy = (ObjectNode) entry.getValue();
                proxies.put(entry.getKey(), new ProxyIndex(
                        proxy.path("cluster").textValue(),
                        optionalText((ObjectNode) proxy.get("bootstrap"), "cluster"),
                        "$/setup/proxies/" + pointer(entry.getKey())));
            });
        }

        Set<String> connectorAliases = fieldNames(objectAt(document, "/subject/connectors"));
        Map<String, JobIndex> jobs = new LinkedHashMap<>();
        List<KafkaEndpoint> endpoints = new ArrayList<>();
        ArrayNode jobNodes = (ArrayNode) document.at("/workload/jobs");
        for (int jobIndex = 0; jobIndex < jobNodes.size(); jobIndex++) {
            ObjectNode job = (ObjectNode) jobNodes.get(jobIndex);
            String alias = job.path("alias").textValue();
            String jobPath = "$/workload/jobs/" + jobIndex;
            ObjectNode sourceEndpoint = (ObjectNode) job.get("source");
            ObjectNode sinkEndpoint = (ObjectNode) job.get("sink");
            JobIndex indexedJob = new JobIndex(
                    alias,
                    new TopicEndpoint(
                            sourceEndpoint.path("cluster").textValue(),
                            sourceEndpoint.path("topic").textValue(),
                            optionalText(sourceEndpoint, "connect_via_proxy"),
                            jobPath + "/source"),
                    new SinkEndpoint(
                            sinkEndpoint.path("cluster").textValue(),
                            sinkEndpoint.path("topic").textValue(),
                            optionalText(sinkEndpoint, "connect_via_proxy"),
                            sinkEndpoint.path("delivery_guarantee").textValue(),
                            optionalText(sinkEndpoint, "transactional_id_prefix"),
                            jobPath + "/sink"),
                    jobPath);
            JobIndex earlier = jobs.putIfAbsent(alias, indexedJob);
            if (earlier != null) {
                issues.add(issue(source, ResolutionScope.COMMON, "preflight.job.duplicate-alias",
                        jobPath + "/alias",
                        "Job alias '" + alias + "' duplicates " + earlier.path() + "/alias"));
            }
            endpoints.add(KafkaEndpoint.source(indexedJob));
            endpoints.add(KafkaEndpoint.sink(indexedJob));

            if (job.get("connectors") instanceof ArrayNode connectors) {
                for (int connectorIndex = 0; connectorIndex < connectors.size(); connectorIndex++) {
                    String connector = connectors.get(connectorIndex).textValue();
                    if (!connectorAliases.contains(connector)) {
                        issues.add(issue(source, scope, "preflight.reference.connector-not-found",
                                jobPath + "/connectors/" + connectorIndex,
                                "Job references undeclared subject connector '" + connector + "'"));
                    }
                }
            }
        }
        inputs.forEach(input -> endpoints.add(KafkaEndpoint.input(input)));

        ArrayNode terminalNodes = (ArrayNode) document.get("terminal_validations");
        List<TerminalValidatorIndex> terminalValidators = new ArrayList<>();
        Map<String, String> terminalTypePaths = new LinkedHashMap<>();
        for (int index = 0; index < terminalNodes.size(); index++) {
            ObjectNode validator = (ObjectNode) terminalNodes.get(index);
            String validatorType = validator.path("type").textValue();
            String validatorPath = "$/terminal_validations/" + index;
            String earlierPath = terminalTypePaths.putIfAbsent(validatorType, validatorPath);
            if (earlierPath != null) {
                issues.add(issue(source, ResolutionScope.COMMON, "preflight.terminal.duplicate-type",
                        validatorPath + "/type",
                        "Terminal validator type '" + validatorType
                                + "' duplicates " + earlierPath + "/type"));
            }
            Set<String> customReasons = new LinkedHashSet<>();
            if (validator.get("failure_reasons") instanceof ArrayNode reasons) {
                reasons.forEach(reason -> customReasons.add(reason.textValue()));
            }
            terminalValidators.add(new TerminalValidatorIndex(
                    validatorType,
                    validator,
                    Set.copyOf(customReasons),
                    validatorPath));
        }
        return new SideIndex(
                document,
                Map.copyOf(clusters),
                Map.copyOf(proxies),
                Set.copyOf(connectorAliases),
                Map.copyOf(jobs),
                List.copyOf(inputs),
                List.copyOf(endpoints),
                List.copyOf(terminalValidators));
    }

    private static void validateSide(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            List<PreflightIssue> issues) {
        validateProxies(source, scope, index, issues);
        validateJobs(source, scope, index, issues);
        validateInputSources(source, scope, index, issues);
        validateTerminalValidators(source, scope, index, issues);
    }

    private static void validateProxies(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            List<PreflightIssue> issues) {
        index.proxies().forEach((alias, proxy) -> {
            boolean proxyClusterExists = index.clusters().containsKey(proxy.cluster());
            if (!proxyClusterExists) {
                issues.add(issue(source, scope, "preflight.reference.kafka-cluster-not-found",
                        proxy.path() + "/cluster",
                        "Proxy '" + alias + "' references undeclared Kafka cluster '"
                                + proxy.cluster() + "'"));
            }
            if (proxy.bootstrapCluster() != null) {
                boolean bootstrapClusterExists =
                        index.clusters().containsKey(proxy.bootstrapCluster());
                if (!bootstrapClusterExists) {
                    issues.add(issue(source, scope, "preflight.reference.kafka-cluster-not-found",
                            proxy.path() + "/bootstrap/cluster",
                            "Proxy '" + alias + "' bootstrap references undeclared Kafka cluster '"
                                    + proxy.bootstrapCluster() + "'"));
                }
                if (proxyClusterExists && bootstrapClusterExists
                        && !proxy.cluster().equals(proxy.bootstrapCluster())) {
                    issues.add(issue(source, scope, "preflight.proxy.bootstrap-cluster-mismatch",
                            proxy.path() + "/bootstrap/cluster",
                            "Managed bootstrap cluster '" + proxy.bootstrapCluster()
                                    + "' must equal proxy cluster '" + proxy.cluster() + "'"));
                }
            }
        });
    }

    private static void validateJobs(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            List<PreflightIssue> issues) {
        index.jobs().values().forEach(job -> {
            validateTopicReference(source, scope, index, job.source().cluster(), job.source().topic(),
                    job.source().path(), "job source", issues);
            validateTopicReference(source, scope, index, job.sink().cluster(), job.sink().topic(),
                    job.sink().path(), "job sink", issues);
            validateProxyRoute(source, scope, index, job.source().cluster(), job.source().proxy(),
                    job.source().path(), "job source", issues);
            validateProxyRoute(source, scope, index, job.sink().cluster(), job.sink().proxy(),
                    job.sink().path(), "job sink", issues);
        });
        index.inputs().forEach(input -> validateProxyRoute(
                source, scope, index, input.cluster(), input.proxy(), input.path(),
                "input source", issues));
    }

    private static void validateInputSources(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            List<PreflightIssue> issues) {
        if (index.inputs().size() > 1) {
            List<String> inputs = index.inputs().stream()
                    .map(input -> input.cluster() + "/" + input.topic())
                    .toList();
            index.inputs().forEach(input -> issues.add(issue(
                    source, scope, "preflight.input.multiple-sources",
                    input.path(), "v1 permits one input_source, found " + inputs)));
            return;
        }

        InputEndpoint input = index.inputs().isEmpty() ? null : index.inputs().getFirst();
        boolean hasCustomValidator = index.terminalValidators().stream()
                .anyMatch(validator -> validator.type().equals("custom"));
        boolean hasManifestValidator = index.terminalValidators().stream()
                .anyMatch(validator -> validator.type().equals("kafka.id-set"));

        if (input != null && input.mode().equals("custom")) {
            if (!hasCustomValidator) {
                issues.add(issue(source, scope, "preflight.input.custom-validator-required",
                        input.path(), "Custom input requires at least one custom terminal validator"));
            }
            index.terminalValidators().stream()
                    .filter(validator -> !validator.type().equals("custom"))
                    .forEach(validator -> issues.add(issue(
                            source, scope, "preflight.input.custom-built-in-conflict",
                            validator.path(),
                            "Custom input cannot use built-in terminal validator '"
                                    + validator.type() + "'")));
        } else if (input != null && !hasManifestValidator) {
            issues.add(issue(source, scope, "preflight.input.built-in-validator-required",
                    input.path(), "Harness-owned input requires a kafka.id-set terminal validator"));
        }
    }

    private static void validateTerminalValidators(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            List<PreflightIssue> issues) {
        InputEndpoint input = index.inputs().size() == 1 ? index.inputs().getFirst() : null;
        for (TerminalValidatorIndex validator : index.terminalValidators()) {
            ObjectNode config = validator.config();
            switch (validator.type()) {
                case "kafka.id-set", "kafka.record-count" -> validateTopicReference(
                        source,
                        scope,
                        index,
                        config.path("cluster").textValue(),
                        config.path("topic").textValue(),
                        validator.path(),
                        "terminal validator '" + validator.type() + "'",
                        issues);
                case "kafka.no-hanging-transactions" -> {
                    String cluster = config.path("cluster").textValue();
                    boolean clusterExists = index.clusters().containsKey(cluster);
                    if (!clusterExists) {
                        issues.add(issue(source, scope,
                                "preflight.reference.kafka-cluster-not-found",
                                validator.path() + "/cluster",
                                "Terminal validator references undeclared Kafka cluster '"
                                        + cluster + "'"));
                    }
                    String prefix = config.path("transactional_id_prefix").textValue();
                    boolean matchingSink = index.jobs().values().stream().anyMatch(job ->
                            job.sink().cluster().equals(cluster)
                                    && "EXACTLY_ONCE".equals(job.sink().deliveryGuarantee())
                                    && prefix.equals(job.sink().transactionalIdPrefix()));
                    if (clusterExists && !matchingSink) {
                        issues.add(issue(source, scope,
                                "preflight.terminal.transactional-sink-not-found",
                                validator.path() + "/transactional_id_prefix",
                                "No exactly-once sink on cluster '" + cluster
                                        + "' uses transactional ID prefix '" + prefix + "'"));
                    }
                }
                case "flink.log-match" -> {
                    String job = config.path("job").textValue();
                    if (!index.jobs().containsKey(job)) {
                        issues.add(issue(source, scope, "preflight.reference.job-not-found",
                                validator.path() + "/job",
                                "Terminal validator references undeclared job '" + job + "'"));
                    }
                }
                default -> {
                    // The resolved schema owns the closed validator type union.
                }
            }

            if (validator.type().equals("kafka.id-set")
                    && index.inputs().size() <= 1
                    && (index.inputs().isEmpty()
                            || !index.inputs().getFirst().mode().equals("custom"))) {
                validateIdSetManifestLink(source, scope, index, validator, input, issues);
            }
        }
    }

    private static void validateIdSetManifestLink(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            TerminalValidatorIndex validator,
            InputEndpoint input,
            List<PreflightIssue> issues) {
        if (input == null || input.mode().equals("custom")) {
            issues.add(issue(source, scope, "preflight.input.manifest-not-available",
                    validator.path() + "/expected",
                    "kafka.id-set requires one harness-owned input manifest"));
            return;
        }
        String outputCluster = validator.config().path("cluster").textValue();
        String outputTopic = validator.config().path("topic").textValue();
        if (!topicExists(index, outputCluster, outputTopic)) {
            return;
        }
        List<JobIndex> outputJobs = index.jobs().values().stream().filter(job ->
                job.sink().cluster().equals(outputCluster)
                        && job.sink().topic().equals(outputTopic))
                .sorted(java.util.Comparator.comparing(JobIndex::alias))
                .toList();
        if (outputJobs.size() != 1) {
            issues.add(issue(source, scope, "preflight.input.validator-source-mismatch",
                    validator.path(),
                    "kafka.id-set output '" + outputCluster + "/" + outputTopic
                            + "' must be the sink of exactly one job, found "
                            + outputJobs.stream().map(JobIndex::alias).toList()));
            return;
        }
        JobIndex outputJob = outputJobs.getFirst();
        if (!outputJob.source().cluster().equals(input.cluster())
                || !outputJob.source().topic().equals(input.topic())) {
            issues.add(issue(source, scope, "preflight.input.validator-source-mismatch",
                    validator.path(),
                    "Job '" + outputJob.alias() + "' writes kafka.id-set output '"
                            + outputCluster + "/" + outputTopic + "' but reads '"
                            + outputJob.source().cluster() + "/" + outputJob.source().topic()
                            + "' instead of input manifest topic '" + input.cluster() + "/"
                            + input.topic() + "'"));
        }
    }

    private static void validateExpectedContract(
            ResolvedScenarioPlan plan,
            List<SideIndex> sideIndexes,
            List<PreflightIssue> issues) {
        ExpectedResultSpecification specification = plan.selectedExpectation().specification();
        ObjectNode expectedDocument = specification.document();
        Set<String> terminalTypes = new LinkedHashSet<>();
        Map<String, Set<String>> allowedReasons = new HashMap<>();
        boolean customInput = false;
        for (SideIndex side : sideIndexes) {
            customInput |= side.inputs().size() == 1 && side.inputs().getFirst().mode().equals("custom");
            for (TerminalValidatorIndex validator : side.terminalValidators()) {
                terminalTypes.add(validator.type());
                allowedReasons.computeIfAbsent(validator.type(), ignored -> new LinkedHashSet<>())
                        .addAll(ValidatorReasonRegistry.builtInReasons(validator.type()));
                if (validator.type().equals("custom")) {
                    allowedReasons.get("custom").addAll(validator.customReasons());
                }
            }
        }

        validateExpectation(
                specification.source(), "$/default", (ObjectNode) expectedDocument.get("default"),
                terminalTypes, allowedReasons, customInput, issues);
        if (expectedDocument.get("cases") instanceof ArrayNode cases) {
            for (int index = 0; index < cases.size(); index++) {
                ObjectNode replacement = ((ObjectNode) cases.get(index)).deepCopy();
                replacement.remove("when");
                validateExpectation(
                        specification.source(), "$/cases/" + index, replacement,
                        terminalTypes, allowedReasons, customInput, issues);
            }
        }
    }

    private static void validateExpectation(
            Path source,
            String path,
            ObjectNode expectation,
            Set<String> terminalTypes,
            Map<String, Set<String>> allowedReasons,
            boolean customInput,
            List<PreflightIssue> issues) {
        if (expectation.has("outcome")) {
            validateFailExpectation(
                    source, ResolutionScope.COMMON, path, expectation,
                    terminalTypes, allowedReasons, customInput, issues);
        } else {
            validateFailExpectation(
                    source, ResolutionScope.CANDIDATE, path + "/candidate",
                    (ObjectNode) expectation.get("candidate"),
                    terminalTypes, allowedReasons, customInput, issues);
        }
    }

    private static void validateFailExpectation(
            Path source,
            ResolutionScope scope,
            String path,
            ObjectNode expectation,
            Set<String> terminalTypes,
            Map<String, Set<String>> allowedReasons,
            boolean customInput,
            List<PreflightIssue> issues) {
        if (!"fail".equals(expectation.path("outcome").textValue())) {
            return;
        }
        String oracle = expectation.path("oracle").textValue();
        String reason = expectation.path("reason").textValue();
        if (!terminalTypes.contains(oracle)) {
            issues.add(issue(source, scope, "preflight.expectation.oracle-not-terminal",
                    path + "/oracle",
                    "Expected failure oracle '" + oracle
                            + "' is not a top-level terminal validator type"));
        } else if (!allowedReasons.getOrDefault(oracle, Set.of()).contains(reason)) {
            issues.add(issue(source, scope, "preflight.expectation.reason-not-declared",
                    path + "/reason",
                    "Reason '" + reason + "' is not declared for terminal validator '"
                            + oracle + "'; allowed reasons: "
                            + allowedReasons.getOrDefault(oracle, Set.of()).stream().sorted().toList()));
        }
        if (customInput && !"custom".equals(oracle)) {
            issues.add(issue(source, scope, "preflight.input.custom-expectation-oracle",
                    path + "/oracle",
                    "A custom input contract may expect failure only from a custom terminal validator"));
        }
    }

    private static void validateTopicReference(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            String cluster,
            String topic,
            String path,
            String owner,
            List<PreflightIssue> issues) {
        ClusterIndex clusterIndex = index.clusters().get(cluster);
        if (clusterIndex == null) {
            issues.add(issue(source, scope, "preflight.reference.kafka-cluster-not-found",
                    path + "/cluster", owner + " references undeclared Kafka cluster '" + cluster + "'"));
            return;
        }
        if (!clusterIndex.topics().containsKey(topic)) {
            issues.add(issue(source, scope, "preflight.reference.kafka-topic-not-found",
                    path + "/topic",
                    owner + " references undeclared topic '" + topic
                            + "' in Kafka cluster '" + cluster + "'"));
        }
    }

    private static boolean topicExists(SideIndex index, String cluster, String topic) {
        ClusterIndex clusterIndex = index.clusters().get(cluster);
        return clusterIndex != null && clusterIndex.topics().containsKey(topic);
    }

    private static void validateProxyRoute(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            String endpointCluster,
            String proxyAlias,
            String path,
            String owner,
            List<PreflightIssue> issues) {
        if (proxyAlias == null) {
            return;
        }
        ProxyIndex proxy = index.proxies().get(proxyAlias);
        if (proxy == null) {
            issues.add(issue(source, scope, "preflight.reference.proxy-not-found",
                    path + "/connect_via_proxy",
                    owner + " references undeclared proxy '" + proxyAlias + "'"));
        } else if (index.clusters().containsKey(endpointCluster)
                && index.clusters().containsKey(proxy.cluster())
                && !endpointCluster.equals(proxy.cluster())) {
            issues.add(issue(source, scope, "preflight.proxy.endpoint-cluster-mismatch",
                    path + "/connect_via_proxy",
                    owner + " uses Kafka cluster '" + endpointCluster + "' but proxy '"
                            + proxyAlias + "' is attached to cluster '" + proxy.cluster() + "'"));
        }
    }

    private static ObjectNode objectAt(ObjectNode document, String pointer) {
        return (ObjectNode) document.at(pointer);
    }

    private static Set<String> fieldNames(ObjectNode object) {
        Set<String> names = new LinkedHashSet<>();
        object.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static String optionalText(ObjectNode object, String field) {
        JsonNode value = object.get(field);
        return value == null ? null : value.textValue();
    }

    private static ResolutionScope scopeOf(ScenarioSide side) {
        return switch (side) {
            case SINGLE -> ResolutionScope.SINGLE;
            case BASELINE -> ResolutionScope.BASELINE;
            case CANDIDATE -> ResolutionScope.CANDIDATE;
        };
    }

    private static String pointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }

    private static PreflightIssue issue(
            Path source,
            ResolutionScope scope,
            String code,
            String path,
            String message) {
        return new PreflightIssue(source, scope, code, path, message);
    }

    private record ClusterIndex(BigInteger brokers, Map<String, String> topics) {}

    private record ProxyIndex(String cluster, String bootstrapCluster, String path) {}

    private record InputEndpoint(
            String cluster, String topic, String mode, String proxy, String path) {}

    private record TopicEndpoint(String cluster, String topic, String proxy, String path) {}

    private record SinkEndpoint(
            String cluster,
            String topic,
            String proxy,
            String deliveryGuarantee,
            String transactionalIdPrefix,
            String path) {}

    private record JobIndex(
            String alias, TopicEndpoint source, SinkEndpoint sink, String path) {}

    private record KafkaEndpoint(
            String kind,
            String cluster,
            String topic,
            String proxy,
            String job,
            String deliveryGuarantee,
            String transactionalIdPrefix,
            String path) {
        private static KafkaEndpoint source(JobIndex job) {
            return new KafkaEndpoint(
                    "source", job.source().cluster(), job.source().topic(), job.source().proxy(),
                    job.alias(), null, null, job.source().path());
        }

        private static KafkaEndpoint sink(JobIndex job) {
            return new KafkaEndpoint(
                    "sink", job.sink().cluster(), job.sink().topic(), job.sink().proxy(),
                    job.alias(), job.sink().deliveryGuarantee(),
                    job.sink().transactionalIdPrefix(), job.sink().path());
        }

        private static KafkaEndpoint input(InputEndpoint input) {
            return new KafkaEndpoint(
                    "input", input.cluster(), input.topic(), input.proxy(),
                    null, null, null, input.path());
        }
    }

    private record TerminalValidatorIndex(
            String type,
            ObjectNode config,
            Set<String> customReasons,
            String path) {}

    private record SideIndex(
            ObjectNode document,
            Map<String, ClusterIndex> clusters,
            Map<String, ProxyIndex> proxies,
            Set<String> connectorAliases,
            Map<String, JobIndex> jobs,
            List<InputEndpoint> inputs,
            List<KafkaEndpoint> endpoints,
            List<TerminalValidatorIndex> terminalValidators) {}
}
