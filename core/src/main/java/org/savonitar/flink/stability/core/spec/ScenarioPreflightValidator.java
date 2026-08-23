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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Validates the resolved declaration graph and expected oracle before Docker exists. */
public final class ScenarioPreflightValidator {
    private static final Pattern STATIC_TARGET_NAME =
            Pattern.compile("^(taskmanager|jobmanager|broker)-([1-9][0-9]*)$");

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
        ObjectNode flink = objectAt(document, "/setup/flink");
        BigInteger jobmanagers = flink.path("jobmanagers").bigIntegerValue();
        BigInteger taskmanagers = flink.path("taskmanagers").bigIntegerValue();
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
                jobmanagers,
                taskmanagers,
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
        validatePhases(source, scope, index, issues);
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
        boolean customInput = index.inputs().size() == 1
                && index.inputs().getFirst().mode().equals("custom");
        for (TerminalValidatorIndex validator : index.terminalValidators()) {
            validateValidatorReferences(
                    source, scope, index, validator, !customInput, issues);
        }
    }

    private static void validateValidatorReferences(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            TerminalValidatorIndex validator,
            boolean validateManifestLink,
            List<PreflightIssue> issues) {
        ObjectNode config = validator.config();
        switch (validator.type()) {
            case "kafka.id-set", "kafka.record-count" -> validateTopicReference(
                    source,
                    scope,
                    index,
                    config.path("cluster").textValue(),
                    config.path("topic").textValue(),
                    validator.path(),
                    "validator '" + validator.type() + "'",
                    issues);
            case "kafka.no-hanging-transactions" -> {
                String cluster = config.path("cluster").textValue();
                boolean clusterExists = index.clusters().containsKey(cluster);
                if (!clusterExists) {
                    issues.add(issue(source, scope,
                            "preflight.reference.kafka-cluster-not-found",
                            validator.path() + "/cluster",
                            "Validator references undeclared Kafka cluster '" + cluster + "'"));
                }
                String prefix = config.path("transactional_id_prefix").textValue();
                boolean matchingSink = hasTransactionalSink(index, cluster, prefix);
                if (clusterExists && !matchingSink) {
                    issues.add(issue(source, scope,
                            "preflight.validator.transactional-sink-not-found",
                            validator.path() + "/transactional_id_prefix",
                            "No exactly-once sink on cluster '" + cluster
                                    + "' uses transactional ID prefix '" + prefix + "'"));
                }
            }
            case "flink.log-match" -> validateJobReference(
                    source,
                    index,
                    config.path("job").textValue(),
                    validator.path() + "/job",
                    "Validator",
                    issues);
            default -> {
                // The resolved schema owns the closed validator type union.
            }
        }

        InputEndpoint input = index.inputs().size() == 1 ? index.inputs().getFirst() : null;
        if (validateManifestLink
                && validator.type().equals("kafka.id-set")
                && index.inputs().size() <= 1) {
            validateIdSetManifestLink(source, scope, index, validator, input, issues);
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

    private static void validatePhases(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            List<PreflightIssue> issues) {
        ArrayNode phases = (ArrayNode) index.document().get("phases");
        Map<String, String> phasePaths = new LinkedHashMap<>();
        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            ObjectNode phase = (ObjectNode) phases.get(phaseIndex);
            String phasePath = "$/phases/" + phaseIndex;
            String name = phase.path("name").textValue();
            String earlier = phasePaths.putIfAbsent(name, phasePath);
            if (earlier != null) {
                issues.add(issue(source, ResolutionScope.COMMON,
                        "preflight.phase.duplicate-name",
                        phasePath + "/name",
                        "Phase name '" + name + "' duplicates " + earlier + "/name"));
            }
        }

        StateArtifactLedger ledger = indexStateArtifacts(source, scope, phases, issues);
        Set<StateArtifactKey> priorArtifacts = new LinkedHashSet<>();
        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            ObjectNode phase = (ObjectNode) phases.get(phaseIndex);
            validateSteps(
                    source,
                    scope,
                    index,
                    (ArrayNode) phase.get("steps"),
                    "$/phases/" + phaseIndex + "/steps",
                    ledger,
                    priorArtifacts,
                    issues);
        }
    }

    private static StateArtifactLedger indexStateArtifacts(
            Path source,
            ResolutionScope scope,
            ArrayNode phases,
            List<PreflightIssue> issues) {
        List<StateArtifactDeclaration> declarations = new ArrayList<>();
        Map<StateArtifactKey, StateArtifactDeclaration> firstByKey = new LinkedHashMap<>();
        Set<StateArtifactKey> invalidKeys = new LinkedHashSet<>();
        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            ObjectNode phase = (ObjectNode) phases.get(phaseIndex);
            indexStateArtifactsInSteps(
                    source,
                    scope,
                    (ArrayNode) phase.get("steps"),
                    "$/phases/" + phaseIndex + "/steps",
                    false,
                    declarations,
                    firstByKey,
                    invalidKeys,
                    issues);
        }
        return new StateArtifactLedger(
                List.copyOf(declarations), Set.copyOf(invalidKeys));
    }

    private static void indexStateArtifactsInSteps(
            Path source,
            ResolutionScope scope,
            ArrayNode steps,
            String stepsPath,
            boolean repeated,
            List<StateArtifactDeclaration> declarations,
            Map<StateArtifactKey, StateArtifactDeclaration> firstByKey,
            Set<StateArtifactKey> invalidKeys,
            List<PreflightIssue> issues) {
        for (int stepIndex = 0; stepIndex < steps.size(); stepIndex++) {
            ObjectNode step = (ObjectNode) steps.get(stepIndex);
            String stepPath = stepsPath + "/" + stepIndex;
            String type = step.has("checkpoint")
                    ? "checkpoint"
                    : step.has("savepoint") ? "savepoint" : null;
            if (type != null) {
                ObjectNode producer = (ObjectNode) step.get(type);
                if (producer.has("as")) {
                    StateArtifactKey key = new StateArtifactKey(
                            producer.path("job").textValue(),
                            type,
                            producer.path("as").textValue());
                    StateArtifactDeclaration declaration =
                            new StateArtifactDeclaration(key, stepPath + "/" + type + "/as");
                    declarations.add(declaration);
                    StateArtifactDeclaration earlier = firstByKey.putIfAbsent(key, declaration);
                    if (earlier != null) {
                        invalidKeys.add(key);
                        issues.add(issue(source, ResolutionScope.COMMON,
                                "preflight.state-artifact.duplicate-identity",
                                declaration.path(),
                                "State artifact '" + key.name() + "' for job '" + key.job()
                                        + "' and type '" + key.type() + "' duplicates "
                                        + earlier.path()));
                    }
                    if (repeated) {
                        invalidKeys.add(key);
                        issues.add(issue(source, scope,
                                "preflight.state-artifact.repeated-name",
                                declaration.path(),
                                "Named state artifact '" + key.name()
                                        + "' is inside a loop that executes more than once"));
                    }
                }
            }
            if (step.get("loop") instanceof ObjectNode loop) {
                boolean nestedRepeated = repeated
                        || loop.path("times").bigIntegerValue().compareTo(BigInteger.ONE) > 0;
                indexStateArtifactsInSteps(
                        source,
                        scope,
                        (ArrayNode) loop.get("steps"),
                        stepPath + "/loop/steps",
                        nestedRepeated,
                        declarations,
                        firstByKey,
                        invalidKeys,
                        issues);
            }
        }
    }

    private static void validateSteps(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            ArrayNode steps,
            String stepsPath,
            StateArtifactLedger ledger,
            Set<StateArtifactKey> priorArtifacts,
            List<PreflightIssue> issues) {
        for (int stepIndex = 0; stepIndex < steps.size(); stepIndex++) {
            ObjectNode step = (ObjectNode) steps.get(stepIndex);
            String stepPath = stepsPath + "/" + stepIndex;
            if (step.get("await") instanceof ObjectNode await) {
                validateAwait(source, scope, index, await, stepPath + "/await", issues);
            } else if (step.get("start") instanceof ObjectNode start) {
                validateJobReference(
                        source, index, start.path("job").textValue(),
                        stepPath + "/start/job", "Start step", issues);
            } else if (step.get("kill") instanceof ObjectNode kill) {
                validateProcessTarget(
                        source, scope, index, (ObjectNode) kill.get("target"),
                        stepPath + "/kill/target", issues);
            } else if (step.get("stop") instanceof ObjectNode stop) {
                validateProcessTarget(
                        source, scope, index, (ObjectNode) stop.get("target"),
                        stepPath + "/stop/target", issues);
            } else if (step.get("loop") instanceof ObjectNode loop) {
                validateSteps(
                        source,
                        scope,
                        index,
                        (ArrayNode) loop.get("steps"),
                        stepPath + "/loop/steps",
                        ledger,
                        priorArtifacts,
                        issues);
            } else if (step.get("checkpoint") instanceof ObjectNode checkpoint) {
                validateStateProducer(
                        source, index, "checkpoint", checkpoint,
                        stepPath + "/checkpoint", priorArtifacts, issues);
            } else if (step.get("savepoint") instanceof ObjectNode savepoint) {
                validateStateProducer(
                        source, index, "savepoint", savepoint,
                        stepPath + "/savepoint", priorArtifacts, issues);
            } else if (step.get("restore") instanceof ObjectNode restore) {
                validateRestore(
                        source, index, restore, stepPath + "/restore",
                        ledger, priorArtifacts, issues);
            } else if (step.get("restart") instanceof ObjectNode restart) {
                validateRestart(source, index, restart, stepPath + "/restart", issues);
            } else if (step.get("validate") instanceof ObjectNode inlineValidator) {
                Set<String> customReasons = new LinkedHashSet<>();
                if (inlineValidator.get("failure_reasons") instanceof ArrayNode reasons) {
                    reasons.forEach(reason -> customReasons.add(reason.textValue()));
                }
                validateValidatorReferences(
                        source,
                        scope,
                        index,
                        new TerminalValidatorIndex(
                                inlineValidator.path("type").textValue(),
                                inlineValidator,
                                Set.copyOf(customReasons),
                                stepPath + "/validate"),
                        true,
                        issues);
            }
        }
    }

    private static void validateAwait(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            ObjectNode await,
            String path,
            List<PreflightIssue> issues) {
        ObjectNode condition = (ObjectNode) await.get("condition");
        String conditionPath = path + "/condition";
        switch (condition.path("type").textValue()) {
            case "job-state", "checkpoint-completed", "savepoint-completed" ->
                    validateJobReference(
                            source, index, condition.path("job").textValue(),
                            conditionPath + "/job", "Await condition", issues);
            case "log-marker" -> {
                if (condition.has("job")) {
                    validateJobReference(
                            source, index, condition.path("job").textValue(),
                            conditionPath + "/job", "Await condition", issues);
                }
            }
            case "taskmanager-count" -> {
                BigInteger value = condition.path("value").bigIntegerValue();
                if (value.compareTo(index.taskmanagers()) > 0) {
                    issues.add(issue(source, scope,
                            "preflight.await.taskmanager-count-exceeds-configured",
                            conditionPath + "/value",
                            "Awaited TaskManager count " + value
                                    + " exceeds configured count " + index.taskmanagers()));
                }
            }
            case "kafka-transaction-state" -> {
                String cluster = condition.path("cluster").textValue();
                if (!index.clusters().containsKey(cluster)) {
                    issues.add(issue(source, scope,
                            "preflight.reference.kafka-cluster-not-found",
                            conditionPath + "/cluster",
                            "Await condition references undeclared Kafka cluster '"
                                    + cluster + "'"));
                } else {
                    String prefix = condition.path("transactional_id_prefix").textValue();
                    if (!hasTransactionalSink(index, cluster, prefix)) {
                        issues.add(issue(source, scope,
                                "preflight.await.transactional-sink-not-found",
                                conditionPath + "/transactional_id_prefix",
                                "No exactly-once sink on cluster '" + cluster
                                        + "' uses transactional ID prefix '" + prefix + "'"));
                    }
                }
            }
            case "record-threshold" -> validateTopicReference(
                    source,
                    scope,
                    index,
                    condition.path("cluster").textValue(),
                    condition.path("topic").textValue(),
                    conditionPath,
                    "await condition",
                    issues);
            default -> {
                // The resolved schema owns the closed await-condition union.
            }
        }
    }

    private static void validateProcessTarget(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            ObjectNode target,
            String path,
            List<PreflightIssue> issues) {
        if ("selector".equals(target.path("kind").textValue())) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "capability.runtime-target-selector.unsupported",
                    path + "/kind",
                    "v1 supports only statically named process targets"));
            return;
        }

        String role = target.path("role").textValue();
        String name = target.path("name").textValue();
        Matcher matcher = STATIC_TARGET_NAME.matcher(name);
        boolean matchesRole = matcher.matches() && matcher.group(1).equals(role);
        BigInteger ordinal = matchesRole
                ? new BigInteger(matcher.group(2))
                : BigInteger.ZERO;
        if (role.equals("broker") && matchesRole) {
            List<String> matchingClusters = index.clusters().entrySet().stream()
                    .filter(entry -> ordinal.compareTo(entry.getValue().brokers()) <= 0)
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
            if (matchingClusters.size() == 1) {
                return;
            }
            if (matchingClusters.size() > 1) {
                issues.add(issue(source, scope,
                        "preflight.target.broker-cluster-ambiguous",
                        path + "/name",
                        "Named broker target '" + name
                                + "' exists in Kafka clusters " + matchingClusters));
                return;
            }
        }

        BigInteger upperBound = switch (role) {
            case "taskmanager" -> index.taskmanagers();
            case "jobmanager" -> index.jobmanagers();
            case "broker" -> index.clusters().values().stream()
                    .map(ClusterIndex::brokers)
                    .max(BigInteger::compareTo)
                    .orElse(BigInteger.ZERO);
            default -> BigInteger.ZERO;
        };
        if (!matchesRole || ordinal.compareTo(upperBound) > 0) {
            issues.add(issue(source, scope,
                    "preflight.target.named-not-found",
                    path + "/name",
                    "No declared " + role + " target named '" + name
                            + "'; available names are " + role + "-1 through "
                            + role + "-" + upperBound));
        }
    }

    private static void validateStateProducer(
            Path source,
            SideIndex index,
            String type,
            ObjectNode producer,
            String path,
            Set<StateArtifactKey> priorArtifacts,
            List<PreflightIssue> issues) {
        String job = producer.path("job").textValue();
        validateJobReference(source, index, job, path + "/job", "State step", issues);
        if (producer.has("as")) {
            priorArtifacts.add(new StateArtifactKey(
                    job, type, producer.path("as").textValue()));
        }
    }

    private static void validateRestore(
            Path source,
            SideIndex index,
            ObjectNode restore,
            String path,
            StateArtifactLedger ledger,
            Set<StateArtifactKey> priorArtifacts,
            List<PreflightIssue> issues) {
        String job = restore.path("job").textValue();
        if (!validateJobReference(source, index, job, path + "/job", "Restore step", issues)) {
            return;
        }
        if (!(restore.get("from") instanceof ObjectNode from) || !from.has("name")) {
            return;
        }

        String type = from.path("type").textValue();
        String name = from.path("name").textValue();
        String namePath = path + "/from/name";
        StateArtifactKey key = new StateArtifactKey(job, type, name);
        if (ledger.invalidKeys().contains(key)) {
            return;
        }
        if (priorArtifacts.contains(key)) {
            return;
        }
        if (ledger.declarations().stream().anyMatch(declaration -> declaration.key().equals(key))) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "preflight.state-artifact.not-prior",
                    namePath,
                    "Named " + type + " artifact '" + name
                            + "' for job '" + job + "' is declared only after this restore"));
            return;
        }
        if (ledger.declarations().stream().anyMatch(declaration ->
                declaration.key().job().equals(job)
                        && declaration.key().name().equals(name))) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "preflight.state-artifact.type-mismatch",
                    namePath,
                    "State artifact '" + name + "' for job '" + job
                            + "' is not a " + type));
            return;
        }
        if (ledger.declarations().stream().anyMatch(declaration ->
                declaration.key().type().equals(type)
                        && declaration.key().name().equals(name))) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "preflight.state-artifact.job-mismatch",
                    namePath,
                    "Named " + type + " artifact '" + name
                            + "' belongs to another job"));
            return;
        }
        issues.add(issue(source, ResolutionScope.COMMON,
                "preflight.state-artifact.not-found",
                namePath,
                "No named " + type + " artifact '" + name
                        + "' is declared for job '" + job + "'"));
    }

    private static void validateRestart(
            Path source,
            SideIndex index,
            ObjectNode restart,
            String path,
            List<PreflightIssue> issues) {
        if ("kafka".equals(restart.path("component").textValue())
                && index.clusters().size() != 1) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "preflight.restart.kafka-cluster-ambiguous",
                    path + "/component",
                    "Kafka restart is ambiguous because the scenario declares clusters "
                            + index.clusters().keySet().stream().sorted().toList()));
        }
    }

    private static boolean validateJobReference(
            Path source,
            SideIndex index,
            String job,
            String path,
            String owner,
            List<PreflightIssue> issues) {
        if (index.jobs().containsKey(job)) {
            return true;
        }
        issues.add(issue(source, ResolutionScope.COMMON,
                "preflight.reference.job-not-found",
                path,
                owner + " references undeclared job '" + job + "'"));
        return false;
    }

    private static boolean hasTransactionalSink(
            SideIndex index, String cluster, String prefix) {
        return index.jobs().values().stream().anyMatch(job ->
                job.sink().cluster().equals(cluster)
                        && "EXACTLY_ONCE".equals(job.sink().deliveryGuarantee())
                        && prefix.equals(job.sink().transactionalIdPrefix()));
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

    private record StateArtifactKey(String job, String type, String name) {}

    private record StateArtifactDeclaration(StateArtifactKey key, String path) {}

    private record StateArtifactLedger(
            List<StateArtifactDeclaration> declarations,
            Set<StateArtifactKey> invalidKeys) {}

    private record SideIndex(
            ObjectNode document,
            Map<String, ClusterIndex> clusters,
            Map<String, ProxyIndex> proxies,
            Set<String> connectorAliases,
            BigInteger jobmanagers,
            BigInteger taskmanagers,
            Map<String, JobIndex> jobs,
            List<InputEndpoint> inputs,
            List<KafkaEndpoint> endpoints,
            List<TerminalValidatorIndex> terminalValidators) {}
}
