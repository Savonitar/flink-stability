package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.artifact.ArtifactRole;
import org.savonitar.flink.stability.core.artifact.ConnectorClusterBundleBuilder;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.core.spec.resolution.KafkaBrokerImagePolicy;
import org.savonitar.flink.stability.core.artifact.PreparedConnectorBundle;
import org.savonitar.flink.stability.core.artifact.PreparedConnectorRuntimeTargetFactory;
import org.savonitar.flink.stability.core.artifact.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.artifact.ResolvedArtifact;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;
import org.savonitar.flink.stability.core.artifact.WorkloadProtocolArtifactValidator;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fail-closed compiler from the broad v1 schema to the first executable runner subset.
 *
 * <p>{@link #compile(ResolvedScenarioPlan)} is the canonical boundary and must run before
 * artifact preparation or Docker provisioning. {@link #bind(PreparedScenarioPlan,
 * ExecutableScenarioPlan)} performs only artifact-dependent checks and creates the verified
 * connector runtime target.</p>
 */
public final class ExecutableScenarioPlanCompiler {
    public static final String WORKLOAD_PROTOCOL_ATTRIBUTE =
            WorkloadProtocolArtifactValidator.ATTRIBUTE;
    public static final String WORKLOAD_PROTOCOL_VERSION =
            WorkloadProtocolArtifactValidator.VERSION;

    private static final Pattern DURATION = Pattern.compile("^([1-9][0-9]*)(ms|s|m|h)$");
    private static final String RESERVED_WORKLOAD_ARGUMENT_PREFIX =
            "--flink-stability.workload.";
    private static final String FORBIDDEN_BOOTSTRAP_OVERRIDE_ARGUMENT = "--bootstrapServers";
    /** The only terminal oracle the first runner executes. */
    static final String KAFKA_ID_SET = "kafka.id-set";
    private static final Set<String> SUPPORTED_STEP_KEYS = Set.of(
            "await", "wait", "loop", "kill", "restart");
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger INT_MAX = BigInteger.valueOf(Integer.MAX_VALUE);

    private final ConnectorClusterBundleBuilder connectorBundleBuilder;
    private final PreparedConnectorRuntimeTargetFactory runtimeTargetFactory;

    public ExecutableScenarioPlanCompiler() {
        this(new ConnectorClusterBundleBuilder(), new PreparedConnectorRuntimeTargetFactory());
    }

    ExecutableScenarioPlanCompiler(
            ConnectorClusterBundleBuilder connectorBundleBuilder,
            PreparedConnectorRuntimeTargetFactory runtimeTargetFactory) {
        this.connectorBundleBuilder = Objects.requireNonNull(
                connectorBundleBuilder, "connectorBundleBuilder");
        this.runtimeTargetFactory = Objects.requireNonNull(
                runtimeTargetFactory, "runtimeTargetFactory");
    }

    /** Compiles and rejects unsupported valid-v1 features without touching artifacts or Docker. */
    public ExecutableScenarioPlan compile(ResolvedScenarioPlan sourcePlan) {
        Objects.requireNonNull(sourcePlan, "sourcePlan");
        if (sourcePlan.scenario().isExperiment()) {
            throw new SpecificationException(Stage.RUNNER_CAPABILITY, List.of(issue(
                    sourcePlan.scenario().template().source(),
                    ResolutionScope.COMMON,
                    "runner.topology.experiment-unsupported",
                    "$/experiment",
                    "The first runner executes only plain scenarios")));
        }

        Path source = sourcePlan.scenario().template().source();
        ObjectNode document = sourcePlan.scenario().side(ScenarioSide.SINGLE).document();
        List<Diagnostic> issues = new ArrayList<>();
        validateInvocation(source, document, issues);
        validateSetup(source, document, issues);
        validateWorkload(source, document, issues);
        validatePhases(source, document, issues);
        validateTerminalValidation(source, document, issues);
        ExpectationCompiler.validate(sourcePlan, issues);
        if (!issues.isEmpty()) {
            throw new SpecificationException(Stage.RUNNER_CAPABILITY, issues);
        }
        return map(sourcePlan, document);
    }

    /**
     * Binds a precompiled plan to its prepared snapshots. The caller remains the sole owner of
     * {@code preparedPlan} and must keep it open until execution has finished.
     */
    public PreparedExecutableScenarioPlan bind(
            PreparedScenarioPlan preparedPlan,
            ExecutableScenarioPlan executablePlan) {
        Objects.requireNonNull(preparedPlan, "preparedPlan");
        Objects.requireNonNull(executablePlan, "executablePlan");
        if (preparedPlan.scenarioPlan() != executablePlan.sourcePlan()) {
            throw new IllegalArgumentException(
                    "Prepared and executable plans must share the exact resolved scenario plan");
        }

        String jobPath = "$/workload/jobs/0/jar";
        ResolvedArtifact workloadArtifact = preparedPlan
                .artifact(ScenarioSide.SINGLE, jobPath)
                .filter(artifact -> artifact.role() == ArtifactRole.WORKLOAD_JOB)
                .orElseThrow(() -> new IllegalStateException(
                        "Prepared plan has no workload artifact at " + jobPath));
        validateWorkloadProtocol(preparedPlan, workloadArtifact, jobPath);

        PreparedConnectorBundle connectorBundle = connectorBundleBuilder.build(
                preparedPlan,
                ScenarioSide.SINGLE,
                executablePlan.flink().imageReference());
        SubjectEntryClassCheck.verify(
                preparedPlan.scenarioPlan().scenario().template().source(),
                connectorBundle,
                workloadArtifact.preparedPath(),
                jobPath);
        FlinkRuntimeTarget runtimeTarget = runtimeTargetFactory.create(connectorBundle);
        return new PreparedExecutableScenarioPlan(
                preparedPlan,
                executablePlan,
                workloadArtifact,
                connectorBundle,
                runtimeTarget);
    }

    private static void validateInvocation(
            Path source,
            ObjectNode document,
            List<Diagnostic> issues) {
        requireEqualInteger(
                source,
                document.path("runs"),
                1,
                "$/runs",
                "runner.invocation.runs-unsupported",
                "The first runner requires runs=1",
                issues);
        requireEqualInteger(
                source,
                document.path("health_retry_limit"),
                0,
                "$/health_retry_limit",
                "runner.invocation.health-retries-unsupported",
                "The first runner requires health_retry_limit=0",
                issues);
    }

    private static void validateSetup(
            Path source,
            ObjectNode document,
            List<Diagnostic> issues) {
        JsonNode proxies = document.at("/setup/proxies");
        if (proxies.isObject() && !proxies.isEmpty()) {
            issues.add(issue(
                    source,
                    "runner.kafka.proxies-unsupported",
                    "$/setup/proxies",
                    "The first runner does not provision Kafka proxies"));
        }

        ObjectNode clusters = (ObjectNode) document.at("/setup/kafka/clusters");
        if (clusters.size() != 1) {
            issues.add(issue(
                    source,
                    "runner.kafka.cluster-count-unsupported",
                    "$/setup/kafka/clusters",
                    "The first runner requires exactly one Kafka cluster, found "
                            + clusters.size()));
        }
        if (!clusters.isEmpty()) {
            Map.Entry<String, JsonNode> first = clusters.fields().next();
            ObjectNode cluster = (ObjectNode) first.getValue();
            String clusterPath = "$/setup/kafka/clusters/" + pointer(first.getKey());
            String image = cluster.path("image").textValue();
            if (image == null || !KafkaBrokerImagePolicy.isSupportedV1(image)) {
                issues.add(issue(
                        source,
                        "runner.kafka.image-version-unsupported",
                        clusterPath + "/image",
                        "The first runner requires an official apache/kafka:4.0.x image"));
            }
            if (!"kraft".equals(cluster.path("mode").textValue())) {
                issues.add(issue(
                        source,
                        "runner.kafka.mode-unsupported",
                        clusterPath + "/mode",
                        "The first runner requires KRaft mode"));
            }
            requireEqualInteger(
                    source,
                    cluster.path("brokers"),
                    1,
                    clusterPath + "/brokers",
                    "runner.kafka.broker-count-unsupported",
                    "The first runner requires exactly one Kafka broker",
                    issues);
            ArrayNode topics = (ArrayNode) cluster.path("topics");
            if (topics.size() != 2) {
                issues.add(issue(
                        source,
                        "runner.kafka.topic-count-unsupported",
                        clusterPath + "/topics",
                        "The first runner requires exactly the input and output topics, found "
                                + topics.size()));
            }
            for (int index = 0; index < topics.size(); index++) {
                ObjectNode topic = (ObjectNode) topics.get(index);
                String topicPath = clusterPath + "/topics/" + index;
                requirePositiveInt(source, topic.path("partitions"),
                        topicPath + "/partitions", issues);
                requireEqualInteger(
                        source,
                        topic.path("replication_factor"),
                        1,
                        topicPath + "/replication_factor",
                        "runner.kafka.replication-factor-unsupported",
                        "A one-broker runner requires replication_factor=1",
                        issues);
                if (topic.get("input_source") instanceof ObjectNode input) {
                    if (input.has("connect_via_proxy")) {
                        issues.add(issue(
                                source,
                                "runner.kafka.proxy-route-unsupported",
                                topicPath + "/input_source/connect_via_proxy",
                                "The first runner does not route input through a proxy"));
                    }
                    if (!"generated".equals(input.path("mode").textValue())) {
                        issues.add(issue(
                                source,
                                "runner.input.mode-unsupported",
                                topicPath + "/input_source/mode",
                                "The first runner requires bounded generated input"));
                    }
                    if (!"integer-sequence".equals(input.path("format").textValue())) {
                        issues.add(issue(
                                source,
                                "runner.input.format-unsupported",
                                topicPath + "/input_source/format",
                                "The first runner requires integer-sequence input"));
                    }
                    requireSupportedInputTotal(
                            source,
                            input.path("total"),
                            topicPath + "/input_source/total",
                            issues);
                }
            }
        }

        ObjectNode flink = (ObjectNode) document.at("/setup/flink");
        requireEqualInteger(
                source,
                flink.path("jobmanagers"),
                1,
                "$/setup/flink/jobmanagers",
                "runner.flink.jobmanager-count-unsupported",
                "The first runner requires exactly one JobManager",
                issues);
        requireEqualInteger(
                source,
                flink.path("taskmanagers"),
                1,
                "$/setup/flink/taskmanagers",
                "runner.flink.taskmanager-count-unsupported",
                "The first runner requires exactly one TaskManager",
                issues);
        if (flink.get("config") instanceof ObjectNode config && !config.isEmpty()) {
            issues.add(issue(
                    source,
                    "runner.flink.config-unsupported",
                    "$/setup/flink/config",
                    "The first runner rejects Flink config entries it cannot map explicitly"));
        }
    }

    private static void validateWorkload(
            Path source,
            ObjectNode document,
            List<Diagnostic> issues) {
        ObjectNode connectors = (ObjectNode) document.at("/subject/connectors");
        if (connectors.size() != 1) {
            issues.add(issue(
                    source,
                    "runner.connector.count-unsupported",
                    "$/subject/connectors",
                    "The first runner requires exactly one registered connector, found "
                            + connectors.size()));
        }

        ArrayNode jobs = (ArrayNode) document.at("/workload/jobs");
        if (jobs.size() != 1) {
            issues.add(issue(
                    source,
                    "runner.workload.job-count-unsupported",
                    "$/workload/jobs",
                    "The first runner requires exactly one workload job, found " + jobs.size()));
        }
        if (jobs.isEmpty()) {
            return;
        }

        ObjectNode job = (ObjectNode) jobs.get(0);
        String jobPath = "$/workload/jobs/0";
        if (!"auto".equals(job.path("start").textValue())) {
            issues.add(issue(
                    source,
                    "runner.workload.manual-start-unsupported",
                    jobPath + "/start",
                    "The first runner starts its one job automatically"));
        }
        requireEqualInteger(
                source,
                job.path("parallelism"),
                1,
                jobPath + "/parallelism",
                "runner.workload.parallelism-unsupported",
                "The first runner requires job parallelism=1",
                issues);

        JsonNode jobConnectors = job.get("connectors");
        if (!(jobConnectors instanceof ArrayNode connectorArray)
                || connectorArray.size() != 1) {
            issues.add(issue(
                    source,
                    "runner.connector.registration-required",
                    jobPath + "/connectors",
                    "The first runner requires the job to register exactly one connector"));
        } else if (connectors.size() == 1
                && !connectors.has(connectorArray.get(0).textValue())) {
            issues.add(issue(
                    source,
                    "runner.connector.registration-mismatch",
                    jobPath + "/connectors/0",
                    "The job connector must name the sole subject connector"));
        }

        ObjectNode sourceEndpoint = (ObjectNode) job.get("source");
        ObjectNode sink = (ObjectNode) job.get("sink");
        if (sourceEndpoint.has("connect_via_proxy")) {
            issues.add(issue(
                    source,
                    "runner.kafka.proxy-route-unsupported",
                    jobPath + "/source/connect_via_proxy",
                    "The first runner does not route a job source through a proxy"));
        }
        if (sink.has("connect_via_proxy")) {
            issues.add(issue(
                    source,
                    "runner.kafka.proxy-route-unsupported",
                    jobPath + "/sink/connect_via_proxy",
                    "The first runner does not route a job sink through a proxy"));
        }
        if (sourceEndpoint.path("topic").textValue()
                .equals(sink.path("topic").textValue())) {
            issues.add(issue(
                    source,
                    "runner.workload.source-sink-topic-conflict",
                    jobPath + "/sink/topic",
                    "The bounded source and terminal sink topics must be distinct"));
        }

        SinkCompiler.validate(source, jobPath + "/sink", sink, issues);

        ObjectNode checkpointing = (ObjectNode) job.get("checkpointing");
        requireDuration(source, checkpointing.path("interval"),
                jobPath + "/checkpointing/interval", issues);
        if (!"EXACTLY_ONCE".equals(checkpointing.path("mode").textValue())) {
            issues.add(issue(
                    source,
                    "runner.workload.checkpoint-mode-unsupported",
                    jobPath + "/checkpointing/mode",
                    "The first runner requires EXACTLY_ONCE checkpointing"));
        }
        boolean hashmapStateBackend =
                "hashmap".equals(job.path("state_backend").asText("hashmap"));
        if (!hashmapStateBackend) {
            issues.add(issue(
                    source,
                    "runner.workload.state-backend-unsupported",
                    jobPath + "/state_backend",
                    "The first runner supports only the hashmap state backend"));
        }
        JsonNode restartStrategy = job.get("restart_strategy");
        if (restartStrategy instanceof ObjectNode restart
                && !"flink-default".equals(restart.path("type").textValue())) {
            issues.add(issue(
                    source,
                    "runner.workload.restart-strategy-unsupported",
                    jobPath + "/restart_strategy/type",
                    "The first runner supports only the flink-default restart strategy"));
        }
        JsonNode checkpointStorage = checkpointing.get("storage");
        if (checkpointStorage instanceof ObjectNode storage
                && !Set.of("jobmanager", "filesystem")
                        .contains(storage.path("type").textValue())) {
            issues.add(issue(
                    source,
                    "runner.workload.checkpoint-storage-unsupported",
                    jobPath + "/checkpointing/storage/type",
                    "The first runner supports only JobManager or filesystem "
                            + "checkpoint storage"));
        }
        validateStateTtl(
                source,
                job.get("state_ttl"),
                jobPath + "/state_ttl",
                hashmapStateBackend,
                issues);
        validateWatermarks(source, job.get("watermarks"), jobPath + "/watermarks", issues);
        validateProgramArguments(source, job.get("program_args"),
                jobPath + "/program_args", issues);
    }

    private static void validateStateTtl(
            Path source,
            JsonNode value,
            String path,
            boolean hashmapStateBackend,
            List<Diagnostic> issues) {
        if (!(value instanceof ObjectNode stateTtl) || !stateTtl.path("enabled").booleanValue()) {
            return;
        }
        requireDuration(source, stateTtl.path("ttl"), path + "/ttl", issues);
        String cleanup = stateTtl.path("cleanup").asText("none");
        if (!Set.of("none", "incremental", "rocksdb-compaction-filter").contains(cleanup)
                || (hashmapStateBackend && "rocksdb-compaction-filter".equals(cleanup))) {
            issues.add(issue(
                    source,
                    "runner.workload.state-ttl-cleanup-unsupported",
                    path + "/cleanup",
                    "State TTL cleanup strategy '" + cleanup
                            + "' is unsupported with the first runner's hashmap backend"));
        }
    }

    private static void validateWatermarks(
            Path source,
            JsonNode value,
            String path,
            List<Diagnostic> issues) {
        if (!(value instanceof ObjectNode watermarks)) {
            return;
        }
        String strategy = watermarks.path("strategy").textValue();
        if (!Set.of(
                        "no-watermarks",
                        "monotonic-timestamps",
                        "bounded-out-of-orderness")
                .contains(strategy)) {
            issues.add(issue(
                    source,
                    "runner.workload.watermarks-unsupported",
                    path + "/strategy",
                    "Unsupported watermark strategy '" + strategy + "'"));
            return;
        }
        if (watermarks.has("max_out_of_orderness")) {
            requireDuration(source, watermarks.path("max_out_of_orderness"),
                    path + "/max_out_of_orderness", issues);
        }
        if (watermarks.has("idleness")) {
            requireDuration(source, watermarks.path("idleness"),
                    path + "/idleness", issues);
        }
    }

    private static void validateProgramArguments(
            Path source,
            JsonNode value,
            String path,
            List<Diagnostic> issues) {
        if (value == null) {
            return;
        }
        ArrayNode arguments = (ArrayNode) value;
        if (arguments.isEmpty()) {
            return;
        }
        for (JsonNode argument : arguments) {
            String token = argument.textValue();
            if (token.startsWith(RESERVED_WORKLOAD_ARGUMENT_PREFIX)
                    || token.equals(FORBIDDEN_BOOTSTRAP_OVERRIDE_ARGUMENT)
                    || token.startsWith(FORBIDDEN_BOOTSTRAP_OVERRIDE_ARGUMENT + "=")) {
                issues.add(issue(
                        source,
                        "runner.workload.program-args-conflict",
                        path,
                        "program_args must not override harness-owned workload settings; "
                                + "conflicting token '" + token + "'"));
                return;
            }
        }
    }

    private static void validatePhases(
            Path source,
            ObjectNode document,
            List<Diagnostic> issues) {
        ArrayNode phases = (ArrayNode) document.get("phases");
        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            ObjectNode phase = (ObjectNode) phases.get(phaseIndex);
            validateSteps(
                    source,
                    (ArrayNode) phase.get("steps"),
                    "$/phases/" + phaseIndex + "/steps",
                    document.at("/workload/jobs/0/alias").textValue(),
                    issues);
        }
        validateTaskManagerLifecycle(source, phases, issues);
    }

    private static void validateSteps(
            Path source,
            ArrayNode steps,
            String stepsPath,
            String jobAlias,
            List<Diagnostic> issues) {
        for (int index = 0; index < steps.size(); index++) {
            ObjectNode step = (ObjectNode) steps.get(index);
            String stepPath = stepsPath + "/" + index;
            String key = step.fieldNames().next();
            if (!SUPPORTED_STEP_KEYS.contains(key)) {
                issues.add(issue(
                        source,
                        "runner.phase.step-unsupported",
                        stepPath + "/" + pointer(key),
                        "The first runner does not execute '" + key + "' steps"));
                continue;
            }
            switch (key) {
                case "await" -> validateAwait(
                        source, (ObjectNode) step.get(key), stepPath + "/await", jobAlias, issues);
                case "wait" -> requireDuration(
                        source,
                        step.at("/wait/duration"),
                        stepPath + "/wait/duration",
                        issues);
                case "loop" -> {
                    ObjectNode loop = (ObjectNode) step.get("loop");
                    requirePositiveInt(source, loop.path("times"),
                            stepPath + "/loop/times", issues);
                    validateSteps(
                            source,
                            (ArrayNode) loop.get("steps"),
                            stepPath + "/loop/steps",
                            jobAlias,
                            issues);
                }
                case "kill" -> validateKill(source, (ObjectNode) step.get("kill"),
                        stepPath + "/kill", issues);
                case "restart" -> validateRestart(source, (ObjectNode) step.get("restart"),
                        stepPath + "/restart", issues);
                default -> throw new IllegalStateException("Unexpected supported step " + key);
            }
        }
    }

    private static void validateAwait(
            Path source,
            ObjectNode await,
            String path,
            String jobAlias,
            List<Diagnostic> issues) {
        requireDuration(source, await.path("timeout"), path + "/timeout", issues);
        ObjectNode condition = (ObjectNode) await.get("condition");
        String type = condition.path("type").textValue();
        if (!Set.of("job-state", "checkpoint-completed").contains(type)) {
            issues.add(issue(
                    source,
                    "runner.phase.await-condition-unsupported",
                    path + "/condition/type",
                    "The first runner awaits only job-state and checkpoint-completed"));
            return;
        }
        if (!jobAlias.equals(condition.path("job").textValue())) {
            issues.add(issue(
                    source,
                    "runner.phase.await-job-unsupported",
                    path + "/condition/job",
                    "Await must target the one compiled workload job '" + jobAlias + "'"));
        }
        if ("job-state".equals(type)
                && !"RUNNING".equals(condition.path("state").textValue())) {
            issues.add(issue(
                    source,
                    "runner.phase.job-state-unsupported",
                    path + "/condition/state",
                    "The first runner can await only the RUNNING state"));
        }
        if ("checkpoint-completed".equals(type)) {
            requirePositiveLong(
                    source,
                    condition.path("count"),
                    path + "/condition/count",
                    issues);
        }
    }

    private static void validateKill(
            Path source,
            ObjectNode kill,
            String path,
            List<Diagnostic> issues) {
        ObjectNode target = (ObjectNode) kill.get("target");
        if (!"named".equals(target.path("kind").textValue())
                || !"taskmanager".equals(target.path("role").textValue())
                || !"taskmanager-1".equals(target.path("name").textValue())) {
            issues.add(issue(
                    source,
                    "runner.phase.kill-target-unsupported",
                    path + "/target",
                    "The first runner can kill only named taskmanager-1"));
        }
    }

    private static void validateRestart(
            Path source,
            ObjectNode restart,
            String path,
            List<Diagnostic> issues) {
        if (!"taskmanager".equals(restart.path("component").textValue())) {
            issues.add(issue(
                    source,
                    "runner.phase.restart-component-unsupported",
                    path + "/component",
                    "The first runner can restart only the TaskManager"));
        }
        if (restart.has("image")) {
            issues.add(issue(
                    source,
                    "runner.phase.restart-image-unsupported",
                    path + "/image",
                    "The first runner restarts the current verified runtime target"));
        }
    }

    private static void validateTaskManagerLifecycle(
            Path source,
            ArrayNode phases,
            List<Diagnostic> issues) {
        TaskManagerState state = TaskManagerState.active();
        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            ObjectNode phase = (ObjectNode) phases.get(phaseIndex);
            state = validateTaskManagerLifecycleInSteps(
                    source,
                    (ArrayNode) phase.get("steps"),
                    "$/phases/" + phaseIndex + "/steps",
                    state,
                    issues);
        }
        if (!state.running()) {
            issues.add(issue(
                    source,
                    "runner.phase.taskmanager-kill-unhealed",
                    state.lastKillPath(),
                    "A killed TaskManager must have a reachable taskmanager restart"));
        }
    }

    private static TaskManagerState validateTaskManagerLifecycleInSteps(
            Path source,
            ArrayNode steps,
            String stepsPath,
            TaskManagerState initial,
            List<Diagnostic> issues) {
        TaskManagerState state = initial;
        for (int index = 0; index < steps.size(); index++) {
            ObjectNode step = (ObjectNode) steps.get(index);
            String stepPath = stepsPath + "/" + index;
            if (step.has("kill") && isSupportedTaskManagerKill((ObjectNode) step.get("kill"))) {
                if (!state.running()) {
                    issues.add(issue(
                            source,
                            "runner.phase.taskmanager-already-stopped",
                            stepPath + "/kill",
                            "Cannot kill taskmanager-1 before it has been restarted"));
                } else {
                    state = TaskManagerState.stopped(stepPath + "/kill");
                }
            } else if (step.has("restart")
                    && isSupportedTaskManagerRestart((ObjectNode) step.get("restart"))) {
                if (state.running()) {
                    issues.add(issue(
                            source,
                            "runner.phase.taskmanager-already-running",
                            stepPath + "/restart",
                            "TaskManager restart must heal a preceding kill"));
                } else {
                    state = TaskManagerState.active();
                }
            } else if (step.get("loop") instanceof ObjectNode loop) {
                TaskManagerState before = state;
                state = validateTaskManagerLifecycleInSteps(
                        source,
                        (ArrayNode) loop.get("steps"),
                        stepPath + "/loop/steps",
                        state,
                        issues);
                if (loop.path("times").bigIntegerValue().compareTo(BigInteger.ONE) > 0
                        && !sameLifecycleState(before, state)) {
                    issues.add(issue(
                            source,
                            "runner.phase.loop-taskmanager-lifecycle-unstable",
                            stepPath + "/loop",
                            "A repeated loop must restore taskmanager-1 to its entry state"));
                }
            }
        }
        return state;
    }

    private static boolean isSupportedTaskManagerKill(ObjectNode kill) {
        ObjectNode target = (ObjectNode) kill.get("target");
        return "named".equals(target.path("kind").textValue())
                && "taskmanager".equals(target.path("role").textValue())
                && "taskmanager-1".equals(target.path("name").textValue());
    }

    private static boolean isSupportedTaskManagerRestart(ObjectNode restart) {
        return "taskmanager".equals(restart.path("component").textValue())
                && !restart.has("image");
    }

    private static boolean sameLifecycleState(TaskManagerState left, TaskManagerState right) {
        return left.running() == right.running();
    }

    private static void validateTerminalValidation(
            Path source,
            ObjectNode document,
            List<Diagnostic> issues) {
        ArrayNode validators = (ArrayNode) document.get("terminal_validations");
        if (validators.size() != 1) {
            issues.add(issue(
                    source,
                    "runner.validation.count-unsupported",
                    "$/terminal_validations",
                    "The first runner requires exactly one terminal validator, found "
                            + validators.size()));
        }
        if (validators.isEmpty()) {
            return;
        }
        ObjectNode validator = (ObjectNode) validators.get(0);
        if (!KAFKA_ID_SET.equals(validator.path("type").textValue())) {
            issues.add(issue(
                    source,
                    "runner.validation.type-unsupported",
                    "$/terminal_validations/0/type",
                    "The first runner supports only terminal kafka.id-set validation"));
        }
        if (!"input-manifest".equals(validator.path("expected").textValue())) {
            issues.add(issue(
                    source,
                    "runner.validation.expected-unsupported",
                    "$/terminal_validations/0/expected",
                    "kafka.id-set must compare against input-manifest"));
        }
        if (validator.has("timeout")) {
            requireDuration(
                    source,
                    validator.path("timeout"),
                    "$/terminal_validations/0/timeout",
                    issues);
        }
        if (document.has("completion_timeout")) {
            requireDuration(source, document.path("completion_timeout"),
                    "$/completion_timeout", issues);
        }
    }

    private static ExecutableScenarioPlan map(
            ResolvedScenarioPlan sourcePlan,
            ObjectNode document) {
        ObjectNode clusters = (ObjectNode) document.at("/setup/kafka/clusters");
        Map.Entry<String, JsonNode> clusterEntry = clusters.fields().next();
        String clusterAlias = clusterEntry.getKey();
        ObjectNode clusterNode = (ObjectNode) clusterEntry.getValue();
        ArrayNode topicNodes = (ArrayNode) clusterNode.get("topics");
        List<ExecutableScenarioPlan.KafkaTopic> topics = new ArrayList<>(topicNodes.size());
        ObjectNode inputNode = null;
        ObjectNode inputTopicNode = null;
        for (JsonNode value : topicNodes) {
            ObjectNode topic = (ObjectNode) value;
            topics.add(new ExecutableScenarioPlan.KafkaTopic(
                    topic.path("name").textValue(),
                    topic.path("partitions").intValue(),
                    topic.path("replication_factor").intValue()));
            if (topic.get("input_source") instanceof ObjectNode input) {
                inputNode = input;
                inputTopicNode = topic;
            }
        }
        Objects.requireNonNull(inputNode, "validated input source");
        Objects.requireNonNull(inputTopicNode, "validated input topic");

        ExecutableScenarioPlan.KafkaCluster kafka = new ExecutableScenarioPlan.KafkaCluster(
                clusterAlias,
                clusterNode.path("image").textValue(),
                ExecutableScenarioPlan.KafkaMode.KRAFT,
                1,
                KafkaBrokerPolicy.v1SingleBroker(),
                topics);
        ObjectNode flinkNode = (ObjectNode) document.at("/setup/flink");
        ExecutableScenarioPlan.FlinkCluster flink = new ExecutableScenarioPlan.FlinkCluster(
                flinkNode.path("image").textValue(), 1, 1);
        ExecutableScenarioPlan.GeneratedIntegerSequenceInput input =
                new ExecutableScenarioPlan.GeneratedIntegerSequenceInput(
                        clusterAlias,
                        inputTopicNode.path("name").textValue(),
                        inputTopicNode.path("partitions").intValue(),
                        inputNode.path("total").longValue());

        ObjectNode jobNode = (ObjectNode) document.at("/workload/jobs/0");
        String scenarioName = document.at("/meta/name").textValue();
        String jobAlias = jobNode.path("alias").textValue();
        ExecutableScenarioPlan.TopicReference source = topicReference(
                (ObjectNode) jobNode.get("source"));
        ObjectNode sinkNode = (ObjectNode) jobNode.get("sink");
        ExecutableScenarioPlan.Sink sink = SinkCompiler.map(sinkNode, topicReference(sinkNode));
        ExecutableScenarioPlan.StateTtl stateTtl = mapStateTtl(jobNode.get("state_ttl"));
        ExecutableScenarioPlan.Watermarks watermarks = mapWatermarks(jobNode.get("watermarks"));
        ExecutableScenarioPlan.RunScopedIdentityPolicy identityPolicy =
                new ExecutableScenarioPlan.RunScopedIdentityPolicy(scenarioName, jobAlias);
        ExecutableScenarioPlan.WorkloadConfiguration workloadConfiguration =
                new ExecutableScenarioPlan.WorkloadConfiguration(
                        jobAlias,
                        source,
                        input.partitions(),
                        input.totalRecords(),
                        sink,
                        stateTtl,
                        watermarks,
                        identityPolicy,
                        SinkCompiler.transactionTimeout(sinkNode));
        ExecutableScenarioPlan.Checkpointing checkpointing =
                new ExecutableScenarioPlan.Checkpointing(
                        parseDuration(jobNode.at("/checkpointing/interval").textValue()),
                        ExecutableScenarioPlan.CheckpointMode.EXACTLY_ONCE,
                        "filesystem".equals(jobNode.at("/checkpointing/storage/type")
                                        .asText("jobmanager"))
                                ? ExecutableScenarioPlan.CheckpointStorage.FILESYSTEM
                                : ExecutableScenarioPlan.CheckpointStorage.JOBMANAGER);
        Map<String, String> standardFlinkConfiguration =
                ExecutableScenarioPlan.standardFlinkConfiguration(
                        1,
                        ExecutableScenarioPlan.StateBackend.HASHMAP,
                        checkpointing);
        ExecutableScenarioPlan.Job job = new ExecutableScenarioPlan.Job(
                jobAlias,
                jobNode.path("jar").textValue(),
                jobNode.withArray("connectors").get(0).textValue(),
                ExecutableScenarioPlan.StartMode.AUTO,
                1,
                source,
                sink,
                ExecutableScenarioPlan.StateBackend.HASHMAP,
                checkpointing,
                ExecutableScenarioPlan.RestartStrategy.FLINK_DEFAULT,
                stateTtl,
                watermarks,
                mapProgramArguments(jobNode.get("program_args")),
                workloadConfiguration,
                standardFlinkConfiguration);

        List<ExecutableScenarioPlan.Phase> phases = mapPhases(
                (ArrayNode) document.get("phases"));
        ObjectNode validationNode = (ObjectNode) document.at("/terminal_validations/0");
        Duration terminalTimeout = validationNode.has("timeout")
                ? parseDuration(validationNode.path("timeout").textValue())
                : ExecutableScenarioPlan.DEFAULT_TERMINAL_VALIDATION_TIMEOUT;
        ExecutableScenarioPlan.KafkaIdSetValidation validation =
                new ExecutableScenarioPlan.KafkaIdSetValidation(
                        topicReference(validationNode),
                        ExecutableScenarioPlan.ExpectedRecords.INPUT_MANIFEST,
                        terminalTimeout);
        Duration completionTimeout = document.has("completion_timeout")
                ? parseDuration(document.path("completion_timeout").textValue())
                : ExecutableScenarioPlan.DEFAULT_JOB_COMPLETION_TIMEOUT;
        return new ExecutableScenarioPlan(
                sourcePlan,
                scenarioName,
                new ExecutableScenarioPlan.InvocationPolicy(1, 0),
                kafka,
                flink,
                input,
                job,
                phases,
                validation,
                ExpectationCompiler.map(sourcePlan),
                completionTimeout);
    }

    private static ExecutableScenarioPlan.TopicReference topicReference(ObjectNode node) {
        return new ExecutableScenarioPlan.TopicReference(
                node.path("cluster").textValue(), node.path("topic").textValue());
    }

    private static ExecutableScenarioPlan.StateTtl mapStateTtl(JsonNode value) {
        if (!(value instanceof ObjectNode stateTtl) || !stateTtl.path("enabled").booleanValue()) {
            return ExecutableScenarioPlan.StateTtl.disabled();
        }
        String cleanup = stateTtl.path("cleanup").asText("none");
        return ExecutableScenarioPlan.StateTtl.enabled(
                parseDuration(stateTtl.path("ttl").textValue()),
                switch (cleanup) {
                    case "none" -> ExecutableScenarioPlan.StateTtlCleanup.NONE;
                    case "incremental" -> ExecutableScenarioPlan.StateTtlCleanup.INCREMENTAL;
                    case "rocksdb-compaction-filter" ->
                            ExecutableScenarioPlan.StateTtlCleanup.ROCKSDB_COMPACTION_FILTER;
                    default -> throw new IllegalStateException("Validated cleanup " + cleanup);
                });
    }

    private static ExecutableScenarioPlan.Watermarks mapWatermarks(JsonNode value) {
        if (!(value instanceof ObjectNode watermarks)) {
            return ExecutableScenarioPlan.Watermarks.noWatermarks();
        }
        ExecutableScenarioPlan.WatermarkStrategy strategy = switch (
                watermarks.path("strategy").textValue()) {
            case "no-watermarks" -> ExecutableScenarioPlan.WatermarkStrategy.NO_WATERMARKS;
            case "monotonic-timestamps" ->
                    ExecutableScenarioPlan.WatermarkStrategy.MONOTONIC_TIMESTAMPS;
            case "bounded-out-of-orderness" ->
                    ExecutableScenarioPlan.WatermarkStrategy.BOUNDED_OUT_OF_ORDERNESS;
            default -> throw new IllegalStateException("Validated watermark strategy");
        };
        Optional<Duration> outOfOrderness = watermarks.has("max_out_of_orderness")
                ? Optional.of(parseDuration(
                        watermarks.path("max_out_of_orderness").textValue()))
                : Optional.empty();
        Optional<Duration> idleness = watermarks.has("idleness")
                ? Optional.of(parseDuration(watermarks.path("idleness").textValue()))
                : Optional.empty();
        return new ExecutableScenarioPlan.Watermarks(strategy, outOfOrderness, idleness);
    }

    private static ExecutableScenarioPlan.ProgramArguments mapProgramArguments(JsonNode value) {
        if (!(value instanceof ArrayNode arguments) || arguments.isEmpty()) {
            return new ExecutableScenarioPlan.ProgramArguments(List.of());
        }
        List<String> values = new ArrayList<>(arguments.size());
        arguments.forEach(argument -> values.add(argument.textValue()));
        return new ExecutableScenarioPlan.ProgramArguments(values);
    }

    private static List<ExecutableScenarioPlan.Phase> mapPhases(ArrayNode phaseNodes) {
        List<ExecutableScenarioPlan.Phase> phases = new ArrayList<>(phaseNodes.size());
        for (JsonNode value : phaseNodes) {
            ObjectNode phase = (ObjectNode) value;
            phases.add(new ExecutableScenarioPlan.Phase(
                    phase.path("name").textValue(),
                    mapSteps((ArrayNode) phase.get("steps"))));
        }
        return List.copyOf(phases);
    }

    private static List<ExecutableScenarioPlan.Step> mapSteps(ArrayNode stepNodes) {
        List<ExecutableScenarioPlan.Step> steps = new ArrayList<>(stepNodes.size());
        for (JsonNode value : stepNodes) {
            ObjectNode step = (ObjectNode) value;
            if (step.get("await") instanceof ObjectNode await) {
                ObjectNode condition = (ObjectNode) await.get("condition");
                ExecutableScenarioPlan.TimeoutOutcome onTimeout =
                        ExecutableScenarioPlan.TimeoutOutcome.valueOf(
                                await.path("on_timeout").textValue().toUpperCase(Locale.ROOT));
                if ("job-state".equals(condition.path("type").textValue())) {
                    steps.add(new ExecutableScenarioPlan.AwaitJobState(
                            condition.path("job").textValue(),
                            ExecutableScenarioPlan.JobState.RUNNING,
                            parseDuration(await.path("timeout").textValue()),
                            onTimeout));
                } else {
                    steps.add(new ExecutableScenarioPlan.AwaitCheckpoints(
                            condition.path("job").textValue(),
                            condition.path("count").longValue(),
                            parseDuration(await.path("timeout").textValue()),
                            onTimeout));
                }
            } else if (step.get("wait") instanceof ObjectNode wait) {
                steps.add(new ExecutableScenarioPlan.Wait(
                        parseDuration(wait.path("duration").textValue())));
            } else if (step.get("kill") instanceof ObjectNode kill) {
                steps.add(new ExecutableScenarioPlan.KillTaskManager(
                        kill.at("/target/name").textValue()));
            } else if (step.has("restart")) {
                steps.add(new ExecutableScenarioPlan.RestartTaskManager());
            } else if (step.get("loop") instanceof ObjectNode loop) {
                steps.add(new ExecutableScenarioPlan.Loop(
                        loop.path("times").intValue(),
                        mapSteps((ArrayNode) loop.get("steps"))));
            } else {
                throw new IllegalStateException("Unsupported step escaped capability validation");
            }
        }
        return List.copyOf(steps);
    }

    private static void validateWorkloadProtocol(
            PreparedScenarioPlan preparedPlan,
            ResolvedArtifact artifact,
            String path) {
        Path jar = artifact.preparedPath();
        try {
            if (!Files.isRegularFile(jar, LinkOption.NOFOLLOW_LINKS) || !Files.isReadable(jar)) {
                throw new IOException("prepared workload JAR is unavailable");
            }
            String actualSha256 = Digests.sha256(jar, LinkOption.NOFOLLOW_LINKS);
            if (!artifact.sha256().equals(actualSha256)) {
                throw capability(
                        preparedPlan,
                        "runner.workload.artifact-changed",
                        path,
                        "Prepared workload JAR changed after preparation; expected SHA-256 "
                                + artifact.sha256() + ", actual " + actualSha256);
            }
            WorkloadProtocolArtifactValidator.validate(jar);
        } catch (SpecificationException exception) {
            throw exception;
        } catch (WorkloadProtocolArtifactValidator.ValidationException exception) {
            throw capability(
                    preparedPlan,
                    "runner.workload.protocol-" + exception.kind().diagnosticSuffix(),
                    path,
                    exception.getMessage());
        } catch (IOException | RuntimeException exception) {
            throw capability(
                    preparedPlan,
                    "runner.workload.protocol-unreadable",
                    path,
                    "Cannot inspect prepared workload protocol marker: "
                            + safeMessage(exception));
        }
    }

    private static SpecificationException capability(
            PreparedScenarioPlan plan,
            String code,
            String path,
            String message) {
        return new SpecificationException(Stage.RUNNER_CAPABILITY, List.of(issue(
                plan.scenarioPlan().scenario().template().source(), code, path, message)));
    }

    private static void requireEqualInteger(
            Path source,
            JsonNode value,
            int expected,
            String path,
            String code,
            String message,
            List<Diagnostic> issues) {
        if (!BigInteger.valueOf(expected).equals(value.bigIntegerValue())) {
            issues.add(issue(source, code, path, message + ", found " + value));
        }
    }

    private static void requirePositiveInt(
            Path source,
            JsonNode value,
            String path,
            List<Diagnostic> issues) {
        BigInteger integer = value.bigIntegerValue();
        if (integer.signum() < 1 || integer.compareTo(INT_MAX) > 0) {
            issues.add(issue(
                    source,
                    "runner.value.integer-out-of-range",
                    path,
                    "Runner integer must be between 1 and " + Integer.MAX_VALUE));
        }
    }

    private static void requirePositiveLong(
            Path source,
            JsonNode value,
            String path,
            List<Diagnostic> issues) {
        BigInteger integer = value.bigIntegerValue();
        if (integer.signum() < 1 || integer.compareTo(LONG_MAX) > 0) {
            issues.add(issue(
                    source,
                    "runner.value.long-out-of-range",
                    path,
                    "Runner count must be between 1 and " + Long.MAX_VALUE));
        }
    }

    private static void requireSupportedInputTotal(
            Path source,
            JsonNode value,
            String path,
            List<Diagnostic> issues) {
        BigInteger total = value.bigIntegerValue();
        if (total.signum() < 1
                || total.compareTo(BigInteger.valueOf(
                        ExecutableScenarioPlan
                                .FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS)) > 0) {
            issues.add(issue(
                    source,
                    "runner.input.total-unsupported",
                    path,
                    "The current in-memory first runner supports generated input totals "
                            + "between 1 and "
                            + ExecutableScenarioPlan
                                    .FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS));
        }
    }

    static void requireDuration(
            Path source,
            JsonNode value,
            String path,
            List<Diagnostic> issues) {
        if (!value.isTextual()) {
            issues.add(issue(
                    source,
                    "runner.value.duration-unsupported",
                    path,
                    "Runner duration must be a resolved duration string"));
            return;
        }
        Matcher matcher = DURATION.matcher(value.textValue());
        if (!matcher.matches()) {
            issues.add(issue(
                    source,
                    "runner.value.duration-unsupported",
                    path,
                    "Runner duration must use positive integer ms/s/m/h syntax"));
            return;
        }
        BigInteger multiplier = switch (matcher.group(2)) {
            case "ms" -> BigInteger.ONE;
            case "s" -> BigInteger.valueOf(1_000);
            case "m" -> BigInteger.valueOf(60_000);
            case "h" -> BigInteger.valueOf(3_600_000);
            default -> throw new IllegalStateException("Validated duration unit");
        };
        if (new BigInteger(matcher.group(1)).multiply(multiplier).compareTo(LONG_MAX) > 0) {
            issues.add(issue(
                    source,
                    "runner.value.duration-out-of-range",
                    path,
                    "Resolved duration exceeds the runner millisecond range"));
        }
    }

    static Duration parseDuration(String value) {
        Matcher matcher = DURATION.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalStateException("Invalid resolved duration " + value);
        }
        long amount = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "ms" -> Duration.ofMillis(amount);
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            case "h" -> Duration.ofHours(amount);
            default -> throw new IllegalStateException("Validated duration unit");
        };
    }

    private static Diagnostic issue(
            Path source,
            String code,
            String path,
            String message) {
        return issue(source, ResolutionScope.SINGLE, code, path, message);
    }

    private static Diagnostic issue(
            Path source,
            ResolutionScope scope,
            String code,
            String path,
            String message) {
        return new Diagnostic(source, scope, code, path, message);
    }

    private static String safeMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank()
                ? exception.getClass().getSimpleName()
                : message;
    }

    private static String pointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }

    private record TaskManagerState(boolean running, String lastKillPath) {
        private static TaskManagerState active() {
            return new TaskManagerState(true, null);
        }

        private static TaskManagerState stopped(String killPath) {
            return new TaskManagerState(false, killPath);
        }
    }
}
