package org.savonitar.flink.stability.core.execution.plan;

import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Immutable, fully typed description of the subset a first-version runner can execute.
 *
 * <p>The source document is retained only as provenance. Runtime code consumes these typed
 * values and must not reinterpret the source {@code JsonNode}.</p>
 */
public final class ExecutableScenarioPlan {
    public static final Duration DEFAULT_JOB_COMPLETION_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration DEFAULT_TERMINAL_VALIDATION_TIMEOUT = Duration.ofMinutes(2);
    /** The Kafka connector classes every protocol-v1 workload builds its job from (R5.6d). */
    public static final List<String> PROTOCOL_V1_SUBJECT_ENTRY_CLASSES = List.of(
            "org.apache.flink.connector.kafka.source.KafkaSource",
            "org.apache.flink.connector.kafka.sink.KafkaSink");
    /** The exactly-once transaction timeout when a scenario declares none. */
    public static final Duration KAFKA_TRANSACTION_TIMEOUT = Duration.ofHours(2);
    public static final long FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS = 1_000_000L;

    private final ResolvedScenarioPlan sourcePlan;
    private final String scenarioName;
    private final InvocationPolicy invocation;
    private final KafkaCluster kafka;
    private final FlinkCluster flink;
    private final GeneratedIntegerSequenceInput input;
    private final Job job;
    private final List<Phase> phases;
    private final KafkaIdSetValidation terminalValidation;
    private final ExpectedOutcome expectedOutcome;
    private final Duration jobCompletionTimeout;

    ExecutableScenarioPlan(
            ResolvedScenarioPlan sourcePlan,
            String scenarioName,
            InvocationPolicy invocation,
            KafkaCluster kafka,
            FlinkCluster flink,
            GeneratedIntegerSequenceInput input,
            Job job,
            List<Phase> phases,
            KafkaIdSetValidation terminalValidation,
            ExpectedOutcome expectedOutcome,
            Duration jobCompletionTimeout) {
        this.sourcePlan = Objects.requireNonNull(sourcePlan, "sourcePlan");
        this.scenarioName = requireNonBlank(scenarioName, "scenarioName");
        this.invocation = Objects.requireNonNull(invocation, "invocation");
        this.kafka = Objects.requireNonNull(kafka, "kafka");
        this.flink = Objects.requireNonNull(flink, "flink");
        this.input = Objects.requireNonNull(input, "input");
        this.job = Objects.requireNonNull(job, "job");
        this.phases = List.copyOf(Objects.requireNonNull(phases, "phases"));
        if (this.phases.isEmpty()) {
            throw new IllegalArgumentException("phases must not be empty");
        }
        this.terminalValidation = Objects.requireNonNull(
                terminalValidation, "terminalValidation");
        this.expectedOutcome = Objects.requireNonNull(expectedOutcome, "expectedOutcome");
        this.jobCompletionTimeout = requirePositive(
                jobCompletionTimeout, "jobCompletionTimeout");
        if (!kafka.alias().equals(input.cluster())
                || !job.source().equals(new TopicReference(input.cluster(), input.topic()))) {
            throw new IllegalArgumentException("Input manifest must describe the job source");
        }
        if (!kafka.alias().equals(job.sink().topic().cluster())
                || job.source().equals(job.sink().topic())) {
            throw new IllegalArgumentException(
                    "Job source and sink must be distinct topics in the one Kafka cluster");
        }
        Map<String, KafkaTopic> topicIndex = new TreeMap<>();
        kafka.topics().forEach(topic -> topicIndex.put(topic.name(), topic));
        KafkaTopic inputTopic = topicIndex.get(input.topic());
        if (inputTopic == null || inputTopic.partitions() != input.partitions()
                || !topicIndex.containsKey(job.sink().topic().topic())) {
            throw new IllegalArgumentException(
                    "Input and sink references must match the declared Kafka topics");
        }
        if (job.workloadConfiguration().sourcePartitions() != input.partitions()
                || job.workloadConfiguration().expectedInputRecords()
                        != input.totalRecords()) {
            throw new IllegalArgumentException(
                    "Workload bounds must match the generated input manifest");
        }
        if (!job.sink().topic().equals(terminalValidation.output())) {
            throw new IllegalArgumentException("Terminal validation must inspect the job sink");
        }
    }

    public ResolvedScenarioPlan sourcePlan() {
        return sourcePlan;
    }

    public String scenarioName() {
        return scenarioName;
    }

    public InvocationPolicy invocation() {
        return invocation;
    }

    public KafkaCluster kafka() {
        return kafka;
    }

    public FlinkCluster flink() {
        return flink;
    }

    public GeneratedIntegerSequenceInput input() {
        return input;
    }

    public Job job() {
        return job;
    }

    public List<Phase> phases() {
        return phases;
    }

    public KafkaIdSetValidation terminalValidation() {
        return terminalValidation;
    }

    public ExpectedOutcome expectedOutcome() {
        return expectedOutcome;
    }

    public Duration jobCompletionTimeout() {
        return jobCompletionTimeout;
    }

    public record InvocationPolicy(int runs, int healthRetryLimit) {
        public InvocationPolicy {
            if (runs != 1 || healthRetryLimit != 0) {
                throw new IllegalArgumentException(
                        "The first executable boundary requires runs=1 and healthRetryLimit=0");
            }
        }
    }

    public record KafkaCluster(
            String alias,
            String imageReference,
            KafkaMode mode,
            int brokers,
            KafkaBrokerPolicy brokerPolicy,
            List<KafkaTopic> topics) {
        public KafkaCluster {
            alias = requireNonBlank(alias, "alias");
            imageReference = requireNonBlank(imageReference, "imageReference");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(brokerPolicy, "brokerPolicy");
            if (brokers != 1) {
                throw new IllegalArgumentException("The first executable boundary requires one broker");
            }
            topics = List.copyOf(Objects.requireNonNull(topics, "topics"));
            if (topics.size() != 2) {
                throw new IllegalArgumentException("The first executable boundary requires two topics");
            }
            if (topics.stream().map(KafkaTopic::name).distinct().count() != topics.size()) {
                throw new IllegalArgumentException("Kafka topic names must be distinct");
            }
        }
    }

    public enum KafkaMode {
        KRAFT
    }

    public record KafkaTopic(String name, int partitions, int replicationFactor) {
        public KafkaTopic {
            name = requireNonBlank(name, "name");
            if (partitions < 1) {
                throw new IllegalArgumentException("partitions must be positive");
            }
            if (replicationFactor != 1) {
                throw new IllegalArgumentException(
                        "The first executable boundary requires replicationFactor=1");
            }
        }
    }

    public record FlinkCluster(
            String imageReference,
            int jobmanagers,
            int taskmanagers) {
        public FlinkCluster {
            imageReference = requireNonBlank(imageReference, "imageReference");
            if (jobmanagers != 1 || taskmanagers != 1) {
                throw new IllegalArgumentException(
                        "The first executable boundary requires one JobManager and one TaskManager");
            }
        }
    }

    public record GeneratedIntegerSequenceInput(
            String cluster,
            String topic,
            int partitions,
            long totalRecords) {
        public GeneratedIntegerSequenceInput {
            cluster = requireNonBlank(cluster, "cluster");
            topic = requireNonBlank(topic, "topic");
            if (partitions < 1 || totalRecords < 1) {
                throw new IllegalArgumentException(
                        "Generated input partitions and totalRecords must be positive");
            }
        }
    }

    public record Job(
            String alias,
            String jarReference,
            String connectorAlias,
            StartMode startMode,
            int parallelism,
            TopicReference source,
            Sink sink,
            StateBackend stateBackend,
            Checkpointing checkpointing,
            RestartStrategy restartStrategy,
            StateTtl stateTtl,
            Watermarks watermarks,
            ProgramArguments programArguments,
            WorkloadConfiguration workloadConfiguration,
            Map<String, String> standardFlinkConfiguration) {
        public Job {
            alias = requireNonBlank(alias, "alias");
            jarReference = requireNonBlank(jarReference, "jarReference");
            connectorAlias = requireNonBlank(connectorAlias, "connectorAlias");
            Objects.requireNonNull(startMode, "startMode");
            if (parallelism != 1) {
                throw new IllegalArgumentException(
                        "The first executable boundary requires job parallelism=1");
            }
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(sink, "sink");
            Objects.requireNonNull(stateBackend, "stateBackend");
            Objects.requireNonNull(checkpointing, "checkpointing");
            Objects.requireNonNull(restartStrategy, "restartStrategy");
            Objects.requireNonNull(stateTtl, "stateTtl");
            Objects.requireNonNull(watermarks, "watermarks");
            Objects.requireNonNull(programArguments, "programArguments");
            Objects.requireNonNull(workloadConfiguration, "workloadConfiguration");
            standardFlinkConfiguration = immutableSortedMap(
                    standardFlinkConfiguration, "standardFlinkConfiguration");
            if (!alias.equals(workloadConfiguration.jobAlias())
                    || !source.equals(workloadConfiguration.source())
                    || !sink.equals(workloadConfiguration.sink())) {
                throw new IllegalArgumentException(
                        "Workload configuration must match its enclosing job");
            }
            Map<String, String> expectedStandardConfiguration =
                    ExecutableScenarioPlan.standardFlinkConfiguration(
                            parallelism, stateBackend, checkpointing);
            if (!expectedStandardConfiguration.equals(standardFlinkConfiguration)) {
                throw new IllegalArgumentException(
                        "Standard Flink configuration does not match typed job options");
            }
        }

        /** Materializes one sorted submission map after runtime-only Kafka values exist. */
        public Map<String, String> materializeFlinkConfiguration(
                String kafkaBootstrapServers,
                Map<Integer, Long> stoppingOffsets,
                int attemptOrdinal,
                String attemptNonce8) {
            Map<String, String> values = new TreeMap<>(standardFlinkConfiguration);
            checkpointing.storage().checkpointDirectory(
                            attemptOrdinal,
                            attemptNonce8,
                            alias,
                            workloadConfiguration.identityPolicy())
                    .ifPresent(directory -> values.put(
                            "execution.checkpointing.dir", directory));
            workloadConfiguration.materialize(
                            kafkaBootstrapServers,
                            stoppingOffsets,
                            attemptOrdinal,
                            attemptNonce8)
                    .forEach((key, value) -> {
                        if (values.putIfAbsent(key, value) != null) {
                            throw new IllegalStateException(
                                    "Standard and workload configuration collide at " + key);
                        }
                    });
            return Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }
    }

    public enum StartMode {
        AUTO
    }

    public record TopicReference(String cluster, String topic) {
        public TopicReference {
            cluster = requireNonBlank(cluster, "cluster");
            topic = requireNonBlank(topic, "topic");
        }
    }

    /** A Kafka sink; the transaction settings exist exactly when delivery is exactly-once. */
    public record Sink(
            TopicReference topic,
            DeliveryGuarantee deliveryGuarantee,
            Optional<String> transactionalIdPrefix,
            Optional<TransactionIdNamingStrategy> transactionIdNamingStrategy) {
        public Sink {
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(deliveryGuarantee, "deliveryGuarantee");
            Objects.requireNonNull(transactionalIdPrefix, "transactionalIdPrefix");
            Objects.requireNonNull(transactionIdNamingStrategy, "transactionIdNamingStrategy");
            transactionalIdPrefix.ifPresent(prefix ->
                    requireNonBlank(prefix, "transactionalIdPrefix"));
            boolean transactional = deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE;
            if (transactionalIdPrefix.isPresent() != transactional
                    || transactionIdNamingStrategy.isPresent() != transactional) {
                throw new IllegalArgumentException(
                        "Transaction settings are required for, and only for, exactly-once");
            }
        }

        public static Sink exactlyOnce(
                TopicReference topic,
                String transactionalIdPrefix,
                TransactionIdNamingStrategy transactionIdNamingStrategy) {
            return new Sink(topic, DeliveryGuarantee.EXACTLY_ONCE,
                    Optional.of(transactionalIdPrefix), Optional.of(transactionIdNamingStrategy));
        }

        public static Sink atLeastOnce(TopicReference topic) {
            return new Sink(topic, DeliveryGuarantee.AT_LEAST_ONCE,
                    Optional.empty(), Optional.empty());
        }
    }

    /**
     * Executable delivery guarantees. {@code AT_LEAST_ONCE} exists for negative controls: after a
     * recovery it is expected to duplicate output.
     */
    public enum DeliveryGuarantee {
        EXACTLY_ONCE,
        AT_LEAST_ONCE
    }

    public enum TransactionIdNamingStrategy {
        INCREMENTING,
        POOLING
    }

    public enum StateBackend {
        HASHMAP("hashmap");

        private final String configurationValue;

        StateBackend(String configurationValue) {
            this.configurationValue = configurationValue;
        }

        public String configurationValue() {
            return configurationValue;
        }
    }

    public record Checkpointing(
            Duration interval,
            CheckpointMode mode,
            CheckpointStorage storage) {
        public Checkpointing {
            interval = requirePositive(interval, "interval");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(storage, "storage");
            if (mode != CheckpointMode.EXACTLY_ONCE) {
                throw new IllegalArgumentException(
                        "The first executable boundary requires exactly-once checkpointing");
            }
        }
    }

    public enum CheckpointMode {
        EXACTLY_ONCE
    }

    public enum CheckpointStorage {
        JOBMANAGER("jobmanager"),
        FILESYSTEM("filesystem");

        private final String configurationValue;

        CheckpointStorage(String configurationValue) {
            this.configurationValue = configurationValue;
        }

        public String configurationValue() {
            return configurationValue;
        }

        Optional<String> checkpointDirectory(
                int attemptOrdinal,
                String attemptNonce8,
                String jobAlias,
                RunScopedIdentityPolicy identityPolicy) {
            // Reuse the attempt-policy validation so the directory and consumer group cannot
            // accidentally be derived from different identity domains.
            identityPolicy.consumerGroupId(attemptOrdinal, attemptNonce8);
            if (this == JOBMANAGER) {
                return Optional.empty();
            }
            return Optional.of("file:/flink/checkpoints/attempt-" + attemptOrdinal + "-"
                    + attemptNonce8 + "/" + jobAlias);
        }
    }

    /** Schema/report marker whose value intentionally means no Flink override is emitted. */
    public enum RestartStrategy {
        FLINK_DEFAULT
    }

    public record StateTtl(
            boolean enabled,
            Optional<Duration> ttl,
            StateTtlCleanup cleanup) {
        public StateTtl {
            ttl = Objects.requireNonNull(ttl, "ttl");
            Objects.requireNonNull(cleanup, "cleanup");
            if (enabled != ttl.isPresent()) {
                throw new IllegalArgumentException(
                        "Enabled state TTL requires a duration and disabled TTL forbids one");
            }
            ttl.ifPresent(value -> requirePositive(value, "ttl"));
            if (!enabled && cleanup != StateTtlCleanup.NONE) {
                throw new IllegalArgumentException("Disabled state TTL requires cleanup NONE");
            }
        }

        public static StateTtl disabled() {
            return new StateTtl(false, Optional.empty(), StateTtlCleanup.NONE);
        }

        public static StateTtl enabled(Duration ttl, StateTtlCleanup cleanup) {
            return new StateTtl(true, Optional.of(ttl), cleanup);
        }
    }

    public enum StateTtlCleanup {
        NONE("none"),
        INCREMENTAL("incremental"),
        ROCKSDB_COMPACTION_FILTER("rocksdb-compaction-filter");

        private final String configurationValue;

        StateTtlCleanup(String configurationValue) {
            this.configurationValue = configurationValue;
        }

        public String configurationValue() {
            return configurationValue;
        }
    }

    public record Watermarks(
            WatermarkStrategy strategy,
            Optional<Duration> maxOutOfOrderness,
            Optional<Duration> idleness) {
        public Watermarks {
            Objects.requireNonNull(strategy, "strategy");
            maxOutOfOrderness = Objects.requireNonNull(
                    maxOutOfOrderness, "maxOutOfOrderness");
            idleness = Objects.requireNonNull(idleness, "idleness");
            maxOutOfOrderness.ifPresent(value -> requirePositive(
                    value, "maxOutOfOrderness"));
            idleness.ifPresent(value -> requirePositive(value, "idleness"));
            if ((strategy == WatermarkStrategy.BOUNDED_OUT_OF_ORDERNESS)
                    != maxOutOfOrderness.isPresent()) {
                throw new IllegalArgumentException(
                        "Only bounded-out-of-orderness requires maxOutOfOrderness");
            }
            if (strategy == WatermarkStrategy.NO_WATERMARKS && idleness.isPresent()) {
                throw new IllegalArgumentException(
                        "no-watermarks does not accept an idleness duration");
            }
        }

        public static Watermarks noWatermarks() {
            return new Watermarks(
                    WatermarkStrategy.NO_WATERMARKS, Optional.empty(), Optional.empty());
        }
    }

    public enum WatermarkStrategy {
        NO_WATERMARKS("no-watermarks"),
        MONOTONIC_TIMESTAMPS("monotonic-timestamps"),
        BOUNDED_OUT_OF_ORDERNESS("bounded-out-of-orderness");

        private final String configurationValue;

        WatermarkStrategy(String configurationValue) {
            this.configurationValue = configurationValue;
        }

        public String configurationValue() {
            return configurationValue;
        }
    }

    /** Job-specific arguments preserved exactly as an ordered structured list. */
    public record ProgramArguments(List<String> values) {
        public ProgramArguments {
            values = List.copyOf(Objects.requireNonNull(values, "values"));
            values.forEach(value -> {
                Objects.requireNonNull(value, "program argument");
                if (value.isEmpty()) {
                    throw new IllegalArgumentException("program argument must not be empty");
                }
            });
        }
    }

    /** Derives an attempt-unique Kafka consumer group that stays stable across job recovery. */
    public record RunScopedIdentityPolicy(String scenarioName, String jobAlias) {
        private static final Pattern NONCE = Pattern.compile("[a-z0-9]{8}");
        private static final Pattern KEBAB = Pattern.compile("[a-z0-9]+(?:-[a-z0-9]+)*");

        public RunScopedIdentityPolicy {
            scenarioName = requireNonBlank(scenarioName, "scenarioName");
            jobAlias = requireNonBlank(jobAlias, "jobAlias");
            if (!KEBAB.matcher(scenarioName).matches() || !KEBAB.matcher(jobAlias).matches()) {
                throw new IllegalArgumentException(
                        "scenarioName and jobAlias must be lower-kebab-case");
            }
        }

        public String consumerGroupId(int attemptOrdinal, String attemptNonce8) {
            if (attemptOrdinal < 1) {
                throw new IllegalArgumentException("attemptOrdinal must be positive");
            }
            attemptNonce8 = requireNonBlank(attemptNonce8, "attemptNonce8");
            if (!NONCE.matcher(attemptNonce8).matches()) {
                throw new IllegalArgumentException(
                        "attemptNonce8 must contain exactly eight lowercase letters or digits");
            }
            return "flink-stability-v1-" + scenarioName + "-" + jobAlias + "-"
                    + attemptOrdinal + "-" + attemptNonce8;
        }
    }

    /** Canonical workload settings, materialized only after Kafka endpoints/offsets exist. */
    public record WorkloadConfiguration(
            String jobAlias,
            TopicReference source,
            int sourcePartitions,
            long expectedInputRecords,
            Sink sink,
            StateTtl stateTtl,
            Watermarks watermarks,
            RunScopedIdentityPolicy identityPolicy,
            Duration transactionTimeout) {
        public static final String PREFIX = "flink-stability.workload.v1.";

        public WorkloadConfiguration {
            jobAlias = requireNonBlank(jobAlias, "jobAlias");
            Objects.requireNonNull(source, "source");
            if (sourcePartitions < 1 || expectedInputRecords < 1) {
                throw new IllegalArgumentException(
                        "sourcePartitions and expectedInputRecords must be positive");
            }
            Objects.requireNonNull(sink, "sink");
            Objects.requireNonNull(stateTtl, "stateTtl");
            Objects.requireNonNull(watermarks, "watermarks");
            Objects.requireNonNull(identityPolicy, "identityPolicy");
            transactionTimeout = requirePositive(transactionTimeout, "transactionTimeout");
            if (!jobAlias.equals(identityPolicy.jobAlias())) {
                throw new IllegalArgumentException("Identity policy must target this job");
            }
            if (!source.cluster().equals(sink.topic().cluster())) {
                throw new IllegalArgumentException(
                        "The first workload protocol requires one Kafka cluster");
            }
            if (transactionTimeout.compareTo(KafkaBrokerPolicy.V1_TRANSACTION_MAX_TIMEOUT) > 0) {
                throw new IllegalArgumentException(
                        "Workload transaction timeout must not exceed the broker maximum");
            }
        }

        /**
         * Produces the exact protocol-v1 Flink configuration after input preload closes.
         * Offsets are exclusive Kafka log-end offsets and must cover every source partition.
         */
        public Map<String, String> materialize(
                String kafkaBootstrapServers,
                Map<Integer, Long> stoppingOffsets,
                int attemptOrdinal,
                String attemptNonce8) {
            kafkaBootstrapServers = requireNonBlank(
                    kafkaBootstrapServers, "kafkaBootstrapServers");
            String canonicalOffsets = canonicalStoppingOffsets(stoppingOffsets);
            Map<String, String> values = new TreeMap<>();
            values.put("flink-stability.workload.protocol", "v1");
            values.put(PREFIX + "job-alias", jobAlias);
            values.put(PREFIX + "source.bootstrap-servers", kafkaBootstrapServers);
            values.put(PREFIX + "source.topic", source.topic());
            values.put(PREFIX + "source.group-id",
                    identityPolicy.consumerGroupId(attemptOrdinal, attemptNonce8));
            values.put(PREFIX + "source.starting-offsets", "committed-or-earliest");
            values.put(PREFIX + "source.isolation-level", "read_uncommitted");
            values.put(PREFIX + "source.stopping-offsets", canonicalOffsets);
            values.put(PREFIX + "sink.bootstrap-servers", kafkaBootstrapServers);
            values.put(PREFIX + "sink.topic", sink.topic().topic());
            values.put(PREFIX + "sink.delivery-guarantee", sink.deliveryGuarantee().name());
            // The workload protocol rejects transaction settings for non-transactional sinks.
            sink.transactionalIdPrefix().ifPresent(prefix -> {
                values.put(PREFIX + "sink.transactional-id-prefix", prefix);
                values.put(PREFIX + "sink.transaction-id-naming-strategy",
                        sink.transactionIdNamingStrategy().orElseThrow().name());
                values.put(PREFIX + "sink.transaction-timeout-ms",
                        Long.toString(transactionTimeout.toMillis()));
            });
            values.put(PREFIX + "state-ttl.enabled", Boolean.toString(stateTtl.enabled()));
            if (stateTtl.enabled()) {
                values.put(PREFIX + "state-ttl.ttl-ms",
                        Long.toString(stateTtl.ttl().orElseThrow().toMillis()));
                values.put(PREFIX + "state-ttl.cleanup",
                        stateTtl.cleanup().configurationValue());
            }
            values.put(PREFIX + "watermarks.strategy",
                    watermarks.strategy().configurationValue());
            watermarks.maxOutOfOrderness().ifPresent(value -> values.put(
                    PREFIX + "watermarks.max-out-of-orderness-ms",
                    Long.toString(value.toMillis())));
            watermarks.idleness().ifPresent(value -> values.put(
                    PREFIX + "watermarks.idleness-ms", Long.toString(value.toMillis())));
            return Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }

        private String canonicalStoppingOffsets(Map<Integer, Long> stoppingOffsets) {
            Objects.requireNonNull(stoppingOffsets, "stoppingOffsets");
            Map<Integer, Long> sorted = new TreeMap<>(stoppingOffsets);
            if (sorted.size() != sourcePartitions) {
                throw new IllegalArgumentException(
                        "Stopping offsets must contain exactly " + sourcePartitions
                                + " source partitions");
            }
            List<String> encoded = new ArrayList<>(sourcePartitions);
            long observedRecords = 0;
            for (int partition = 0; partition < sourcePartitions; partition++) {
                Long exclusiveOffset = sorted.get(partition);
                if (exclusiveOffset == null || exclusiveOffset < 0) {
                    throw new IllegalArgumentException(
                            "Stopping offsets must contain nonnegative partition " + partition);
                }
                observedRecords = Math.addExact(observedRecords, exclusiveOffset);
                encoded.add(partition + ":" + exclusiveOffset);
            }
            if (observedRecords != expectedInputRecords) {
                throw new IllegalArgumentException(
                        "Stopping offsets describe " + observedRecords + " records, expected "
                                + expectedInputRecords);
            }
            return String.join(",", encoded);
        }
    }

    public record Phase(String name, List<Step> steps) {
        public Phase {
            name = requireNonBlank(name, "name");
            steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("steps must not be empty");
            }
        }
    }

    public sealed interface Step permits AwaitJobState, AwaitCheckpoints, Wait,
            KillTaskManager, RestartTaskManager, Loop {}

    public record AwaitJobState(
            String jobAlias,
            JobState state,
            Duration timeout,
            TimeoutOutcome onTimeout) implements Step {
        public AwaitJobState {
            jobAlias = requireNonBlank(jobAlias, "jobAlias");
            Objects.requireNonNull(state, "state");
            timeout = requirePositive(timeout, "timeout");
            Objects.requireNonNull(onTimeout, "onTimeout");
        }
    }

    public enum JobState {
        RUNNING
    }

    public record AwaitCheckpoints(
            String jobAlias,
            long completedCount,
            Duration timeout,
            TimeoutOutcome onTimeout) implements Step {
        public AwaitCheckpoints {
            jobAlias = requireNonBlank(jobAlias, "jobAlias");
            if (completedCount < 1) {
                throw new IllegalArgumentException("completedCount must be positive");
            }
            timeout = requirePositive(timeout, "timeout");
            Objects.requireNonNull(onTimeout, "onTimeout");
        }
    }

    public enum TimeoutOutcome {
        FAIL,
        INCONCLUSIVE
    }

    public record Wait(Duration duration) implements Step {
        public Wait {
            duration = requirePositive(duration, "duration");
        }
    }

    public record KillTaskManager(String targetName) implements Step {
        public KillTaskManager {
            targetName = requireNonBlank(targetName, "targetName");
            if (!"taskmanager-1".equals(targetName)) {
                throw new IllegalArgumentException(
                        "The first executable boundary has only taskmanager-1");
            }
        }
    }

    public record RestartTaskManager() implements Step {}

    public record Loop(int times, List<Step> steps) implements Step {
        public Loop {
            if (times < 1) {
                throw new IllegalArgumentException("times must be positive");
            }
            steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("steps must not be empty");
            }
        }
    }

    public record KafkaIdSetValidation(
            TopicReference output,
            ExpectedRecords expected,
            Duration timeout) {
        public KafkaIdSetValidation {
            Objects.requireNonNull(output, "output");
            Objects.requireNonNull(expected, "expected");
            timeout = requirePositive(timeout, "timeout");
        }
    }

    public enum ExpectedRecords {
        INPUT_MANIFEST
    }

    /**
     * The selected expectation (SPEC-002 E4): pass, or a pinned failure of the one executable
     * oracle with one of its registered failure reasons.
     */
    public record ExpectedOutcome(Outcome outcome, Optional<String> oracle, Optional<String> reason) {
        public enum Outcome {
            PASS,
            FAIL
        }

        public ExpectedOutcome {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(oracle, "oracle");
            Objects.requireNonNull(reason, "reason");
            boolean failure = outcome == Outcome.FAIL;
            if (oracle.isPresent() != failure || reason.isPresent() != failure) {
                throw new IllegalArgumentException(
                        "An expected failure names its oracle and reason; a pass names neither");
            }
        }

        public static ExpectedOutcome pass() {
            return new ExpectedOutcome(Outcome.PASS, Optional.empty(), Optional.empty());
        }

        public static ExpectedOutcome failure(String oracle, String reason) {
            return new ExpectedOutcome(Outcome.FAIL,
                    Optional.of(requireNonBlank(oracle, "oracle")),
                    Optional.of(requireNonBlank(reason, "reason")));
        }
    }

    static Map<String, String> standardFlinkConfiguration(
            int parallelism,
            StateBackend stateBackend,
            Checkpointing checkpointing) {
        Map<String, String> values = new TreeMap<>();
        values.put("execution.checkpointing.interval",
                Long.toString(checkpointing.interval().toMillis()));
        values.put("execution.checkpointing.mode", checkpointing.mode().name());
        values.put("execution.checkpointing.storage",
                checkpointing.storage().configurationValue());
        values.put("parallelism.default", Integer.toString(parallelism));
        values.put("state.backend.type", stateBackend.configurationValue());
        return Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    private static Map<String, String> immutableSortedMap(
            Map<String, String> source,
            String name) {
        Objects.requireNonNull(source, name);
        Map<String, String> sorted = new TreeMap<>();
        source.forEach((key, value) -> {
            requireNonBlank(key, name + " key");
            requireNonBlank(value, name + " value");
            sorted.put(key, value);
        });
        return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
