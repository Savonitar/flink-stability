package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Strict parser for the harness-to-workload v1 configuration contract. */
final class WorkloadProtocolV1Configuration {

    static final String PROTOCOL = "flink-stability.workload.protocol";
    static final String JOB_ALIAS = "flink-stability.workload.v1.job-alias";

    static final String SOURCE_BOOTSTRAP_SERVERS =
            "flink-stability.workload.v1.source.bootstrap-servers";
    static final String SOURCE_TOPIC = "flink-stability.workload.v1.source.topic";
    static final String SOURCE_GROUP_ID = "flink-stability.workload.v1.source.group-id";
    static final String SOURCE_STARTING_OFFSETS =
            "flink-stability.workload.v1.source.starting-offsets";
    static final String SOURCE_STOPPING_OFFSETS =
            "flink-stability.workload.v1.source.stopping-offsets";
    static final String SOURCE_ISOLATION_LEVEL =
            "flink-stability.workload.v1.source.isolation-level";

    static final String SINK_BOOTSTRAP_SERVERS =
            "flink-stability.workload.v1.sink.bootstrap-servers";
    static final String SINK_TOPIC = "flink-stability.workload.v1.sink.topic";
    static final String SINK_DELIVERY_GUARANTEE =
            "flink-stability.workload.v1.sink.delivery-guarantee";
    static final String SINK_TRANSACTIONAL_ID_PREFIX =
            "flink-stability.workload.v1.sink.transactional-id-prefix";
    static final String SINK_TRANSACTION_ID_NAMING_STRATEGY =
            "flink-stability.workload.v1.sink.transaction-id-naming-strategy";
    static final String SINK_TRANSACTION_TIMEOUT_MS =
            "flink-stability.workload.v1.sink.transaction-timeout-ms";

    static final String STATE_TTL_ENABLED =
            "flink-stability.workload.v1.state-ttl.enabled";
    static final String STATE_TTL_MS = "flink-stability.workload.v1.state-ttl.ttl-ms";
    static final String STATE_TTL_CLEANUP = "flink-stability.workload.v1.state-ttl.cleanup";

    static final String WATERMARKS_STRATEGY =
            "flink-stability.workload.v1.watermarks.strategy";
    static final String WATERMARKS_MAX_OUT_OF_ORDERNESS_MS =
            "flink-stability.workload.v1.watermarks.max-out-of-orderness-ms";
    static final String WATERMARKS_IDLENESS_MS =
            "flink-stability.workload.v1.watermarks.idleness-ms";

    private static final String RESERVED_PREFIX = "flink-stability.workload.";
    private static final String VERSION = "v1";
    private static final String COMMITTED_OR_EARLIEST = "committed-or-earliest";
    private static final String READ_UNCOMMITTED = "read_uncommitted";

    private static final Set<String> KNOWN_KEYS = Set.of(
            PROTOCOL,
            JOB_ALIAS,
            SOURCE_BOOTSTRAP_SERVERS,
            SOURCE_TOPIC,
            SOURCE_GROUP_ID,
            SOURCE_STARTING_OFFSETS,
            SOURCE_STOPPING_OFFSETS,
            SOURCE_ISOLATION_LEVEL,
            SINK_BOOTSTRAP_SERVERS,
            SINK_TOPIC,
            SINK_DELIVERY_GUARANTEE,
            SINK_TRANSACTIONAL_ID_PREFIX,
            SINK_TRANSACTION_ID_NAMING_STRATEGY,
            SINK_TRANSACTION_TIMEOUT_MS,
            STATE_TTL_ENABLED,
            STATE_TTL_MS,
            STATE_TTL_CLEANUP,
            WATERMARKS_STRATEGY,
            WATERMARKS_MAX_OUT_OF_ORDERNESS_MS,
            WATERMARKS_IDLENESS_MS);

    private final String jobAlias;
    private final String sourceBootstrapServers;
    private final String sourceTopic;
    private final String sourceGroupId;
    private final Map<Integer, Long> sourceStoppingOffsets;
    private final String sinkBootstrapServers;
    private final String sinkTopic;
    private final DeliveryGuarantee deliveryGuarantee;
    private final String transactionalIdPrefix;
    private final TransactionNamingStrategy transactionNamingStrategy;
    private final Integer transactionTimeoutMs;
    private final StateTtlSettings stateTtl;
    private final WatermarkSettings watermarks;

    private WorkloadProtocolV1Configuration(
            String jobAlias,
            String sourceBootstrapServers,
            String sourceTopic,
            String sourceGroupId,
            Map<Integer, Long> sourceStoppingOffsets,
            String sinkBootstrapServers,
            String sinkTopic,
            DeliveryGuarantee deliveryGuarantee,
            String transactionalIdPrefix,
            TransactionNamingStrategy transactionNamingStrategy,
            Integer transactionTimeoutMs,
            StateTtlSettings stateTtl,
            WatermarkSettings watermarks) {
        this.jobAlias = jobAlias;
        this.sourceBootstrapServers = sourceBootstrapServers;
        this.sourceTopic = sourceTopic;
        this.sourceGroupId = sourceGroupId;
        this.sourceStoppingOffsets = sourceStoppingOffsets;
        this.sinkBootstrapServers = sinkBootstrapServers;
        this.sinkTopic = sinkTopic;
        this.deliveryGuarantee = deliveryGuarantee;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.transactionNamingStrategy = transactionNamingStrategy;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.stateTtl = stateTtl;
        this.watermarks = watermarks;
    }

    static WorkloadProtocolV1Configuration from(ReadableConfig readableConfig) {
        Objects.requireNonNull(readableConfig, "readableConfig");
        Map<String, String> values = readableConfig.toMap();
        rejectUnknownReservedKeys(values);

        requireExact(values, PROTOCOL, VERSION);
        String jobAlias = required(values, JOB_ALIAS);
        if (!jobAlias.matches("[a-z0-9]+(?:-[a-z0-9]+)*")) {
            throw invalid(JOB_ALIAS, "must be a lower-kebab-case alias");
        }

        String sourceBootstrapServers = required(values, SOURCE_BOOTSTRAP_SERVERS);
        String sourceTopic = required(values, SOURCE_TOPIC);
        String sourceGroupId = required(values, SOURCE_GROUP_ID);
        requireExact(values, SOURCE_STARTING_OFFSETS, COMMITTED_OR_EARLIEST);
        Map<Integer, Long> stoppingOffsets = parseStoppingOffsets(
                required(values, SOURCE_STOPPING_OFFSETS));
        requireExact(values, SOURCE_ISOLATION_LEVEL, READ_UNCOMMITTED);

        String sinkBootstrapServers = required(values, SINK_BOOTSTRAP_SERVERS);
        String sinkTopic = required(values, SINK_TOPIC);
        DeliveryGuarantee deliveryGuarantee = parseDeliveryGuarantee(values);

        String transactionalIdPrefix = optional(values, SINK_TRANSACTIONAL_ID_PREFIX);
        TransactionNamingStrategy namingStrategy = parseTransactionNamingStrategy(values);
        Integer transactionTimeoutMs = optionalPositiveInt(values, SINK_TRANSACTION_TIMEOUT_MS);
        validateTransactionOptions(
                deliveryGuarantee,
                transactionalIdPrefix,
                namingStrategy,
                transactionTimeoutMs);

        StateTtlSettings stateTtl = parseStateTtl(values);
        WatermarkSettings watermarks = parseWatermarks(values);

        return new WorkloadProtocolV1Configuration(
                jobAlias,
                sourceBootstrapServers,
                sourceTopic,
                sourceGroupId,
                stoppingOffsets,
                sinkBootstrapServers,
                sinkTopic,
                deliveryGuarantee,
                transactionalIdPrefix,
                namingStrategy,
                transactionTimeoutMs,
                stateTtl,
                watermarks);
    }

    private static void rejectUnknownReservedKeys(Map<String, String> values) {
        values.keySet().stream()
                .filter(key -> key.startsWith(RESERVED_PREFIX))
                .filter(key -> !KNOWN_KEYS.contains(key))
                .sorted()
                .findFirst()
                .ifPresent(key -> {
                    throw invalid(key, "is not defined by workload protocol v1");
                });
    }

    private static void validateTransactionOptions(
            DeliveryGuarantee deliveryGuarantee,
            String transactionalIdPrefix,
            TransactionNamingStrategy namingStrategy,
            Integer transactionTimeoutMs) {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            if (transactionalIdPrefix == null) {
                throw missing(SINK_TRANSACTIONAL_ID_PREFIX);
            }
            if (namingStrategy == null) {
                throw missing(SINK_TRANSACTION_ID_NAMING_STRATEGY);
            }
            if (transactionTimeoutMs == null) {
                throw missing(SINK_TRANSACTION_TIMEOUT_MS);
            }
            return;
        }

        rejectPresent(transactionalIdPrefix, SINK_TRANSACTIONAL_ID_PREFIX, deliveryGuarantee);
        rejectPresent(namingStrategy, SINK_TRANSACTION_ID_NAMING_STRATEGY, deliveryGuarantee);
        rejectPresent(transactionTimeoutMs, SINK_TRANSACTION_TIMEOUT_MS, deliveryGuarantee);
    }

    private static void rejectPresent(Object value, String key, DeliveryGuarantee guarantee) {
        if (value != null) {
            throw invalid(key, "is not valid with delivery guarantee " + guarantee.name());
        }
    }

    private static StateTtlSettings parseStateTtl(Map<String, String> values) {
        boolean enabled = requiredBoolean(values, STATE_TTL_ENABLED);
        if (!enabled) {
            rejectPresent(values, STATE_TTL_MS, "state TTL is disabled");
            rejectPresent(values, STATE_TTL_CLEANUP, "state TTL is disabled");
            return StateTtlSettings.disabled();
        }

        long ttlMs = requiredPositiveLong(values, STATE_TTL_MS);
        String cleanup = optional(values, STATE_TTL_CLEANUP);
        return StateTtlSettings.enabled(ttlMs, cleanup == null ? "none" : cleanup);
    }

    private static WatermarkSettings parseWatermarks(Map<String, String> values) {
        String strategy = required(values, WATERMARKS_STRATEGY);
        Long maxOutOfOrdernessMs = optionalNonNegativeLong(
                values, WATERMARKS_MAX_OUT_OF_ORDERNESS_MS);
        Long idlenessMs = optionalPositiveLong(values, WATERMARKS_IDLENESS_MS);

        switch (strategy) {
            case "no-watermarks":
                rejectPresent(
                        maxOutOfOrdernessMs,
                        WATERMARKS_MAX_OUT_OF_ORDERNESS_MS,
                        "watermark strategy is no-watermarks");
                rejectPresent(
                        idlenessMs,
                        WATERMARKS_IDLENESS_MS,
                        "watermark strategy is no-watermarks");
                break;
            case "monotonic-timestamps":
                rejectPresent(
                        maxOutOfOrdernessMs,
                        WATERMARKS_MAX_OUT_OF_ORDERNESS_MS,
                        "watermark strategy is monotonic-timestamps");
                break;
            case "bounded-out-of-orderness":
                if (maxOutOfOrdernessMs == null) {
                    throw missing(WATERMARKS_MAX_OUT_OF_ORDERNESS_MS);
                }
                break;
            default:
                throw invalid(
                        WATERMARKS_STRATEGY,
                        "must be no-watermarks, monotonic-timestamps, or bounded-out-of-orderness");
        }
        return new WatermarkSettings(strategy, maxOutOfOrdernessMs, idlenessMs);
    }

    private static Map<Integer, Long> parseStoppingOffsets(String value) {
        Map<Integer, Long> offsets = new LinkedHashMap<>();
        int previousPartition = -1;
        for (String pair : value.split(",", -1)) {
            if (!pair.matches("(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)")) {
                throw invalid(
                        SOURCE_STOPPING_OFFSETS,
                        "must contain canonical partition:exclusiveOffset pairs");
            }
            int separator = pair.indexOf(':');
            int partition;
            long offset;
            try {
                partition = Integer.parseInt(pair.substring(0, separator));
                offset = Long.parseLong(pair.substring(separator + 1));
            } catch (NumberFormatException e) {
                throw invalid(
                        SOURCE_STOPPING_OFFSETS,
                        "contains a partition or offset outside the supported numeric range",
                        e);
            }
            if (partition <= previousPartition) {
                throw invalid(
                        SOURCE_STOPPING_OFFSETS,
                        "must use unique, strictly ascending numeric partition IDs");
            }
            offsets.put(partition, offset);
            previousPartition = partition;
        }
        return Collections.unmodifiableMap(offsets);
    }

    private static DeliveryGuarantee parseDeliveryGuarantee(Map<String, String> values) {
        String value = required(values, SINK_DELIVERY_GUARANTEE);
        switch (value) {
            case "NONE":
                return DeliveryGuarantee.NONE;
            case "AT_LEAST_ONCE":
                return DeliveryGuarantee.AT_LEAST_ONCE;
            case "EXACTLY_ONCE":
                return DeliveryGuarantee.EXACTLY_ONCE;
            default:
                throw invalid(
                        SINK_DELIVERY_GUARANTEE,
                        "must be NONE, AT_LEAST_ONCE, or EXACTLY_ONCE");
        }
    }

    private static TransactionNamingStrategy parseTransactionNamingStrategy(
            Map<String, String> values) {
        String value = optional(values, SINK_TRANSACTION_ID_NAMING_STRATEGY);
        if (value == null) {
            return null;
        }
        switch (value) {
            case "INCREMENTING":
                return TransactionNamingStrategy.INCREMENTING;
            case "POOLING":
                return TransactionNamingStrategy.POOLING;
            default:
                throw invalid(
                        SINK_TRANSACTION_ID_NAMING_STRATEGY,
                        "must be INCREMENTING or POOLING");
        }
    }

    private static boolean requiredBoolean(Map<String, String> values, String key) {
        String value = required(values, key);
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw invalid(key, "must be exactly true or false");
    }

    private static Integer optionalPositiveInt(Map<String, String> values, String key) {
        String value = optional(values, key);
        if (value == null) {
            return null;
        }
        if (!value.matches("[1-9][0-9]*")) {
            throw invalid(key, "must be a positive base-10 integer");
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw invalid(key, "exceeds the supported integer range", e);
        }
    }

    private static long requiredPositiveLong(Map<String, String> values, String key) {
        Long value = optionalPositiveLong(values, key);
        if (value == null) {
            throw missing(key);
        }
        return value;
    }

    private static Long optionalPositiveLong(Map<String, String> values, String key) {
        String value = optional(values, key);
        if (value == null) {
            return null;
        }
        if (!value.matches("[1-9][0-9]*")) {
            throw invalid(key, "must be a positive base-10 integer");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw invalid(key, "exceeds the supported long range", e);
        }
    }

    private static Long optionalNonNegativeLong(Map<String, String> values, String key) {
        String value = optional(values, key);
        if (value == null) {
            return null;
        }
        if (!value.matches("0|[1-9][0-9]*")) {
            throw invalid(key, "must be a nonnegative base-10 integer");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw invalid(key, "exceeds the supported long range", e);
        }
    }

    private static void requireExact(Map<String, String> values, String key, String expected) {
        String actual = required(values, key);
        if (!expected.equals(actual)) {
            throw invalid(key, "must be exactly '" + expected + "'");
        }
    }

    private static String required(Map<String, String> values, String key) {
        String value = optional(values, key);
        if (value == null) {
            throw missing(key);
        }
        return value;
    }

    private static String optional(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            return null;
        }
        if (value.isEmpty() || !value.equals(value.trim())) {
            throw invalid(key, "must be nonblank and must not have surrounding whitespace");
        }
        return value;
    }

    private static void rejectPresent(Map<String, String> values, String key, String reason) {
        if (values.containsKey(key)) {
            throw invalid(key, "must be absent because " + reason);
        }
    }

    private static void rejectPresent(Object value, String key, String reason) {
        if (value != null) {
            throw invalid(key, "must be absent because " + reason);
        }
    }

    private static IllegalArgumentException missing(String key) {
        return new IllegalArgumentException("Missing required workload configuration key '" + key + "'");
    }

    private static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("Invalid workload configuration key '" + key + "': " + message);
    }

    private static IllegalArgumentException invalid(String key, String message, Throwable cause) {
        return new IllegalArgumentException(
                "Invalid workload configuration key '" + key + "': " + message,
                cause);
    }

    String jobAlias() {
        return jobAlias;
    }

    String sourceBootstrapServers() {
        return sourceBootstrapServers;
    }

    String sourceTopic() {
        return sourceTopic;
    }

    String sourceGroupId() {
        return sourceGroupId;
    }

    Map<TopicPartition, Long> sourceStoppingOffsetsByTopic() {
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
        sourceStoppingOffsets.forEach(
                (partition, offset) -> offsets.put(new TopicPartition(sourceTopic, partition), offset));
        return Collections.unmodifiableMap(offsets);
    }

    Map<Integer, Long> sourceStoppingOffsets() {
        return sourceStoppingOffsets;
    }

    String sinkBootstrapServers() {
        return sinkBootstrapServers;
    }

    String sinkTopic() {
        return sinkTopic;
    }

    DeliveryGuarantee deliveryGuarantee() {
        return deliveryGuarantee;
    }

    String transactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    TransactionNamingStrategy transactionNamingStrategy() {
        return transactionNamingStrategy;
    }

    Integer transactionTimeoutMs() {
        return transactionTimeoutMs;
    }

    StateTtlSettings stateTtl() {
        return stateTtl;
    }

    WatermarkSettings watermarks() {
        return watermarks;
    }

    static final class StateTtlSettings implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final int INCREMENTAL_CLEANUP_SIZE = 100;
        private static final long ROCKSDB_QUERY_AFTER_ENTRIES = 1_000L;

        private final boolean enabled;
        private final Long ttlMs;
        private final String cleanup;

        private StateTtlSettings(boolean enabled, Long ttlMs, String cleanup) {
            this.enabled = enabled;
            this.ttlMs = ttlMs;
            this.cleanup = cleanup;
        }

        static StateTtlSettings disabled() {
            return new StateTtlSettings(false, null, null);
        }

        static StateTtlSettings enabled(long ttlMs, String cleanup) {
            if (!Set.of("none", "incremental", "rocksdb-compaction-filter").contains(cleanup)) {
                throw invalid(
                        STATE_TTL_CLEANUP,
                        "must be none, incremental, or rocksdb-compaction-filter");
            }
            return new StateTtlSettings(true, ttlMs, cleanup);
        }

        boolean enabled() {
            return enabled;
        }

        long ttlMs() {
            if (ttlMs == null) {
                throw new IllegalStateException("State TTL is disabled");
            }
            return ttlMs;
        }

        String cleanup() {
            return cleanup;
        }

        StateTtlConfig toFlinkConfig() {
            if (!enabled) {
                return StateTtlConfig.DISABLED;
            }

            StateTtlConfig.Builder builder = StateTtlConfig.newBuilder(Duration.ofMillis(ttlMs));
            switch (cleanup) {
                case "none":
                    builder.disableCleanupInBackground();
                    break;
                case "incremental":
                    builder.cleanupIncrementally(INCREMENTAL_CLEANUP_SIZE, true);
                    break;
                case "rocksdb-compaction-filter":
                    builder.cleanupInRocksdbCompactFilter(ROCKSDB_QUERY_AFTER_ENTRIES);
                    break;
                default:
                    throw new IllegalStateException("Unsupported state TTL cleanup: " + cleanup);
            }
            return builder.build();
        }
    }

    static final class WatermarkSettings {
        private final String strategy;
        private final Long maxOutOfOrdernessMs;
        private final Long idlenessMs;

        private WatermarkSettings(
                String strategy,
                Long maxOutOfOrdernessMs,
                Long idlenessMs) {
            this.strategy = strategy;
            this.maxOutOfOrdernessMs = maxOutOfOrdernessMs;
            this.idlenessMs = idlenessMs;
        }

        String strategy() {
            return strategy;
        }

        Long maxOutOfOrdernessMs() {
            return maxOutOfOrdernessMs;
        }

        Long idlenessMs() {
            return idlenessMs;
        }

        WatermarkStrategy<String> toFlinkStrategy() {
            WatermarkStrategy<String> watermarkStrategy;
            switch (strategy) {
                case "no-watermarks":
                    return WatermarkStrategy.noWatermarks();
                case "monotonic-timestamps":
                    watermarkStrategy = WatermarkStrategy
                            .<String>forMonotonousTimestamps()
                            .withTimestampAssigner((event, previousTimestamp) -> eventTimestamp(event));
                    break;
                case "bounded-out-of-orderness":
                    watermarkStrategy = WatermarkStrategy
                            .<String>forBoundedOutOfOrderness(
                                    Duration.ofMillis(maxOutOfOrdernessMs))
                            .withTimestampAssigner((event, previousTimestamp) -> eventTimestamp(event));
                    break;
                default:
                    throw new IllegalStateException("Unsupported watermark strategy: " + strategy);
            }
            return idlenessMs == null
                    ? watermarkStrategy
                    : watermarkStrategy.withIdleness(Duration.ofMillis(idlenessMs));
        }

        private static long eventTimestamp(String event) {
            try {
                return Long.parseLong(event);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Timestamp watermark strategies require base-10 millisecond records", e);
            }
        }
    }
}
