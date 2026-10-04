package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Exact image, logical identity, and broker policy for one v1 Kafka runtime. */
public record KafkaRuntimeTarget(
        String clusterAlias,
        String imageReference,
        KafkaBrokerPolicy brokerPolicy, int brokers,
        Optional<String> imageId, String launchType, Map<String, String> brokerConfig, String layout) {

    public static final int INTERNAL_LISTENER_PORT = 19_092;
    public static final String APACHE_KAFKA = "apache-kafka";
    public static final String GENERIC_KRAFT = "generic-kraft";
    public static final String APACHE_LAYOUT = "apache";
    public static final String CONFLUENT_PLATFORM_LAYOUT = "confluent-platform";
    public static final String LOG_DIRECTORY = "/tmp/kafka-logs";
    private static final Set<String> OWNED_KEYS = Set.of("node.id", "broker.id", "cluster.id",
            "process.roles", "listeners", "advertised.listeners", "listener.security.protocol.map",
            "inter.broker.listener.name", "security.inter.broker.protocol", "controller.listener.names",
            "log.dir", "log.dirs", "metadata.log.dir", "auto.create.topics.enable", "controlled.shutdown.enable");

    // These belong to the Apache image wrapper/JVM launcher, not the broker properties file.
    private static final Set<String> APACHE_WRAPPER_ENVIRONMENT = Set.of(
            "KAFKA_VERSION", "KAFKA_HEAP_OPTS", "KAFKA_LOG4J_OPTS", "KAFKA_OPTS", "KAFKA_JMX_OPTS",
            "KAFKA_JVM_PERFORMANCE_OPTS", "KAFKA_GC_LOG_OPTS", "KAFKA_LOG4J_ROOT_LOGLEVEL",
            "KAFKA_LOG4J_LOGGERS", "KAFKA_TOOLS_LOG4J_LOGLEVEL", "KAFKA_JMX_HOSTNAME");

    private static final Pattern CLUSTER_ALIAS =
            Pattern.compile("^[a-z0-9]+(?:-[a-z0-9]+)*$");
    private static final Pattern OFFICIAL_KAFKA_4_0_IMAGE =
            Pattern.compile("^apache/kafka:4\\.0\\.(?:0|[1-9][0-9]*)"
                    + "(?:@sha256:[0-9a-f]{64})?$");

    public KafkaRuntimeTarget(String clusterAlias, String imageReference, KafkaBrokerPolicy policy) {
        this(clusterAlias, imageReference, policy, 1);
    }

    public KafkaRuntimeTarget(String clusterAlias, String imageReference, KafkaBrokerPolicy policy, int brokers) {
        this(clusterAlias, imageReference, policy, brokers, Optional.empty(), APACHE_KAFKA, Map.of());
    }

    public KafkaRuntimeTarget(String clusterAlias, String imageReference, KafkaBrokerPolicy policy, int brokers,
                              Optional<String> imageId, String launchType, Map<String, String> brokerConfig) {
        this(clusterAlias, imageReference, policy, brokers, imageId, launchType, brokerConfig, APACHE_LAYOUT);
    }

    public static String requireLayout(String layout) {
        if (!Set.of(APACHE_LAYOUT, CONFLUENT_PLATFORM_LAYOUT).contains(Objects.requireNonNull(layout, "layout")))
            throw new IllegalArgumentException("Kafka generic-kraft layout must be apache or confluent-platform");
        return layout;
    }

    public static boolean isReservedBrokerConfigKey(String key) {
        return OWNED_KEYS.contains(key) || key.startsWith("controller.quorum.")
                || key.startsWith("listener.name.");
    }

    public static boolean isReservedBrokerConfigKey(String key, String launchType) {
        return isReservedBrokerConfigKey(key) || (APACHE_KAFKA.equals(launchType)
                && APACHE_WRAPPER_ENVIRONMENT.contains(apacheEnvironmentKey(key)));
    }

    public static String apacheEnvironmentKey(String key) {
        return "KAFKA_" + key.toUpperCase(java.util.Locale.ROOT)
                .replace("_", "__").replace("-", "___").replace('.', '_');
    }

    private static String apachePropertyKey(String environmentKey) {
        // Exact Apache Kafka 4.0 Docker wrapper decoding. Adjacent punctuation can be ambiguous.
        return environmentKey.substring("KAFKA_".length()).toLowerCase(java.util.Locale.ROOT)
                .replace('_', '.').replace("...", "-").replace("..", "_");
    }

    public static Map<String, String> validateBrokerConfig(Map<String, String> configuration) {
        return validateBrokerConfig(configuration, APACHE_KAFKA);
    }

    public static Map<String, String> validateBrokerConfig(Map<String, String> configuration, String launchType) {
        Objects.requireNonNull(configuration, "brokerConfig");
        Objects.requireNonNull(launchType, "launchType");
        configuration.forEach((key, value) -> {
            if (key == null || !key.matches("[a-z][a-z0-9._-]*"))
                throw new IllegalArgumentException("Kafka broker configuration key must be lowercase: " + key);
            if (isReservedBrokerConfigKey(key, launchType))
                throw new IllegalArgumentException("runner.kafka.config-reserved-key: " + key);
            if (APACHE_KAFKA.equals(launchType) && !key.equals(apachePropertyKey(apacheEnvironmentKey(key))))
                throw new IllegalArgumentException("Apache Kafka environment encoding cannot preserve broker property key: " + key);
            if (value == null || value.chars().anyMatch(character ->
                    (character < 0x20 && character != '\t' && character != '\f') || character == 0x7f))
                throw new IllegalArgumentException("Kafka broker configuration values must be single-line scalars without unsupported control characters: " + key);
        });
        return Collections.unmodifiableMap(new TreeMap<>(configuration));
    }

    public boolean customConfiguration() {
        return imageId.isPresent() || !APACHE_KAFKA.equals(launchType) || !brokerConfig.isEmpty();
    }

    public java.util.List<KafkaRuntimeLaunch> resolvedLaunches() {
        if (!GENERIC_KRAFT.equals(launchType)) return java.util.List.of();
        return java.util.stream.IntStream.rangeClosed(1, brokers)
                .mapToObj(node -> KafkaRuntimeLaunch.genericKraft(this, node, "${HOST}", "${PORT}"))
                .toList();
    }

    public Map<String, String> resolvedBrokerConfig() {
        var result = new TreeMap<>(brokerPolicy.kafkaConfiguration());
        result.putAll(brokerConfig);
        return Collections.unmodifiableMap(result);
    }

    public String brokerAlias(int ordinal) {
        if (ordinal < 1 || ordinal > brokers) throw new IllegalArgumentException("Unknown broker ordinal");
        return brokers == 1 ? networkAlias() : networkAlias() + "-broker-" + ordinal;
    }

    public KafkaRuntimeTarget {
        Objects.requireNonNull(clusterAlias, "clusterAlias");
        Objects.requireNonNull(imageReference, "imageReference");
        Objects.requireNonNull(brokerPolicy, "brokerPolicy");
        Objects.requireNonNull(imageId, "imageId");
        Objects.requireNonNull(launchType, "launchType");
        layout = requireLayout(layout);
        if (APACHE_KAFKA.equals(launchType) && !APACHE_LAYOUT.equals(layout))
            throw new IllegalArgumentException("Kafka layout applies only to generic-kraft launch");
        imageId.ifPresent(value -> {
            if (!value.matches("sha256:[0-9a-f]{64}"))
                throw new IllegalArgumentException("Kafka image_id must be a full local sha256 image ID");
        });
        if (!Set.of(APACHE_KAFKA, GENERIC_KRAFT).contains(launchType))
            throw new IllegalArgumentException("Kafka launch type must be apache-kafka or generic-kraft");
        brokerConfig = validateBrokerConfig(brokerConfig, launchType);
        if (imageReference.isBlank() || imageReference.chars().anyMatch(Character::isWhitespace))
            throw new IllegalArgumentException("Kafka image reference must be non-blank and contain no whitespace");
        if ((brokers != 1 && brokers != 3) || brokerPolicy.transactionStateLogReplicationFactor() != brokers)
            throw new IllegalArgumentException("Kafka topology and replication policy must agree (one or three brokers)");
        if (!CLUSTER_ALIAS.matcher(clusterAlias).matches()) {
            throw new IllegalArgumentException(
                    "Kafka cluster alias must be lower-kebab-case: " + clusterAlias);
        }
        if (imageId.isEmpty() && !OFFICIAL_KAFKA_4_0_IMAGE.matcher(imageReference).matches()) {
            throw new IllegalArgumentException(
                    "The v1 runtime requires an exact apache/kafka:4.0.<patch> image: "
                            + imageReference);
        }
    }

    public String networkAlias() {
        return "kafka-" + clusterAlias;
    }

    public String internalBootstrapServers() {
        return java.util.stream.IntStream.rangeClosed(1, brokers).mapToObj(i -> brokerAlias(i) + ":" + INTERNAL_LISTENER_PORT)
                .collect(java.util.stream.Collectors.joining(","));
    }
}
