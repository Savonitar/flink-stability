package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Official Apache Kafka 4.0 runtime used by the first executable v1 path. */
final class ApacheKafkaRuntime implements KafkaRuntimeCluster {

    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

    private final KafkaRuntimeTarget target;
    private final Map<String, String> configuredEnvironment;
    private final KafkaContainer container;
    private boolean startAttempted;
    private boolean started;

    ApacheKafkaRuntime(Network network, KafkaRuntimeTarget target) {
        Objects.requireNonNull(network, "network");
        this.target = Objects.requireNonNull(target, "target");
        Map<String, String> environment = new LinkedHashMap<>(
                brokerEnvironment(target.brokerPolicy()));
        environment.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        this.configuredEnvironment = Collections.unmodifiableMap(environment);
        this.container = new KafkaContainer(DockerImageName.parse(target.imageReference()))
                .withNetwork(network)
                .withNetworkAliases(target.networkAlias())
                .withListener(target.internalBootstrapServers())
                .withEnv(configuredEnvironment)
                .withStartupTimeout(STARTUP_TIMEOUT)
                .withLogConsumer(new Slf4jLogConsumer(
                        LoggerFactory.getLogger("KAFKA_V1_CONTAINER_LOGS")));
    }

    @Override
    public synchronized void start() {
        if (started) {
            throw new IllegalStateException("Kafka is already running");
        }
        startAttempted = true;
        container.start();
        started = true;
    }

    @Override
    public synchronized void stop() {
        if (!startAttempted) {
            return;
        }
        container.stop();
        started = false;
        startAttempted = false;
    }

    @Override
    public synchronized KafkaRuntimeEndpoints endpoints() {
        requireStarted();
        return new KafkaRuntimeEndpoints(
                target.clusterAlias(),
                target.imageReference(),
                target.internalBootstrapServers(),
                container.getBootstrapServers());
    }

    String configuredImageReference() {
        return target.imageReference();
    }

    String configuredNetworkAlias() {
        return target.networkAlias();
    }

    String configuredInternalListener() {
        return target.internalBootstrapServers();
    }

    Map<String, String> configuredEnvironment() {
        return configuredEnvironment;
    }

    private void requireStarted() {
        if (!started || !container.isRunning()) {
            throw new IllegalStateException("Kafka is not running");
        }
    }

    private static Map<String, String> brokerEnvironment(KafkaBrokerPolicy policy) {
        Map<String, String> configuration = policy.kafkaConfiguration();
        return Map.of(
                "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                configuration.get("transaction.max.timeout.ms"),
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR",
                configuration.get("offsets.topic.replication.factor"),
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                configuration.get("transaction.state.log.replication.factor"),
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
                configuration.get("transaction.state.log.min.isr"),
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS",
                configuration.get("group.initial.rebalance.delay.ms"));
    }
}
