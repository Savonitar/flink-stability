package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Typed broker settings for the supported single-node and three-node KRaft runtimes. */
public record KafkaBrokerPolicy(
        Duration transactionMaxTimeout,
        int offsetsTopicReplicationFactor,
        int transactionStateLogReplicationFactor,
        int transactionStateLogMinIsr,
        Duration groupInitialRebalanceDelay) {

    public static final Duration V1_TRANSACTION_MAX_TIMEOUT = Duration.ofHours(2);

    public static KafkaBrokerPolicy v1SingleBroker() {
        return new KafkaBrokerPolicy(V1_TRANSACTION_MAX_TIMEOUT, 1, 1, 1, Duration.ZERO);
    }

    public static KafkaBrokerPolicy threeBrokers() {
        return new KafkaBrokerPolicy(V1_TRANSACTION_MAX_TIMEOUT, 3, 3, 2, Duration.ZERO);
    }

    public KafkaBrokerPolicy {
        Objects.requireNonNull(transactionMaxTimeout, "transactionMaxTimeout");
        Objects.requireNonNull(groupInitialRebalanceDelay, "groupInitialRebalanceDelay");
        if (!V1_TRANSACTION_MAX_TIMEOUT.equals(transactionMaxTimeout)) {
            throw new IllegalArgumentException(
                    "The v1 Kafka transaction max timeout must be exactly two hours");
        }
        if (!(offsetsTopicReplicationFactor == 1 && transactionStateLogReplicationFactor == 1 && transactionStateLogMinIsr == 1)
                && !(offsetsTopicReplicationFactor == 3 && transactionStateLogReplicationFactor == 3 && transactionStateLogMinIsr == 2)) {
            throw new IllegalArgumentException(
                    "Internal-topic replication must be 1/1/1 or 3/3/2");
        }
        if (!groupInitialRebalanceDelay.isZero()) {
            throw new IllegalArgumentException(
                    "The v1 Kafka group initial rebalance delay must be zero");
        }
    }

    public Map<String, String> kafkaConfiguration() {
        Map<String, String> values = new TreeMap<>();
        values.put(
                "group.initial.rebalance.delay.ms",
                Long.toString(groupInitialRebalanceDelay.toMillis()));
        values.put(
                "offsets.topic.replication.factor",
                Integer.toString(offsetsTopicReplicationFactor));
        values.put(
                "transaction.max.timeout.ms",
                Long.toString(transactionMaxTimeout.toMillis()));
        values.put(
                "transaction.state.log.min.isr",
                Integer.toString(transactionStateLogMinIsr));
        values.put(
                "transaction.state.log.replication.factor",
                Integer.toString(transactionStateLogReplicationFactor));
        if (transactionStateLogReplicationFactor == 3) {
            values.put("default.replication.factor", "3");
            values.put("min.insync.replicas", "2");
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }
}
