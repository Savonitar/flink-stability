package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.resolution.KafkaBrokerImagePolicy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.*;

/** Executable Kafka topology/input boundary, independent of Flink topology validation. */
final class KafkaTopologyCompiler {
    private KafkaTopologyCompiler() {}
    static void validate(Path source, ObjectNode document, List<Diagnostic> issues) {
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
            if (image == null || (!cluster.has("image_id") && !KafkaBrokerImagePolicy.isSupportedV1(image))) {
                issues.add(issue(
                        source,
                        "runner.kafka.image-version-unsupported",
                        clusterPath + "/image",
                        "The first runner requires an official apache/kafka:4.0.x image"));
            }
            if (cluster.has("image_id")) {
                try {
                    org.savonitar.flink.stability.runtime.api.Checks.requireDockerImageId(
                            cluster.path("image_id").asText(), "Kafka image_id");
                } catch (IllegalArgumentException invalid) {
                    issues.add(issue(source, "runner.kafka.image-id-invalid", clusterPath + "/image_id", invalid.getMessage()));
                }
            }
            String launch = cluster.path("launch").path("type").asText("apache-kafka");
            if (!java.util.Set.of("apache-kafka", "generic-kraft").contains(launch)) {
                issues.add(issue(source, "runner.kafka.launch-unsupported", clusterPath + "/launch/type",
                        "Kafka launch type must be apache-kafka or generic-kraft"));
            }
            cluster.path("broker_config").fields().forEachRemaining(entry -> {
                try {
                    org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget.validateBrokerConfig(
                            Map.of(entry.getKey(), entry.getValue().asText()), launch);
                } catch (IllegalArgumentException invalid) {
                    issues.add(issue(source,
                            org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget.isReservedBrokerConfigKey(entry.getKey(), launch)
                                    ? "runner.kafka.config-reserved-key" : "runner.kafka.config-invalid",
                            clusterPath + "/broker_config/" + pointer(entry.getKey()), invalid.getMessage()));
                }
            });
            if (!"kraft".equals(cluster.path("mode").textValue())) {
                issues.add(issue(
                        source,
                        "runner.kafka.mode-unsupported",
                        clusterPath + "/mode",
                        "The first runner requires KRaft mode"));
            }
            int brokers = cluster.path("brokers").intValue();
            if (!cluster.path("brokers").canConvertToInt() || (brokers != 1 && brokers != 3)) {
                issues.add(issue(source, "runner.kafka.broker-count-unsupported", clusterPath + "/brokers",
                        "The runner supports one or three KRaft brokers"));
            }
            if (brokers == 3 && !document.at("/setup/proxies").isMissingNode() && !document.at("/setup/proxies").isEmpty()) {
                issues.add(issue(source, "runner.kafka.multi-broker-proxy-unsupported", "$/setup/proxies",
                        "Three-broker proxy routing is outside this execution slice"));
            }
            ArrayNode topics = (ArrayNode) cluster.path("topics");
            if (brokers == 3 && java.util.stream.StreamSupport.stream(topics.spliterator(), false)
                    .mapToLong(topic -> topic.path("partitions").asLong()).sum() > 128) {
                issues.add(issue(source, "runner.kafka.partition-bound-exceeded", clusterPath + "/topics",
                        "Three-broker metadata is bounded to 128 declared partitions"));
            }
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
                        brokers,
                        topicPath + "/replication_factor",
                        "runner.kafka.replication-factor-unsupported",
                        "Topic replication_factor must match the broker count",
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

    }
}
