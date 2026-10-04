package org.savonitar.flink.stability.runtime.api;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Explicit launcher contract; only host and hostPort differ between plan and created container. */
public record KafkaRuntimeLaunch(String brokerAlias, Map<String, String> brokerProperties,
                                 Map<String, String> environment, List<String> command,
                                 String logDirectory, String readinessCommand, String layout) {
    public static final String CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk";
    public static final String CONFIG_FILE = "/tmp/flink-stability-kafka.properties";
    public static final String READINESS_COMMAND = readinessCommand(KafkaRuntimeTarget.APACHE_LAYOUT);
    public static final List<String> COMMAND = command(KafkaRuntimeTarget.APACHE_LAYOUT);

    public KafkaRuntimeLaunch(String brokerAlias, Map<String, String> brokerProperties,
                              Map<String, String> environment, List<String> command,
                              String logDirectory, String readinessCommand) {
        this(brokerAlias, brokerProperties, environment, command, logDirectory, readinessCommand,
                KafkaRuntimeTarget.APACHE_LAYOUT);
    }

    public static String tool(String layout, String name) {
        return switch (KafkaRuntimeTarget.requireLayout(layout)) {
            case KafkaRuntimeTarget.APACHE_LAYOUT -> "/opt/kafka/bin/" + name + ".sh";
            case KafkaRuntimeTarget.CONFLUENT_PLATFORM_LAYOUT -> "/usr/bin/" + name;
            default -> throw new IllegalArgumentException("Unsupported Kafka tool layout: " + layout);
        };
    }

    public static String readinessCommand(String layout) {
        return tool(layout, "kafka-broker-api-versions") + " --bootstrap-server localhost:19092";
    }

    public static List<String> command(String layout) {
        return List.of("/bin/sh", "-ec",
            "printf '%s\\n' \"$FLINK_STABILITY_KAFKA_PROPERTIES\" > " + CONFIG_FILE + "\n"
                    + tool(layout, "kafka-storage") + " format --ignore-formatted -t \"$CLUSTER_ID\" -c " + CONFIG_FILE + "\n"
                    + "exec " + tool(layout, "kafka-server-start") + " " + CONFIG_FILE);
    }

    public KafkaRuntimeLaunch {
        brokerProperties = Collections.unmodifiableMap(new TreeMap<>(brokerProperties));
        environment = Collections.unmodifiableMap(new TreeMap<>(environment));
        command = List.copyOf(command);
        layout = KafkaRuntimeTarget.requireLayout(layout);
    }

    public static KafkaRuntimeLaunch genericKraft(KafkaRuntimeTarget target, int node,
                                                   String host, String hostPort) {
        String alias = target.brokerAlias(node);
        var properties = new TreeMap<String, String>();
        properties.put("offsets.topic.num.partitions", "1");
        properties.put("log.flush.interval.messages", Long.toString(Long.MAX_VALUE));
        properties.putAll(target.resolvedBrokerConfig());
        properties.put("node.id", Integer.toString(node));
        properties.put("process.roles", "broker,controller");
        properties.put("listeners", "INTERNAL://0.0.0.0:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094");
        properties.put("advertised.listeners", "INTERNAL://" + alias + ":19092,EXTERNAL://" + host + ":" + hostPort);
        properties.put("listener.security.protocol.map", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
        properties.put("inter.broker.listener.name", "INTERNAL");
        properties.put("controller.listener.names", "CONTROLLER");
        properties.put("controller.quorum.voters", IntStream.rangeClosed(1, target.brokers())
                .mapToObj(id -> id + "@" + target.brokerAlias(id) + ":9094").collect(Collectors.joining(",")));
        properties.put("log.dirs", KafkaRuntimeTarget.LOG_DIRECTORY);
        properties.put("auto.create.topics.enable", "false");
        properties.put("controlled.shutdown.enable", "true");
        String serialized = properties.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + escapePropertyValue(entry.getValue())).collect(Collectors.joining("\n"));
        return new KafkaRuntimeLaunch(alias, properties,
                Map.of("CLUSTER_ID", CLUSTER_ID, "FLINK_STABILITY_KAFKA_PROPERTIES", serialized),
                command(target.layout()), KafkaRuntimeTarget.LOG_DIRECTORY, readinessCommand(target.layout()), target.layout());
    }

    /** Encode a literal scalar for both the direct properties file and the Apache Docker wrapper. */
    public static String escapePropertyValue(String value) {
        // Properties.load would otherwise reinterpret backslashes and trim leading spaces.
        return value.replace("\\", "\\\\").replace(" ", "\\ ").replace("\t", "\\t").replace("\f", "\\f");
    }
}
