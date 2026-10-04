package org.savonitar.flink.stability.runtime.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Shared guard for process and REST configuration; values are literal scalar strings. */
public final class FlinkConfiguration {
    private FlinkConfiguration() {}

    public static boolean reserved(String key) {
        return key.startsWith("classloader.") || key.equals("pipeline.jars") || key.equals("pipeline.classpaths")
                || key.equals("parallelism.default") || key.equals("state.backend.type")
                || key.equals("state.backend") || key.equals("execution.checkpointing.interval")
                || key.equals("execution.checkpointing.mode")
                || key.equals("jobmanager.rpc.address") || key.equals("jobmanager.bind-host")
                || key.equals("blob.server.port") || key.equals("query.server.port")
                || key.equals("rest.address") || key.equals("rest.bind-address") || key.equals("rest.port")
                || key.equals("taskmanager.numberOfTaskSlots")
                || key.equals("taskmanager.resource-id")
                || key.equals("execution.checkpointing.storage")
                || key.equals("state.checkpoints.dir") || key.equals("state.savepoints.dir")
                || key.equals("state.checkpoint-storage") || key.equals("state.backend.fs.checkpointdir")
                || key.equals("execution.checkpointing.savepoint-dir") || key.equals("savepoints.state.backend.fs.dir")
                || key.equals("execution.checkpointing.local-backup.dirs") || key.equals("taskmanager.state.local.root-dirs")
                || key.equals("recovery.mode") || key.equals("recovery.jobmanager.port")
                || key.startsWith("recovery.zookeeper.")
                || key.startsWith("execution.checkpointing.storage.")
                || key.startsWith("execution.checkpointing.dir")
                || key.equals("high-availability") || key.startsWith("high-availability.")
                || key.equals("env.java.opts") || key.startsWith("env.java.opts.")
                || key.equals("zookeeper.sasl.disable")
                || key.equals("security.delegation.tokens.enabled")
                || key.startsWith("security.delegation.token.provider.flink-stability-synthetic.")
                || key.startsWith("flink-stability.token-service.")
                || key.equals("security.delegation.tokens.renewal.time-ratio")
                || key.startsWith("security.delegation.tokens.renewal.retry.")
                || key.startsWith("flink-stability.workload.");
    }

    public static Map<String, String> validate(Map<String, String> values) {
        TreeMap<String, String> checked = new TreeMap<>();
        Objects.requireNonNull(values, "config").forEach((key, value) -> {
            if (key == null || !key.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) {
                throw new IllegalArgumentException("Flink configuration keys must be simple property names");
            }
            if (reserved(key)) throw new IllegalArgumentException("Reserved Flink configuration key: " + key);
            if (value == null || value.chars().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException("Flink configuration value must be a single-line scalar: " + key);
            }
            checked.put(key, value);
        });
        return Collections.unmodifiableMap(checked);
    }

    /**
     * Declaration as YAML literal scalars. This is not input to an image's entrypoint parser:
     * custom-config launches merge it into standard config.yaml before executing Flink scripts.
     */
    public static String properties(Map<String, String> values) {
        StringBuilder result = new StringBuilder();
        validate(values).forEach((key, value) -> result.append('\n').append(key)
                .append(": '").append(value.replace("'", "''")).append("'"));
        return result.toString();
    }
}
