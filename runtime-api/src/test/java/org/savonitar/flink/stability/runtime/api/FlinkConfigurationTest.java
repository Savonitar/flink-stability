package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class FlinkConfigurationTest {
    @Test void preservesLiteralValuesAndRejectsConfigurationInjection() {
        assertEquals("\na.key: '# literal: ''value'''\nz.key: 'true'",
                FlinkConfiguration.properties(Map.of("z.key", "true", "a.key", "# literal: 'value'")));
        for (String value : List.of("one\ntwo", "one\rtwo", "one\0two")) {
            assertThrows(IllegalArgumentException.class,
                    () -> FlinkConfiguration.validate(Map.of("custom.key", value)));
        }
        assertThrows(IllegalArgumentException.class,
                () -> FlinkConfiguration.validate(Map.of("a: injected", "value")));
    }

    @Test void ownsTopologyStorageLoggingAndTypedJobConfiguration() {
        for (String key : List.of("jobmanager.rpc.address", "taskmanager.numberOfTaskSlots",
                "taskmanager.resource-id", "execution.checkpointing.storage", "execution.checkpointing.dir",
                "state.checkpoints.dir", "state.savepoints.dir", "state.checkpoint-storage",
                "state.backend.fs.checkpointdir", "execution.checkpointing.savepoint-dir", "savepoints.state.backend.fs.dir",
                "execution.checkpointing.local-backup.dirs", "taskmanager.state.local.root-dirs",
                "recovery.mode", "recovery.jobmanager.port", "recovery.zookeeper.quorum",
                "high-availability", "high-availability.type",
                "env.java.opts.all", "env.java.opts.taskmanager", "flink-stability.workload.protocol",
                "parallelism.default", "state.backend.type", "execution.checkpointing.interval",
                "execution.checkpointing.mode", "blob.server.port", "query.server.port",
                "classloader.resolve-order", "classloader.parent-first-patterns.additional",
                "classloader.parent-first-patterns.default", "pipeline.jars", "pipeline.classpaths")) {
            assertTrue(FlinkConfiguration.reserved(key), key);
            assertThrows(IllegalArgumentException.class,
                    () -> FlinkConfiguration.validate(Map.of(key, "value")), key);
        }
        var map = FlinkConfiguration.validate(Map.of("execution.checkpointing.unaligned.enabled", "true"));
        assertEquals("true", map.get("execution.checkpointing.unaligned.enabled"));
        assertThrows(UnsupportedOperationException.class, () -> map.put("other", "x"));
    }

    @Test void imagePathsRejectTraversalAndBindingIncludesThePin() {
        for (String path : List.of("connector.jar", "/opt/flink/lib/../connector.jar", "/tmp/./connector.jar",
                "/tmp/*.jar", "/tmp/a b.jar", "/tmp/connector")) {
            assertThrows(IllegalArgumentException.class,
                    () -> new ImageConnectorArtifact("kafka", path, "a".repeat(64)), path);
        }
        var primary = new ImageConnectorArtifact("kafka", "/opt/flink/lib/connector.jar", "a".repeat(64));
        var first = new FlinkConnectorBundleInstallation("local/flink:custom", List.of(),
                new ConnectorClasspathManifest(List.of()), List.of(primary));
        var changed = new FlinkConnectorBundleInstallation("local/flink:custom", List.of(),
                new ConnectorClasspathManifest(List.of()), List.of(new ImageConnectorArtifact(
                        "kafka", primary.containerPath(), "b".repeat(64))));
        assertNotEquals(first.targetBindingSha256(), changed.targetBindingSha256());
        assertThrows(IllegalArgumentException.class,
                () -> new FlinkConnectorBundleInstallation("local/flink:custom", List.of(),
                        new ConnectorClasspathManifest(List.of()), List.of(primary, primary)));
    }
}
