package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.FlinkConfiguration;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class FlinkProcessConfigurationTest {
    @TempDir Path temporaryDirectory;

    @Test void memoryAndLiteralValuesReachTheYamlLoaderWithoutEntrypointRewriting() {
        var requested = Map.of("taskmanager.memory.process.size", "1600m",
                "custom.literal", "  # literal: 'quote' \" $USER ${USER} \\ end  ",
                "custom.empty", "", "custom.boolean", "true", "custom.number", "01");
        byte[] file = FlinkProcessConfiguration.merge(bytes("""
                taskmanager:
                  memory:
                    process:
                      size: 1728m
                env.java.opts.all: --add-opens=java.base/java.lang=ALL-UNNAMED
                """), "taskmanager.numberOfTaskSlots: 4", requested, "jobmanager-1");
        String startup = startup(requested);
        assertEquals(requested, FlinkProcessConfiguration.observe(file, file, requested, startup));
        String serialized = new String(file, StandardCharsets.UTF_8);
        assertTrue(serialized.contains("env.java.opts.all: --add-opens=java.base/java.lang=ALL-UNNAMED"));
        assertTrue(serialized.contains("jobmanager.rpc.address: jobmanager-1"));
        assertTrue(serialized.contains("taskmanager.numberOfTaskSlots: 4"));
    }

    @Test void copiedFileAndActualStartupAreBothRequiredIncludingDynamicOverrides() {
        var requested = Map.of("taskmanager.memory.process.size", "1600m");
        byte[] file = FlinkProcessConfiguration.merge(bytes("{}"), "taskmanager.numberOfTaskSlots: 4", requested, "jm");
        for (String log : List.of("", "Loading configuration property: taskmanager.memory.process.size, '1600m'\n",
                startup(requested) + "Loading dynamic configuration property: taskmanager.memory.process.size, 1728m\n",
                "Loading dynamic configuration property: taskmanager.memory.process.size, 1600m\n")) {
            assertThrows(IllegalStateException.class, () -> FlinkProcessConfiguration.observe(file, file, requested, log));
        }
        assertThrows(IllegalStateException.class,
                () -> FlinkProcessConfiguration.observe(bytes("{}"), file, requested, startup(requested)));
        assertThrows(IllegalStateException.class, () -> FlinkProcessConfiguration.observe(file, file,
                Map.of("taskmanager.memory.process.size", "1728m"), startup(requested)));
        assertEquals(requested, FlinkProcessConfiguration.observe(file, file, requested,
                startup(requested) + "Loading dynamic configuration property: taskmanager.memory.process.size, 1600m\n"));
    }

    @Test void originalImageUsesFlinksYaml12CoreSchemaWithoutRetypingYaml11Scalars() {
        byte[] file = FlinkProcessConfiguration.merge(bytes("""
                custom:
                  on: on
                  off: off
                  octal: 0123
                  yes: yes
                  empty: ''
                """), "taskmanager.numberOfTaskSlots: 4", Map.of("custom.new", "new"), "jm");
        var decoded = (Map<?, ?>) new org.snakeyaml.engine.v2.api.Load(
                org.snakeyaml.engine.v2.api.LoadSettings.builder()
                        .setSchema(new org.snakeyaml.engine.v2.schema.CoreSchema()).build())
                .loadFromString(new String(file, StandardCharsets.UTF_8));
        assertEquals("on", decoded.get("custom.on"));
        assertEquals("off", decoded.get("custom.off"));
        assertEquals("yes", decoded.get("custom.yes"));
        assertEquals(123, decoded.get("custom.octal"));
        assertEquals("", decoded.get("custom.empty"));
    }

    @Test void ambiguousImageYamlCannotBeMergedSilently() {
        for (String invalid : List.of("a: 1\na: 2\n", "a.b: 1\na:\n  b: 2\n", "- a\n- b\n", "x: &a {y: 1}\nz: *a\n")) {
            assertThrows(RuntimeException.class, () -> FlinkProcessConfiguration.merge(bytes(invalid),
                    "taskmanager.numberOfTaskSlots: 4", Map.of("custom.key", "value"), "jm"));
        }
        assertThrows(IllegalArgumentException.class, () -> FlinkProcessConfiguration.merge(
                new byte[FlinkProcessConfiguration.MAX_BYTES + 1], "taskmanager.numberOfTaskSlots: 4", Map.of(), "jm"));
    }

    @Test void officialEntrypointFixtureDemonstratesWhyTheCustomLauncherBypassesIt() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.isExecutable(Path.of("/bin/bash")));
        Path fixture = temporaryDirectory.resolve("entrypoint.sh");
        try (var source = getClass().getResourceAsStream("/flink-entrypoint-properties.sh")) {
            Files.copy(java.util.Objects.requireNonNull(source), fixture);
        }
        String properties = FlinkConfiguration.properties(Map.of("taskmanager.memory.process.size", "1600m",
                "custom.literal", "a b $PLACEHOLDER"));
        var process = new ProcessBuilder("/bin/bash", fixture.toString(), properties).redirectErrorStream(true).start();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        String transformed = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), transformed);
        assertTrue(transformed.contains("taskmanager.memory.process.size='1600m'"), transformed);
        assertTrue(transformed.contains("custom.literal='abexpanded'"), transformed);
        byte[] file = FlinkProcessConfiguration.merge(bytes("{}"), "taskmanager.numberOfTaskSlots: 4",
                Map.of("taskmanager.memory.process.size", "1600m", "custom.literal", "a b $PLACEHOLDER"), "jm");
        assertEquals("a b $PLACEHOLDER", FlinkProcessConfiguration.observe(file, file,
                Map.of("custom.literal", "a b $PLACEHOLDER"), startup(Map.of("custom.literal", "a b $PLACEHOLDER")))
                .get("custom.literal"));
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String startup(Map<String, String> properties) {
        return properties.entrySet().stream().map(entry -> "INFO GlobalConfiguration - Loading configuration property: "
                + entry.getKey() + ", " + entry.getValue() + "\n").collect(Collectors.joining());
    }
}
