package org.savonitar.flink.stability.testcontainers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class KafkaLifecycleInspectionTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String STATUS = "Name:\tjava\nState:\tS (sleeping)\nPid:\t1\nPPid:\t0\nNSpid:\t1\n";
    private static final String STAT = "1 (java (process)) S " + "0 ".repeat(18) + "12345 0\n";
    private static final String CMDLINE = "/opt/java/bin/java\0-Xmx1g\0kafka.Kafka\0/config/server.properties\0";

    @Test void sharedWriterPublishesTimestampedJsonWithoutLeavingPartialFiles(@TempDir Path output) throws Exception {
        long before = System.nanoTime();
        KafkaLifecycleInspection.record(output, "observation", Map.of("status", "completed"));
        long after = System.nanoTime();
        var receipt = JSON.readTree(output.resolve("observation.json").toFile());
        assertDoesNotThrow(() -> Instant.parse(receipt.path("wall").asText()));
        assertTrue(receipt.path("monotonicNanos").isIntegralNumber());
        assertTrue(receipt.path("monotonicNanos").asLong() >= before && receipt.path("monotonicNanos").asLong() <= after);
        assertEquals("completed", receipt.path("status").asText());
        assertFalse(Files.exists(output.resolve("observation.json.partial")));
    }

    @Test void retainsMatchingStockAndCustomImageEntrypoints(@TempDir Path output) throws Exception {
        String[][] entrypoints = {{"/__cacert_entrypoint.sh"}, {"/custom/bootstrap.sh", "--initialize"}};
        for (int index = 0; index < entrypoints.length; index++) {
            Path selected = Files.createDirectory(output.resolve(Integer.toString(index)));
            String[] expected = entrypoints[index];
            KafkaLifecycleInspection.launch(selected, expected, expected.clone(), new String[]{"sh", "-c", "exec starter"}, Map.of());
            var receipt = JSON.readTree(selected.resolve("created-launch.json").toFile());
            assertEquals(JSON.valueToTree(expected), receipt.path("expectedEntrypoint"));
            assertEquals(receipt.path("expectedEntrypoint"), receipt.path("entrypoint"));
            assertEquals("exact-null-and-empty-distinct", receipt.path("entrypointPolicy").asText());
        }
    }

    @Test void retainsExpectedAndObservedEntrypointsBeforeRejectingMismatch(@TempDir Path output) throws Exception {
        assertThrows(IOException.class, () -> KafkaLifecycleInspection.launch(output,
                new String[]{"/custom/bootstrap.sh"}, new String[]{"/__cacert_entrypoint.sh"}, new String[]{"sh"}, Map.of()));
        var receipt = JSON.readTree(output.resolve("created-launch.json").toFile());
        assertEquals("/custom/bootstrap.sh", receipt.at("/expectedEntrypoint/0").asText());
        assertEquals("/__cacert_entrypoint.sh", receipt.at("/entrypoint/0").asText());
    }

    @Test void entrypointAbsenceKeepsNullAndEmptyDistinctAndNeverInventsStockValues(@TempDir Path output) throws Exception {
        String[][] absent = {null, new String[0]};
        for (int expected = 0; expected < absent.length; expected++) {
            for (int observed = 0; observed < absent.length; observed++) {
                Path selected = Files.createDirectory(output.resolve(expected + "-" + observed));
                String[] wanted = absent[expected], actual = absent[observed];
                if (expected == observed) KafkaLifecycleInspection.launch(selected, wanted, actual, new String[]{"sh"}, Map.of());
                else assertThrows(IOException.class, () -> KafkaLifecycleInspection.launch(selected, wanted, actual, new String[]{"sh"}, Map.of()));
                var receipt = JSON.readTree(selected.resolve("created-launch.json").toFile());
                assertTrue(receipt.has("expectedEntrypoint")); assertTrue(receipt.has("entrypoint"));
                assertEquals(expected == 0, receipt.path("expectedEntrypoint").isNull());
                assertEquals(observed == 0, receipt.path("entrypoint").isNull());
                if (expected == 1) assertEquals(JSON.createArrayNode(), receipt.path("expectedEntrypoint"));
                if (observed == 1) assertEquals(JSON.createArrayNode(), receipt.path("entrypoint"));
            }
        }
    }

    @Test void parsesObservedPropertiesAndKeepsJavaDuplicateSemantics() throws Exception {
        byte[] raw = ("# observed file\nnode.id=7\nnode.id=1\nprocess.roles=broker,controller\n"
                + "log.retention.ms=3600000\nunselected=value\n").getBytes(StandardCharsets.ISO_8859_1);
        var actual = KafkaLifecycleInspection.brokerProperties(raw, Set.of("node.id", "process.roles", "log.retention.ms"));
        assertEquals(Map.of("node.id", "1", "process.roles", "broker,controller", "log.retention.ms", "3600000"), actual);
        assertNull(actual.getProperty("unselected"));
        assertFalse(KafkaLifecycleInspection.brokerProperties(new byte[0], Set.of("node.id")).containsKey("node.id"));
    }

    @Test void preservesSuccessfulTransferBeforeMalformedPropertiesFail(@TempDir Path output) throws Exception {
        byte[] raw = "node.id=\\uZZZZ\n".getBytes(StandardCharsets.ISO_8859_1);
        byte[] observed = KafkaLifecycleInspection.transfer(output, "broker-config", "broker-config.raw", "/actual/file",
                sink -> sink.write(raw));
        assertThrows(IllegalArgumentException.class, () -> KafkaLifecycleInspection.brokerProperties(observed, Set.of("node.id")));
        assertArrayEquals(raw, Files.readAllBytes(output.resolve("broker-config.raw")));
        var receipt = JSON.readTree(output.resolve("broker-config.transfer.json").toFile());
        assertEquals("completed", receipt.path("status").asText());
        assertEquals(raw.length, receipt.path("bytesCaptured").asInt());
        assertEquals("/actual/file", receipt.path("source").asText());
    }

    @Test void preservesPartialTransferAndOriginalFailure(@TempDir Path output) throws Exception {
        IOException failure = new IOException("archive read failed");
        assertSame(failure, assertThrows(IOException.class, () -> KafkaLifecycleInspection.transfer(
                output, "broker-config", "broker-config.raw", "/actual/file", sink -> {
                    sink.write(new byte[]{0, 13, -1}); throw failure;
                })));
        assertArrayEquals(new byte[]{0, 13, -1}, Files.readAllBytes(output.resolve("broker-config.raw")));
        var receipt = JSON.readTree(output.resolve("broker-config.transfer.json").toFile());
        assertEquals("failed", receipt.path("status").asText());
        assertEquals(3, receipt.path("bytesCaptured").asInt());
        assertTrue(receipt.path("error").asText().contains("archive read failed"));
    }

    @Test void preservesTransferApiFailureWithNoInventedExitCode(@TempDir Path output) throws Exception {
        var failure = new IllegalStateException("Docker archive unavailable");
        assertSame(failure, assertThrows(IllegalStateException.class, () -> KafkaLifecycleInspection.transfer(
                output, "starter", "actual-starter.sh", "/actual/starter", sink -> { throw failure; })));
        var receipt = JSON.readTree(output.resolve("starter.transfer.json").toFile());
        assertEquals(0, receipt.path("bytesCaptured").asInt());
        assertFalse(receipt.has("exitCode"));
        assertEquals(failure.getClass().getName(), receipt.path("errorType").asText());
    }

    @Test void retainsCommandFailureStatusStdoutAndStderrBeforeRejecting(@TempDir Path output) throws Exception {
        assertThrows(IOException.class, () -> KafkaLifecycleInspection.command(output, "pid1-stat", List.of("/missing/tool"),
                () -> execResult(127, "partial\0output\n", "tool: not found\n")));
        assertEquals("partial\0output\n", Files.readString(output.resolve("pid1-stat.stdout")));
        assertEquals("tool: not found\n", Files.readString(output.resolve("pid1-stat.stderr")));
        var receipt = JSON.readTree(output.resolve("pid1-stat.command.json").toFile());
        assertEquals(127, receipt.path("exitCode").asInt());
        assertEquals("completed", receipt.path("status").asText());
    }

    @Test void retainsCommandApiFailureAndSuccessfulEmptyStderr(@TempDir Path output) throws Exception {
        var failure = new IOException("exec transport failed");
        assertSame(failure, assertThrows(IOException.class, () -> KafkaLifecycleInspection.command(
                output, "failed", List.of("/tool"), () -> { throw failure; })));
        var receipt = JSON.readTree(output.resolve("failed.command.json").toFile());
        assertEquals("failed", receipt.path("status").asText()); assertFalse(receipt.has("exitCode"));
        assertEquals("exec transport failed", receipt.path("errorMessage").asText());
        assertEquals("value", KafkaLifecycleInspection.command(output, "success", List.of("/tool"),
                () -> execResult(0, "value", "")));
        assertEquals(0, Files.size(output.resolve("success.stderr")));
    }

    @Test void parsesNamespaceIdentityWithoutShellTextUtilities() throws Exception {
        String selected = KafkaLifecycleInspection.processIdentity(STATUS, STAT, CMDLINE, "pid:[4026532441]\n");
        assertTrue(selected.contains("Name:\tjava\n")); assertTrue(selected.contains("Pid:\t1\n"));
        assertTrue(selected.contains("starttime_ticks=12345\n"));
        assertTrue(selected.contains("pid_namespace=pid:[4026532441]\n"));
        assertTrue(selected.contains("cmdline=/opt/java/bin/java -Xmx1g kafka.Kafka /config/server.properties \n"));
    }

    @Test void rejectsMissingOrMalformedProcessIdentity() {
        for (String status : List.of("", STATUS.replace("java", "sh"), STATUS.replace("Pid:\t1", "Pid:\t2")))
            assertThrows(IOException.class, () -> KafkaLifecycleInspection.processIdentity(status, STAT, CMDLINE, "pid:[1]"));
        for (String stat : List.of("", "1 java malformed", STAT.replace("12345", "missing"), STAT.replace("1 (", "2 (")))
            assertThrows(IOException.class, () -> KafkaLifecycleInspection.processIdentity(STATUS, stat, CMDLINE, "pid:[1]"));
        for (String namespace : List.of("", "mnt:[1]", "pid:[broken]"))
            assertThrows(IOException.class, () -> KafkaLifecycleInspection.processIdentity(STATUS, STAT, CMDLINE, namespace));
        assertThrows(IOException.class, () -> KafkaLifecycleInspection.processIdentity(STATUS, STAT, "shell\0", "pid:[1]"));
    }

    @Test void retainsOtherInspectionApiFailuresAndReturnedEvidence(@TempDir Path output) throws Exception {
        var failure = new IOException("top unavailable");
        assertSame(failure, assertThrows(IOException.class, () -> KafkaLifecycleInspection.observation(
                output, "host-top-failed", () -> { throw failure; }, value -> Map.of())));
        assertEquals("top unavailable", JSON.readTree(output.resolve("host-top-failed.json").toFile()).path("errorMessage").asText());
        String observed = KafkaLifecycleInspection.observation(output, "host-top", () -> "unexpected command", value -> Map.of("command", value));
        assertEquals("unexpected command", observed);
        assertEquals(observed, JSON.readTree(output.resolve("host-top.json").toFile()).path("command").asText());
    }

    private static Container.ExecResult execResult(int exitCode, String stdout, String stderr) throws Exception {
        var constructor = Container.ExecResult.class.getDeclaredConstructor(int.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(exitCode, stdout, stderr);
    }
}
