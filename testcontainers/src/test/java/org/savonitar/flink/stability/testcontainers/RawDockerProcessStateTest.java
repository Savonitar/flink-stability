package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.RawEvidenceStore;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class RawDockerProcessStateTest {
    @TempDir Path temporary;
    static final String ID = "a".repeat(64);
    static final String STATE = "{ \"Running\" : false, \"Paused\":false, \"OOMKilled\":false,\"ExitCode\":137,\"FinishedAt\":\"2026-10-29T00:00:00Z\" }";
    static String body(String id) { return "{\"Id\":\"" + id + "\",\"Config\":{\"Env\":[\"GENERATED_FIXTURE=excluded\"]},\"State\":" + STATE + "}"; }
    static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }

    @Test void oneRequestBindsExactStateSourceTokensWithoutContainerConfiguration() throws Exception {
        var calls = new AtomicInteger();
        var store = new RawEvidenceStore(temporary.toRealPath().resolve("raw"));
        var reader = new RawDockerProcessState(store, "1.44", (endpoint, timeout) -> {
            assertEquals("/v1.44/containers/" + ID + "/json", endpoint); calls.incrementAndGet();
            return new RawDockerProcessState.Response(200, bytes(body(ID)));
        });
        var state = reader.inspect(ID, ContainerOperationDeadline.start("test", Duration.ofSeconds(2), System::nanoTime));
        assertFalse(state.running()); assertEquals(137, state.exitCode().orElseThrow()); assertEquals(1, calls.get());
        String retained = Files.readString(Path.of(state.rawEvidence().orElseThrow().path()));
        assertEquals("{\"Id\":\"" + ID + "\",\"State\":" + STATE + "}\n", retained);
        assertFalse(retained.contains("Config")); assertFalse(retained.contains("GENERATED_FIXTURE"));
    }

    @Test void duplicateTrailingIncompleteAndCoercedStatesFailClosed() throws Exception {
        for (String changed : new String[]{body(ID) + " {}", body(ID).replace("\"Id\":", "\"Id\":\"x\",\"Id\":"),
                body(ID).replace("false", "\"false\""), body("b".repeat(64)), "{\"Id\":\"" + ID + "\"}"}) {
            var store = new RawEvidenceStore(Files.createTempDirectory(temporary.toRealPath(), "case").resolve("raw"));
            var reader = new RawDockerProcessState(store, "1.44", (path, timeout) -> new RawDockerProcessState.Response(200, bytes(changed)));
            assertThrows(RuntimeException.class, () -> reader.inspect(ID,
                    ContainerOperationDeadline.start("test", Duration.ofSeconds(2), System::nanoTime)));
        }
    }

    @Test void remoteEndpointAndCaptureCollisionCannotBecomePhysicalDeath() throws Exception {
        var store = new RawEvidenceStore(temporary.toRealPath().resolve("raw"));
        assertThrows(IllegalArgumentException.class, () -> new RawDockerProcessState(store, java.net.URI.create("http://example.invalid"), "1.44"));
        Files.writeString(temporary.toRealPath().resolve("raw/000001.json"), "existing");
        var reader = new RawDockerProcessState(store, "1.44", (path, timeout) -> new RawDockerProcessState.Response(200, bytes(body(ID))));
        assertThrows(RuntimeException.class, () -> reader.inspect(ID,
                ContainerOperationDeadline.start("test", Duration.ofSeconds(2), System::nanoTime)));
    }
}
