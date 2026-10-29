package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.RawEvidenceStore;
import org.savonitar.flink.stability.runtime.api.StorageFaultControl;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointInspectorRawEvidenceTest {
    @TempDir Path temporary;
    private static final String JOB = "a".repeat(32);
    private static final String PREFIX = "/flink/checkpoints/job_fixture/" + JOB + "/chk-7/";
    private static final String SHA = "b".repeat(64);
    private static final StorageFaultControl.Instance INSTANCE = new StorageFaultControl.Instance(
            "jobmanager-1", "jobmanager-1#1", "container-fixture", "instance-fixture", FlinkComponentRole.JOB_MANAGER, 25);

    @Test void typedProofBindsExactOriginalWhitespaceAndUtf8Bytes() throws Exception {
        RawEvidenceStore store = store("valid");
        byte[] source = (" \n" + receipt() + "\t\n").getBytes(StandardCharsets.UTF_8);
        var result = capture(store, source, 0);
        var raw = result.rawEvidence().orElseThrow();
        assertTrue(result.complete()); assertTrue(result.diagnostics().isEmpty());
        assertEquals(INSTANCE, result.instance()); assertEquals(JOB, result.jobId());
        assertEquals(7, result.checkpointId()); assertEquals("flink-stability-keyed-prefix-v1", result.operatorUid());
        assertEquals(PREFIX + "_metadata", result.metadata().path());
        assertEquals(PREFIX + "state-1", result.files().getFirst().path());
        assertEquals(128, result.files().getFirst().size()); assertEquals(SHA, result.files().getFirst().sha256());
        assertArrayEquals(source, Files.readAllBytes(Path.of(raw.path())));
        assertEquals(Digests.sha256(source), raw.sha256()); assertEquals(source.length, raw.bytes());
        assertEquals("checkpoint-inspector-stdout", raw.kind()); assertEquals("EXEC", raw.method());
        assertEquals("/containers/container-fixture/checkpoint-inspector/" + JOB + "/7", raw.endpoint());
        assertEquals(0, raw.status()); assertEquals(100, raw.startedNanos()); assertEquals(200, raw.completedNanos());
        assertEquals(1, raw.sequence()); assertEquals(raw, store.since(0).getFirst());
    }

    @Test void malformedBytesJsonAndNonzeroExitAreRetainedBeforeRejection() throws Exception {
        byte[][] invalid = {
                {(byte) 0xc0, (byte) 0x80},
                "{\"version\":1".getBytes(StandardCharsets.UTF_8),
                (receipt() + " {}").getBytes(StandardCharsets.UTF_8),
                receipt().replace("\"version\":1", "\"version\":1,\"version\":1").getBytes(StandardCharsets.UTF_8)
        };
        for (int i = 0; i < invalid.length; i++) {
            RawEvidenceStore store = store("invalid-" + i);
            byte[] source = invalid[i];
            assertThrows(IllegalStateException.class, () -> capture(store, source, 0));
            assertArrayEquals(source, Files.readAllBytes(Path.of(store.since(0).getFirst().path())));
            assertEquals(Digests.sha256(source), store.since(0).getFirst().sha256());
        }
        RawEvidenceStore failedExit = store("exit");
        byte[] valid = receipt().getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> capture(failedExit, valid, 1));
        assertEquals(1, failedExit.since(0).getFirst().status());
        assertArrayEquals(valid, Files.readAllBytes(Path.of(failedExit.since(0).getFirst().path())));
    }

    @Test void ForeignIdentityAndEscapedOrDuplicateFilesNeverProduceCompleteProof() throws Exception {
        String[] invalid = {
                receipt().replace("\"jobId\":\"" + JOB, "\"jobId\":\"" + "c".repeat(32)),
                receipt().replace("\"checkpointId\":7", "\"checkpointId\":8"),
                receipt().replace("state-1", "../state-1"),
                receipt().replace("state-1", "_metadata"),
                receipt().replace("\"files\":[" + file("state-1", 128) + "]",
                        "\"files\":[" + file("state-1", 128) + "," + file("state-1", 128) + "]"),
                receipt().replace("\"size\":128", "\"size\":0"),
                receipt().replace("\"files\":[" + file("state-1", 128) + "]", "\"files\":[]")
        };
        for (int i = 0; i < invalid.length; i++) {
            RawEvidenceStore store = store("foreign-" + i);
            byte[] source = invalid[i].getBytes(StandardCharsets.UTF_8);
            assertThrows(IllegalStateException.class, () -> capture(store, source, 0));
            assertEquals(Digests.sha256(source), store.since(0).getFirst().sha256());
        }
    }

    @Test void CaptureFailureAndBoundsCannotYieldTypedProofOrRecoverSilently() throws Exception {
        Path directory = temporary.toRealPath().resolve("failed");
        RawEvidenceStore store = new RawEvidenceStore(directory);
        Files.writeString(directory.resolve("000001.json"), "existing-evidence");
        byte[] valid = receipt().getBytes(StandardCharsets.UTF_8);
        assertThrows(UncheckedIOException.class, () -> capture(store, valid, 0));
        assertEquals(0, store.cursor());
        assertEquals("existing-evidence", Files.readString(directory.resolve("000001.json")));
        assertThrows(UncheckedIOException.class, () -> capture(store, valid, 0));
        RawEvidenceStore bounded = store("bounded");
        assertThrows(UncheckedIOException.class, () -> capture(bounded, new byte[RawEvidenceStore.MAX_RESPONSE_BYTES + 1], 0));
        assertEquals(0, bounded.cursor());
        assertThrows(UncheckedIOException.class, () -> capture(bounded, valid, 0));
    }

    private RawEvidenceStore store(String name) throws Exception {
        return new RawEvidenceStore(temporary.toRealPath().resolve(name));
    }

    private static StorageFaultControl.Materialization capture(RawEvidenceStore store, byte[] source, int exitCode) {
        return FuseCheckpointStorageRuntime.retainInspectorReceipt(source, exitCode, store,
                INSTANCE, "job_fixture", JOB, 7, 100, 200);
    }

    private static String receipt() {
        return "{\"version\":1,\"jobId\":\"" + JOB + "\",\"checkpointId\":7,"
                + "\"operatorUid\":\"flink-stability-keyed-prefix-v1\",\"fixture\":\"é\","
                + "\"metadata\":" + file("_metadata", 256) + ",\"files\":[" + file("state-1", 128) + "]}";
    }

    private static String file(String name, long size) {
        return "{\"path\":\"" + PREFIX + name + "\",\"size\":" + size + ",\"sha256\":\"" + SHA + "\"}";
    }
}
