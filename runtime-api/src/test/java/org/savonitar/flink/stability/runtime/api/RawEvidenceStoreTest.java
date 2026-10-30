package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class RawEvidenceStoreTest {
    @TempDir Path temporary;

    @Test void preservesExactBytesAndIndependentImmutableReferences() throws Exception {
        Path root = temporary.toRealPath().resolve("raw");
        var store = new RawEvidenceStore(root);
        byte[] bytes = "{ \"value\": \"é\" }\n".getBytes(StandardCharsets.UTF_8);
        var ref = store.retain(bytes, "test", "call-1", "GET", "/owned", 200, 10, 20);
        assertArrayEquals(bytes, Files.readAllBytes(Path.of(ref.path())));
        assertEquals(Digests.sha256(Path.of(ref.path())), ref.sha256());
        assertEquals(bytes.length, ref.bytes());
        assertEquals(1, store.cursor());
        assertThrows(UnsupportedOperationException.class, () -> store.since(0).clear());
        assertThrows(IOException.class, () -> new RawEvidenceStore(root));
        assertEquals("rw-------", java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(Path.of(ref.path()))));
    }

    @Test void writeCollisionAndOversizePermanentlyFailClosedWithoutOverwrite() throws Exception {
        var store = new RawEvidenceStore(temporary.toRealPath().resolve("collision"));
        Files.writeString(temporary.toRealPath().resolve("collision/000001.json"), "existing");
        assertThrows(IOException.class, () -> store.retain("{}".getBytes(), "test", "x", "GET", "/", 200, 0, 1));
        assertEquals("existing", Files.readString(temporary.toRealPath().resolve("collision/000001.json")));
        assertThrows(IOException.class, () -> store.retain("{}".getBytes(), "test", "x", "GET", "/", 200, 0, 1));
        var bounded = new RawEvidenceStore(temporary.toRealPath().resolve("bounded"));
        assertThrows(IOException.class, () -> bounded.retain(new byte[RawEvidenceStore.MAX_RESPONSE_BYTES + 1], "test", "x", "GET", "/", 200, 0, 1));
        assertEquals(0, bounded.cursor());
    }

    @Test void rejectsSymlinkParents() throws Exception {
        Path root = temporary.toRealPath();
        Files.createDirectory(root.resolve("real")); Files.createSymbolicLink(root.resolve("link"), root.resolve("real"));
        assertThrows(IOException.class, () -> new RawEvidenceStore(root.resolve("link/raw")));
    }

    @Test void emptyBodyIsAnExactReceiptAndDoesNotPoisonLaterCaptures() throws Exception {
        var store = new RawEvidenceStore(temporary.toRealPath().resolve("empty"));
        var empty = store.retain(new byte[0], "test", "empty", "GET", "/", 503, 1, 2);
        assertEquals(0, empty.bytes());
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", empty.sha256());
        assertEquals(0, Files.size(Path.of(empty.path())));
        store.retain(new byte[]{1}, "test", "next", "GET", "/", 200, 3, 4);
        assertEquals(new RawEvidenceStore.Utilisation(2, 1, RawEvidenceStore.MAX_CAPTURES,
                RawEvidenceStore.MAX_TOTAL_BYTES, true), store.utilisation());
        assertThrows(IllegalArgumentException.class, () -> new RawEvidenceRef(empty.path(), empty.sha256(), -1,
                "test", 1, 1, 2, "group", "GET", "/", 200));
    }

    @Test void unselectedBatchesConsumeNoFilesSequencesOrPersistentBudget() throws Exception {
        var store = new RawEvidenceStore(temporary.toRealPath().resolve("selected"));
        for (int i = 0; i <= RawEvidenceStore.MAX_CAPTURES; i++) {
            try (var batch = store.batch()) {
                var ref = batch.retain(new byte[]{1}, "test", "poll-" + i, "GET", "/", 200, i, i);
                assertEquals(1, ref.sequence()); assertFalse(Files.exists(Path.of(ref.path())));
            }
        }
        assertEquals(0, store.cursor()); assertEquals(0, store.utilisation().bytes());
        RawEvidenceRef selected;
        byte[] bytes = {2};
        try (var batch = store.batch()) {
            selected = batch.retain(bytes, "test", "selected", "GET", "/", 200, 1, 2);
            bytes[0] = 99;
            batch.commit(); batch.commit();
        }
        assertArrayEquals(new byte[]{2}, Files.readAllBytes(Path.of(selected.path())));
        assertEquals(java.util.List.of(selected), store.since(0));
        assertEquals(1, store.utilisation().captures()); assertTrue(store.utilisation().complete());
    }

    @Test void selectedBatchWriteFailureRemainsVisibleAndNeverOverwritesEvidence() throws Exception {
        var store = new RawEvidenceStore(temporary.toRealPath().resolve("selected-failure"));
        try (var batch = store.batch()) {
            var first = batch.retain(new byte[]{1}, "test", "selected", "GET", "/", 200, 1, 2);
            var ref = batch.retain(new byte[]{2}, "test", "selected", "GET", "/next", 200, 3, 4);
            Files.writeString(Path.of(ref.path()), "reserved");
            assertThrows(IOException.class, batch::commit);
            assertEquals(java.util.List.of(first), store.since(0));
            assertArrayEquals(new byte[]{1}, Files.readAllBytes(Path.of(first.path())));
            assertEquals("reserved", Files.readString(Path.of(ref.path())));
        }
        assertEquals(1, store.utilisation().captures()); assertEquals(1, store.utilisation().bytes());
        assertFalse(store.utilisation().complete());
        assertThrows(IOException.class, store::batch);
    }
}
