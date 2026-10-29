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
}
