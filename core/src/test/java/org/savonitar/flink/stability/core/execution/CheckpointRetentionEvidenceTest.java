package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.Digests;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

class CheckpointRetentionEvidenceTest {
    @TempDir Path temporary;
    @BeforeEach void realRoot() throws Exception { temporary = temporary.toRealPath(); }

    @Test void copiesCheckpointAndHaBytesOnlyAfterFenceAndRetainsExactReceipts() throws Exception {
        Path source = Files.createDirectories(temporary.resolve("attempt"));
        Files.writeString(Files.createDirectories(source.resolve("job/chk-7")).resolve("_metadata"), "checkpoint");
        Files.writeString(Files.createDirectories(source.resolve("ha")).resolve("jobgraph"), "ha-state");
        Path output = temporary.resolve("retained");
        var notFenced = CheckpointRetentionEvidence.copy(source, output, false);
        assertEquals("not-copied", notFenced.status());
        assertFalse(Files.exists(output));
        var result = CheckpointRetentionEvidence.copy(source, output, true);
        assertEquals("complete", result.status(), result.diagnostics().toString());
        assertEquals(output.resolve("checkpoints/ha").toString(), result.haRoot());
        assertEquals(2, result.files().size());
        assertEquals(18, result.bytes());
        for (var file : result.files()) {
            Path path = output.resolve(file.path());
            assertEquals(file.size(), Files.size(path));
            try (var input = Files.newInputStream(path)) { assertEquals(file.sha256(), Digests.sha256(input)); }
        }
        assertEquals("checkpoint", Files.readString(source.resolve("job/chk-7/_metadata")), "Original state remains untouched");
        assertEquals("incomplete", CheckpointRetentionEvidence.copy(source, output, true).status(), "Never overwrite previous evidence");
    }

    @Test void overlapLinksMissingSourcesAndInterruptedCopiesAreExplicitlyIncomplete() throws Exception {
        Path source = Files.createDirectory(temporary.resolve("attempt"));
        assertEquals("incomplete", CheckpointRetentionEvidence.copy(source, source.resolve("inside"), true).status());
        assertEquals("incomplete", CheckpointRetentionEvidence.copy(temporary.resolve("missing"), temporary.resolve("missing-copy"), true).status());
        Files.createSymbolicLink(source.resolve("link"), temporary.resolve("outside"));
        var linked = CheckpointRetentionEvidence.copy(source, temporary.resolve("linked-copy"), true);
        assertEquals("incomplete", linked.status());
        assertTrue(linked.diagnostics().getFirst().contains("Nonregular"));
        Files.delete(source.resolve("link"));
        Files.writeString(source.resolve("checkpoint"), "bytes");
        Thread.currentThread().interrupt();
        try { assertEquals("incomplete", CheckpointRetentionEvidence.copy(source, temporary.resolve("interrupted"), true).status()); }
        finally { Thread.interrupted(); }
    }
}
