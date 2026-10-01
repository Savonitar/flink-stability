package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class RetainedFlinkLogTest {
    @TempDir Path directory;
    @Test void retainsChunksWithoutDuplicationAndBoundsDiskUse() throws Exception {
        var log = new RetainedFlinkLog(new FlinkClassLoadLog("taskmanager-1#2", directory.resolve("classes.log")));
        log.created("container-1");
        log.accept("abc".getBytes());
        log.accept("def\n".getBytes());
        assertEquals("abcdef\n", Files.readString(log.snapshot().path()));
        log.accept(new byte[(int) FlinkComponentLog.MAX_BYTES]);
        assertTrue(log.snapshot().truncated());
        assertEquals(FlinkComponentLog.MAX_BYTES, Files.size(log.snapshot().path()));
        log.created("container-2");
        assertTrue(log.snapshot().error().isPresent());
    }
    @Test void snapshotFlushesAndCloseRetainsFinalBytesWithoutReopening() throws Exception {
        var log = new RetainedFlinkLog(new FlinkClassLoadLog("tm#1", directory.resolve("classes.log")));
        log.created("id");
        log.accept("before".getBytes());
        Path path = log.snapshot().path();
        assertEquals("before", Files.readString(path));
        log.accept(" after".getBytes());
        log.close();
        assertEquals("before after", Files.readString(path));
        log.accept(" ignored".getBytes());
        log.close();
        assertEquals("before after", Files.readString(path));
        assertTrue(log.snapshot().error().isEmpty());
        log.created("replacement");
        assertTrue(log.snapshot().error().isPresent(), "A closed collector cannot silently label replacement output");
    }
    @Test void captureErrorsAreDiagnosticsAndNeverEscape() {
        var log = new RetainedFlinkLog(new FlinkClassLoadLog("tm#1", directory.resolve("missing/classes.log")));
        assertDoesNotThrow(() -> log.created("id"));
        assertDoesNotThrow(() -> log.accept(new byte[10]));
        assertTrue(log.snapshot().error().isPresent());
    }
}
