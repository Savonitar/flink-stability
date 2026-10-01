package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class SelectiveKafkaLogCaptureTest {
    private static final String LOG = "00000000000000000000.log";
    private static final String ID = "1".repeat(64), NETWORK = "2".repeat(64), IMAGE = "sha256:" + "3".repeat(64);
    private static final OwnedKafkaArchiveCapture.Binding OWNER = new OwnedKafkaArchiveCapture.Binding(
            ID, NETWORK, "kafka-main", "main", "apache/kafka:4.0.0", 1);
    @TempDir Path directory;
    @Test void inventoriesBothTopicsAndCopiesOnlyOwnedLogFiles() {
        Driver driver = new Driver();
        var result = capture(driver, List.of(new KafkaLogCapture.Partition("input", 0), new KafkaLogCapture.Partition("output", 0)));
        assertTrue(result.diagnostics().isEmpty(), result.diagnostics().toString());
        assertEquals(2, result.inventories().size()); assertEquals(2, result.archives().size());
        assertEquals(List.of("/tmp/kafka-logs/input-0/" + LOG, "/tmp/kafka-logs/output-0/" + LOG), driver.opened);
        assertTrue(result.archives().stream().allMatch(a -> a.containerId().equals(ID) && a.workerFinished()));
        assertTrue(result.reservedCalls() <= KafkaLogCapture.MAX_CALLS);
        assertTrue(result.retainedBytes() <= KafkaLogCapture.MAX_BYTES);
    }
    @Test void sharedCallLimitStopsFurtherArchivesAndForeignIdentityCannotCopy() {
        Driver driver = new Driver();
        List<KafkaLogCapture.Partition> partitions = new ArrayList<>();
        for (int i = 0; i < 128; i++) partitions.add(new KafkaLogCapture.Partition("input", i));
        var result = capture(driver, partitions);
        assertFalse(result.diagnostics().isEmpty()); assertTrue(result.reservedCalls() <= 512);
        assertTrue(result.archives().size() < 128);
        driver.foreign = true; driver.opened.clear();
        result = capture(driver, List.of(new KafkaLogCapture.Partition("output", 0)));
        assertTrue(result.archives().isEmpty()); assertTrue(driver.opened.isEmpty());
    }
    @Test void archiveBytesShareOneBudgetAcrossPartitions() {
        var driver = new Driver(); driver.archiveBytes = 16 * 1024 * 1024;
        var partitions = java.util.stream.IntStream.range(0, 6)
                .mapToObj(i -> new KafkaLogCapture.Partition("output", i)).toList();
        var result = capture(driver, partitions);
        assertTrue(result.retainedBytes() <= KafkaLogCapture.MAX_BYTES);
        assertEquals(4, driver.opened.size());
        assertFalse(result.diagnostics().isEmpty());
        assertNotEquals("TRANSPORT_EOF", result.archives().getLast().status());
    }
    @Test void expiredBudgetAndUnfinishedInventoryStopWithoutCopying() {
        Driver driver = new Driver(); driver.unfinished = true;
        var result = capture(driver, List.of(new KafkaLogCapture.Partition("output", 0)));
        assertFalse(result.diagnostics().isEmpty()); assertTrue(result.archives().isEmpty());
        java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
        var deadline = MonotonicDeadline.start(Duration.ofSeconds(1), clock::get); clock.set(2_000_000_000L);
        result = SelectiveKafkaLogCapture.collect(OWNER, driver, List.of(new KafkaLogCapture.Partition("output", 0)), directory, deadline);
        assertEquals(0, result.reservedCalls()); assertTrue(result.archives().isEmpty());
    }
    private KafkaLogCapture capture(Driver driver, List<KafkaLogCapture.Partition> partitions) {
        return SelectiveKafkaLogCapture.collect(OWNER, driver, partitions, directory,
                MonotonicDeadline.start(Duration.ofSeconds(5), System::nanoTime));
    }
    private static class Driver implements OwnedKafkaArchiveCapture.Driver {
        final List<String> opened = new ArrayList<>(); boolean foreign, unfinished;
        int archiveBytes = 1536;
        public OwnedKafkaArchiveCapture.Inspection inspect() {
            return new OwnedKafkaArchiveCapture.Inspection(foreign ? "4".repeat(64) : ID, IMAGE, NETWORK, List.of("kafka-main"), true);
        }
        public InputStream openArchive(String path) { opened.add(path); return new ByteArrayInputStream(new byte[archiveBytes]); }
        public KafkaLogInventory.Command listFileNames(String path, int max, ContainerOperationDeadline deadline, Runnable active) {
            return command(path + "\0" + path + "/" + LOG + "\0" + path + "/00000000000000000000.index\0");
        }
        public KafkaLogInventory.Command fileMetadata(String path, int max, ContainerOperationDeadline deadline, Runnable active) {
            return command(path.endsWith(".log") ? "81a4 0 100 1\n" : path.endsWith(".index") ? "81a4 134217728 100 2\n" : "41ed 0 100 3\n");
        }
        private KafkaLogInventory.Command command(String value) {
            return new KafkaLogInventory.Command(value.getBytes(StandardCharsets.US_ASCII), new byte[0], 0, !unfinished);
        }
    }
}
