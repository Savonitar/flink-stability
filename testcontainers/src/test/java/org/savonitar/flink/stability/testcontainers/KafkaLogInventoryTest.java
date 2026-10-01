package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class KafkaLogInventoryTest {
    private static final String ID = "1".repeat(64), NETWORK = "2".repeat(64), IMAGE = "sha256:" + "3".repeat(64);
    private static final String LOG = "00000000000000000000.log";
    private static final OwnedKafkaArchiveCapture.Binding BINDING = new OwnedKafkaArchiveCapture.Binding(
            ID, NETWORK, "kafka-main", "main", "apache/kafka:4.0.0", 1);
    private static final OwnedKafkaArchiveCapture.Partition PARTITION = new OwnedKafkaArchiveCapture.Partition(
            Set.of("/tmp/kafka-logs"), "/tmp/kafka-logs", "output", 0);
    @TempDir Path directory;

    @Test void inventoryBindsExactLogAndOmitsIndexPayloads() {
        FakeDriver driver = new FakeDriver();
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        var inventory = capture(driver, identity);
        assertEquals(KafkaLogInventory.Status.OBSERVED, inventory.status());
        assertEquals(2, inventory.entries().size());
        assertEquals(0, driver.opened.get());
        assertEquals(4, driver.calls.get()); // Names, root metadata, log metadata, index metadata.
        assertEquals(128L * 1024 * 1024, inventory.entries().stream().filter(e -> !e.log()).findFirst().orElseThrow().size());
        assertThrows(IllegalArgumentException.class, () -> inventory.requireLog(identity, "00000000000000000000.index"));
        assertThrows(IllegalArgumentException.class, () -> inventory.requireLog(identity, "../" + LOG));
        var forged = new OwnedKafkaArchiveCapture.Identity(identity.owner(), identity.imageId(), identity.observedAt());
        assertThrows(IllegalArgumentException.class, () -> inventory.requireLog(forged, LOG));
        var receipt = OwnedKafkaArchiveCapture.copyLog(BINDING, driver, identity, inventory, LOG,
                directory, "log.tar.part", 4096, budget());
        assertEquals(OwnedKafkaArchiveCapture.Status.TRANSPORT_EOF, receipt.status());
        assertEquals(PARTITION.directory() + "/" + LOG, driver.openedPath);
        byte[] transcript = inventory.transcript(); transcript[0]++;
        assertNotEquals(transcript[0], inventory.transcript()[0]);
        assertEquals(KafkaLogInventory.Comparison.UNCHANGED_METADATA, inventory.compare(capture(driver, identity)));
    }

    @Test void appendReplacementAndMissingAfterObservationDoNotClaimStableCoverage() {
        FakeDriver driver = new FakeDriver();
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        var before = capture(driver, identity);
        driver.logStat = "81a4 11 100 2\n";
        assertEquals(KafkaLogInventory.Comparison.CHANGED, before.compare(capture(driver, identity)));
        driver.logStat = "81a4 10 100 3\n";
        assertEquals(KafkaLogInventory.Comparison.CHANGED, before.compare(capture(driver, identity)));
        assertEquals(KafkaLogInventory.Comparison.UNAVAILABLE, before.compare(null));
        driver.names = PARTITION.directory() + "\0";
        assertEquals(KafkaLogInventory.Comparison.CHANGED, before.compare(capture(driver, identity)));
        driver.names = PARTITION.directory() + "\0" + PARTITION.directory() + "/" + LOG + ".swap\0";
        var unstable = capture(driver, identity);
        assertEquals(KafkaLogInventory.Status.UNSTABLE, unstable.status());
        assertEquals(KafkaLogInventory.Comparison.UNAVAILABLE, before.compare(unstable));
        assertThrows(IllegalArgumentException.class, () -> unstable.requireLog(identity, LOG));
    }

    @Test void unsafeDuplicateOversizedAndNonRegularEntriesFailClosed() {
        for (String suffix : List.of("../escape", LOG + "\0" + PARTITION.directory() + "/" + LOG,
                "99999999999999999999.log", "a/b", "name\nforged", "bad.log")) {
            FakeDriver driver = new FakeDriver();
            driver.names = PARTITION.directory() + "\0" + PARTITION.directory() + "/" + suffix + "\0";
            var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
            assertEquals(KafkaLogInventory.Status.INVALID, capture(driver, identity).status(), suffix);
            assertEquals(0, driver.opened.get());
        }
        for (String stat : List.of("a1ff 10 100 2\n", "41ed 10 100 2\n", "61b0 10 100 2\n", "81a4 nope 100 2\n")) {
            FakeDriver driver = new FakeDriver(); driver.logStat = stat;
            var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
            assertEquals(KafkaLogInventory.Status.INVALID, capture(driver, identity).status());
        }
        FakeDriver driver = new FakeDriver(); driver.logStat = "81a4 16777217 100 2\n";
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        var inventory = capture(driver, identity);
        assertThrows(IllegalArgumentException.class, () -> OwnedKafkaArchiveCapture.copyLog(BINDING,
                driver, identity, inventory, LOG, directory, "oversize.part", 1024, budget()));
        assertEquals(0, driver.opened.get());
    }

    @Test void outputBoundsCommandFailureAndIdentityChangesRemainIncomplete() {
        FakeDriver driver = new FakeDriver();
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        var limited = KafkaLogInventory.capture(BINDING, driver, identity, PARTITION, 32, budget());
        assertEquals(KafkaLogInventory.Status.BYTE_LIMIT, limited.status());
        assertTrue(limited.transcript().length <= 32);
        driver.commandExit = 1;
        assertEquals(KafkaLogInventory.Status.IO_FAILURE, capture(driver, identity).status());
        driver.commandExit = 0; driver.finished = false;
        var abandoned = capture(driver, identity);
        assertEquals(KafkaLogInventory.Status.ABANDONED, abandoned.status());
        assertFalse(abandoned.workerFinished());
        driver.finished = true; driver.image = "sha256:" + "4".repeat(64);
        assertEquals(KafkaLogInventory.Status.INVALID, capture(driver, identity).status());
    }

    @Test void cancelledNonInterruptibleListingCannotIssueLaterMetadataCalls() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var exited = new CountDownLatch(1);
            FakeDriver driver = new FakeDriver() {
                @Override public KafkaLogInventory.Command listFileNames(String path, int max, ContainerOperationDeadline deadline, Runnable checkActive) {
                    entered.countDown();
                    while (true) { try { release.await(); break; } catch (InterruptedException ignored) { } }
                    try { return super.listFileNames(path, max, deadline, checkActive); } finally { exited.countDown(); }
                }
            };
            var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
            var executor = Executors.newSingleThreadExecutor();
            try {
                var result = executor.submit(() -> KafkaLogInventory.capture(BINDING, driver, identity, PARTITION,
                        65536, MonotonicDeadline.start(Duration.ofMillis(200), System::nanoTime)));
                assertTrue(entered.await(1, TimeUnit.SECONDS));
                var receipt = result.get(1, TimeUnit.SECONDS);
                assertEquals(KafkaLogInventory.Status.ABANDONED, receipt.status());
                release.countDown(); assertTrue(exited.await(1, TimeUnit.SECONDS));
                executor.shutdown(); assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
                assertEquals(0, driver.statCalls.get());
            } finally { release.countDown(); executor.shutdownNow(); }
        });
    }

    @Test void callbackSharesStdoutStderrBoundAndNeverCompletesOverflow() throws Exception {
        try (var callback = new KafkaInventoryCommand.Output(4)) {
            callback.onNext(new Frame(StreamType.STDOUT, new byte[] {1, 2, 3}));
            callback.onNext(new Frame(StreamType.STDERR, new byte[] {4, 5}));
            callback.onComplete();
            var result = callback.result(0, true);
            assertArrayEquals(new byte[] {1, 2, 3}, result.stdout());
            assertArrayEquals(new byte[] {4}, result.stderr());
            assertFalse(result.finished());
        }
    }

    @Test void fatalArchiveReadKeepsThrowingCloseSuppressed() {
        FakeDriver driver = new FakeDriver() {
            @Override public InputStream openArchive(String path) {
                return new InputStream() {
                    @Override public int read() { throw new AssertionError("primary-read"); }
                    @Override public void close() throws IOException { throw new IOException("close-failure"); }
                };
            }
        };
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        var inventory = capture(driver, identity);
        var failure = assertThrows(AssertionError.class, () -> OwnedKafkaArchiveCapture.copyLog(BINDING,
                driver, identity, inventory, LOG, directory, "fatal.part", 4096, budget()));
        assertEquals("primary-read", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("close-failure", failure.getSuppressed()[0].getMessage());
    }

    private static KafkaLogInventory capture(FakeDriver driver, OwnedKafkaArchiveCapture.Identity identity) {
        return KafkaLogInventory.capture(BINDING, driver, identity, PARTITION, 65536, budget());
    }
    private static MonotonicDeadline budget() { return MonotonicDeadline.start(Duration.ofSeconds(2), System::nanoTime); }
    private static byte[] ascii(String text) { return text.getBytes(StandardCharsets.US_ASCII); }

    private static class FakeDriver implements OwnedKafkaArchiveCapture.Driver {
        String names = PARTITION.directory() + "\0" + PARTITION.directory() + "/" + LOG + "\0"
                + PARTITION.directory() + "/00000000000000000000.index\0";
        String logStat = "81a4 10 100 2\n", image = IMAGE, openedPath;
        long commandExit;
        boolean finished = true;
        AtomicInteger calls = new AtomicInteger(), statCalls = new AtomicInteger(), opened = new AtomicInteger();
        @Override public OwnedKafkaArchiveCapture.Inspection inspect() {
            return new OwnedKafkaArchiveCapture.Inspection(ID, image, NETWORK, List.of("kafka-main"), true);
        }
        @Override public InputStream openArchive(String path) { opened.incrementAndGet(); openedPath = path; return new ByteArrayInputStream(new byte[] {1, 2}); }
        @Override public KafkaLogInventory.Command listFileNames(String path, int max, ContainerOperationDeadline deadline, Runnable checkActive) {
            assertEquals(PARTITION.directory(), path); calls.incrementAndGet();
            return new KafkaLogInventory.Command(ascii(names), new byte[0], commandExit, finished);
        }
        @Override public KafkaLogInventory.Command fileMetadata(String path, int max, ContainerOperationDeadline deadline, Runnable checkActive) {
            calls.incrementAndGet(); statCalls.incrementAndGet();
            String stat = path.equals(PARTITION.directory()) ? "41ed 4096 100 1\n"
                    : path.endsWith(".index") ? "81a4 134217728 100 3\n" : logStat;
            return new KafkaLogInventory.Command(ascii(stat), new byte[0], 0, true);
        }
    }
}
