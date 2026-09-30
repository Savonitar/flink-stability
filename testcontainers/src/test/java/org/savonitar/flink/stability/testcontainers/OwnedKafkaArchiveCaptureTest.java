package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.testcontainers.kafka.KafkaContainer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class OwnedKafkaArchiveCaptureTest {
    private static final String ID = "1".repeat(64);
    private static final String NETWORK = "2".repeat(64);
    private static final String IMAGE = "sha256:" + "3".repeat(64);
    private static final OwnedKafkaArchiveCapture.Binding BINDING = new OwnedKafkaArchiveCapture.Binding(
            ID, NETWORK, "kafka-main", "main", "apache/kafka:4.0.0", 1);
    private static final OwnedKafkaArchiveCapture.Partition PARTITION = new OwnedKafkaArchiveCapture.Partition(
            Set.of("/tmp/kafka-logs"), "/tmp/kafka-logs", "declared-output", 0);

    @TempDir Path directory;

    @Test void exactOwnedTransportIsHashedButDoesNotClaimTarValidity() throws Exception {
        byte[] bytes = "not-a-tar-but-transport-complete".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        FakeDriver driver = new FakeDriver(bytes);
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        var receipt = copy(driver, identity, "archive.part", bytes.length);
        assertEquals(OwnedKafkaArchiveCapture.Status.TRANSPORT_EOF, receipt.status());
        assertTrue(receipt.workerFinished()); assertEquals(bytes.length, receipt.observedBytes());
        assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), receipt.sha256().orElseThrow());
        assertArrayEquals(bytes, Files.readAllBytes(receipt.partialArchive()));
        assertEquals("/tmp/kafka-logs/declared-output-0", driver.path);
        assertEquals(3, driver.inspections.get()); assertEquals(1, driver.opened.get());
        assertEquals(1, driver.closed.get());
        assertThrows(IllegalArgumentException.class, () -> copy(driver, identity, "archive.part", bytes.length));
        assertArrayEquals(bytes, Files.readAllBytes(receipt.partialArchive()));
    }

    @Test void limitPlusOneAndIoFailureRetainOnlyBoundedPrefixes() throws Exception {
        FakeDriver driver = new FakeDriver(new byte[] {1, 2, 3, 4});
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        var capped = copy(driver, identity, "limit.part", 3);
        assertEquals(OwnedKafkaArchiveCapture.Status.BYTE_LIMIT, capped.status());
        assertEquals(3, capped.observedBytes()); assertTrue(capped.sha256().isEmpty());
        assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(capped.partialArchive()));
        driver.stream = new InputStream() {
            int reads;
            @Override public int read() throws IOException { throw new IOException("fixture"); }
            @Override public int read(byte[] b, int off, int len) throws IOException {
                if (reads++ == 0) { b[off] = 9; return 1; }
                throw new IOException("fixture");
            }
        };
        var failed = copy(driver, identity, "io.part", 10);
        assertEquals(OwnedKafkaArchiveCapture.Status.IO_FAILURE, failed.status());
        assertEquals(1, failed.observedBytes()); assertTrue(failed.sha256().isEmpty());
        assertArrayEquals(new byte[] {9}, Files.readAllBytes(failed.partialArchive()));
        driver.stream = new ByteArrayInputStream(new byte[0]);
        var empty = copy(driver, identity, "empty.part", 10);
        assertEquals(OwnedKafkaArchiveCapture.Status.TRANSPORT_EOF, empty.status());
        assertEquals(0, empty.observedBytes()); // EOF does not certify nonempty/valid tar.
    }

    @Test void identityMismatchBeforeAndAfterTransferIsNeverComplete() throws Exception {
        for (var changed : List.of(
                new OwnedKafkaArchiveCapture.Inspection("4".repeat(64), IMAGE, NETWORK, List.of("kafka-main"), true),
                new OwnedKafkaArchiveCapture.Inspection(ID, "sha256:" + "4".repeat(64), NETWORK, List.of("kafka-main"), true),
                new OwnedKafkaArchiveCapture.Inspection(ID, IMAGE, "4".repeat(64), List.of("kafka-main"), true),
                new OwnedKafkaArchiveCapture.Inspection(ID, IMAGE, NETWORK, List.of("wrong-alias"), true),
                new OwnedKafkaArchiveCapture.Inspection(ID, IMAGE, NETWORK, List.of("kafka-main"), false))) {
            FakeDriver driver = new FakeDriver(new byte[] {1});
            var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
            driver.changed = changed;
            var receipt = copy(driver, identity, "identity-" + driver.hashCode() + ".part", 10);
            assertEquals(OwnedKafkaArchiveCapture.Status.IDENTITY_MISMATCH, receipt.status());
            assertEquals(0, driver.opened.get()); assertTrue(receipt.sha256().isEmpty());
        }
        FakeDriver after = new FakeDriver(new byte[] {1, 2});
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, after, budget());
        after.changeAfterOpen = true;
        var receipt = copy(after, identity, "after.part", 10);
        assertEquals(OwnedKafkaArchiveCapture.Status.IDENTITY_MISMATCH, receipt.status());
        assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(receipt.partialArchive()));
        assertTrue(receipt.sha256().isEmpty());
    }

    @Test void unsafeRequestAndDestinationRejectBeforeArchiveOpen() throws Exception {
        for (String root : List.of("/", "/tmp/../kafka", "/tmp//kafka", "/tmp/kafka/", "/tmp\\kafka")) {
            assertThrows(IllegalArgumentException.class, () ->
                    new OwnedKafkaArchiveCapture.Partition(Set.of(root), root, "output", 0));
        }
        assertThrows(IllegalArgumentException.class, () -> new OwnedKafkaArchiveCapture.Partition(
                Set.of("/tmp/kafka-logs"), "/different", "output", 0));
        for (String topic : List.of("../private", "..", "a/b", "", "a\\b")) {
            assertThrows(IllegalArgumentException.class, () -> new OwnedKafkaArchiveCapture.Partition(
                    Set.of("/tmp/kafka-logs"), "/tmp/kafka-logs", topic, 0));
        }
        FakeDriver driver = new FakeDriver(new byte[] {1});
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        for (String name : List.of("../escape.part", "file.tar", "/absolute.part", "a/b.part")) {
            assertThrows(IllegalArgumentException.class, () -> copy(driver, identity, name, 10));
        }
        Path link = directory.resolve("linked"); Files.createSymbolicLink(link, directory);
        assertThrows(IllegalArgumentException.class, () -> OwnedKafkaArchiveCapture.copy(BINDING, driver,
                identity, PARTITION, link, "link.part", 10, budget()));
        assertThrows(IllegalArgumentException.class, () -> copy(driver, identity, "large.part", 128L * 1024 * 1024 + 1));
        assertEquals(0, driver.opened.get());
    }

    @Test void sharedDeadlineAndInterruptionNeverPublishLateHash() throws Exception {
        FakeDriver driver = new FakeDriver(new byte[] {1});
        var identity = OwnedKafkaArchiveCapture.observe(BINDING, driver, budget());
        AtomicLong now = new AtomicLong();
        var deadline = MonotonicDeadline.start(Duration.ofSeconds(1), now::get);
        driver.stream = new ByteArrayInputStream(new byte[] {1}) {
            @Override public synchronized int read(byte[] b, int off, int len) {
                int read = super.read(b, off, len); now.set(1_000_000_000L); return read;
            }
        };
        var expired = OwnedKafkaArchiveCapture.copy(BINDING, driver, identity, PARTITION,
                directory, "clock.part", 10, deadline);
        assertEquals(OwnedKafkaArchiveCapture.Status.ABANDONED_PARTIAL, expired.status());
        assertTrue(expired.sha256().isEmpty()); assertEquals(0, expired.observedBytes());
        Thread.currentThread().interrupt();
        try {
            var interrupted = copy(driver, identity, "interrupt.part", 10);
            assertEquals(OwnedKafkaArchiveCapture.Status.ABANDONED_PARTIAL, interrupted.status());
            assertTrue(Thread.currentThread().isInterrupted()); assertTrue(interrupted.sha256().isEmpty());
        } finally { Thread.interrupted(); }
    }

    @Test void stalledArchiveDoesNotPreventOwnerStopAndCannotPublishAfterCancellation() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
            CountDownLatch readExited = new CountDownLatch(1), stopped = new CountDownLatch(1);
            FakeDriver driver = new FakeDriver(new byte[] {1});
            driver.stream = new InputStream() {
                @Override public int read() { return -1; }
                @Override public int read(byte[] b, int off, int len) {
                    entered.countDown();
                    try { while (true) { try { release.await(); break; } catch (InterruptedException ignored) { } } }
                    finally { readExited.countDown(); }
                    b[off] = 7; return 1;
                }
                @Override public void close() { } // Deliberately ignores cancellation/closure.
            };
            FakeContainer container = new FakeContainer(stopped);
            ApacheKafkaRuntime runtime = new ApacheKafkaRuntime(target(), container, () -> NETWORK,
                    (id, network) -> { assertEquals(ID, id); assertEquals(NETWORK, network); return driver; });
            var executor = Executors.newSingleThreadExecutor();
            try {
                runtime.start();
                var identity = runtime.observeIdentity(budget());
                var capture = executor.submit(() -> runtime.copyOutputPartition(identity, PARTITION,
                        directory, "blocked.part", 10,
                        MonotonicDeadline.start(Duration.ofMillis(500), System::nanoTime)));
                assertTrue(entered.await(2, TimeUnit.SECONDS), "Worker reached blocking read");
                var receipt = capture.get(2, TimeUnit.SECONDS);
                assertEquals(OwnedKafkaArchiveCapture.Status.ABANDONED_PARTIAL, receipt.status());
                assertFalse(receipt.workerFinished()); assertTrue(receipt.sha256().isEmpty());
                runtime.stop(); // Must reach driver while old read still ignores interruption.
                assertTrue(stopped.await(1, TimeUnit.SECONDS)); assertEquals(1, release.getCount());
                assertThrows(IllegalArgumentException.class, () -> runtime.copyOutputPartition(identity,
                        PARTITION, directory, "stopped.part", 10, budget()));
                release.countDown(); assertTrue(readExited.await(1, TimeUnit.SECONDS));
                assertTrue(receipt.sha256().isEmpty());
                assertEquals(OwnedKafkaArchiveCapture.Status.ABANDONED_PARTIAL, receipt.status());
                assertEquals(0, receipt.observedBytes());
            } finally {
                release.countDown();
                assertTrue(readExited.await(1, TimeUnit.SECONDS));
                executor.shutdownNow(); assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            }
        });
    }

    @Test void ownerRequiresItsExactObservedStartupIdentityAndRejectsRestartGeneration() {
        FakeDriver driver = new FakeDriver(new byte[] {1});
        var container = new FakeContainer(new CountDownLatch(1));
        var owner = new ApacheKafkaRuntime(target(), container, () -> NETWORK, (id, network) -> driver);
        assertThrows(IllegalStateException.class, () -> owner.observeIdentity(budget()));
        owner.start(); var first = owner.observeIdentity(budget());
        var forged = new OwnedKafkaArchiveCapture.Identity(first.owner(), first.imageId(), first.observedAt());
        assertThrows(IllegalArgumentException.class, () -> owner.copyOutputPartition(forged, PARTITION,
                directory, "forged.part", 10, budget()));
        owner.stop(); owner.start(); owner.observeIdentity(budget());
        assertThrows(IllegalArgumentException.class, () -> owner.copyOutputPartition(first, PARTITION,
                directory, "old-generation.part", 10, budget()));
        owner.stop(); assertEquals(0, driver.opened.get());
    }

    private OwnedKafkaArchiveCapture.Receipt copy(FakeDriver driver, OwnedKafkaArchiveCapture.Identity identity,
                                                  String name, long limit) {
        return OwnedKafkaArchiveCapture.copy(BINDING, driver, identity, PARTITION, directory, name, limit, budget());
    }
    private static MonotonicDeadline budget() { return MonotonicDeadline.start(Duration.ofSeconds(2), System::nanoTime); }
    private static KafkaRuntimeTarget target() { return new KafkaRuntimeTarget("main", "apache/kafka:4.0.0", KafkaBrokerPolicy.v1SingleBroker()); }

    private static final class FakeDriver implements OwnedKafkaArchiveCapture.Driver {
        final byte[] bytes;
        final AtomicInteger opened = new AtomicInteger(), inspections = new AtomicInteger(), closed = new AtomicInteger();
        volatile OwnedKafkaArchiveCapture.Inspection changed;
        volatile boolean changeAfterOpen;
        InputStream stream;
        String path;
        FakeDriver(byte[] bytes) { this.bytes = bytes; }
        @Override public OwnedKafkaArchiveCapture.Inspection inspect() {
            inspections.incrementAndGet();
            return changed != null ? changed : new OwnedKafkaArchiveCapture.Inspection(ID, IMAGE, NETWORK, List.of("kafka-main"), true);
        }
        @Override public InputStream openArchive(String path) {
            opened.incrementAndGet(); this.path = path;
            if (changeAfterOpen) changed = new OwnedKafkaArchiveCapture.Inspection(ID, IMAGE, NETWORK, List.of(), false);
            InputStream input = stream == null ? new ByteArrayInputStream(bytes) : stream;
            return new java.io.FilterInputStream(input) {
                @Override public void close() throws IOException { closed.incrementAndGet(); super.close(); }
            };
        }
    }
    private static final class FakeContainer extends KafkaContainer {
        final CountDownLatch stopped;
        FakeContainer(CountDownLatch stopped) { super("apache/kafka:4.0.0"); this.stopped = stopped; }
        @Override public void start() { }
        @Override public void stop() { stopped.countDown(); }
        @Override public String getContainerId() { return ID; }
    }
}
