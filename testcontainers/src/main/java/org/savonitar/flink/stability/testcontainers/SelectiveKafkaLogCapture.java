package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** One diagnostic collection from the exact live owner, after the data oracle. */
final class SelectiveKafkaLogCapture {
    // The registered official Apache Kafka 4.0 runtime uses this directory. Never search other roots.
    static final String LOG_ROOT = "/tmp/kafka-logs";
    private SelectiveKafkaLogCapture() {}

    static KafkaLogCapture collect(OwnedKafkaArchiveCapture.Binding binding,
            OwnedKafkaArchiveCapture.Driver driver, List<KafkaLogCapture.Partition> partitions,
            Path directory, MonotonicDeadline deadline) {
        var inventories = new ArrayList<KafkaLogCapture.Inventory>();
        var archives = new ArrayList<KafkaLogCapture.Archive>();
        var errors = new ArrayList<String>();
        var budget = new Budget(driver, deadline);
        long bytes = 0;
        try {
            if (partitions.size() > 128 || Set.copyOf(partitions).size() != partitions.size()) {
                throw new IllegalArgumentException("At most 128 distinct declared partitions are supported");
            }
            var identity = OwnedKafkaArchiveCapture.observe(binding, budget, deadline);
            for (var requested : partitions) {
                budget.check();
                var partition = new OwnedKafkaArchiveCapture.Partition(Set.of(LOG_ROOT), LOG_ROOT,
                        requested.topic(), requested.partition());
                int allowance = (int) Math.min(KafkaLogInventory.MAX_INVENTORY_BYTES, KafkaLogCapture.MAX_BYTES - bytes);
                if (allowance < 64) throw new IllegalStateException("Shared capture byte budget exhausted");
                var inventory = KafkaLogInventory.capture(binding, budget, identity, partition, allowance, deadline);
                byte[] transcript = inventory.transcript();
                bytes += transcript.length;
                Path transcriptPath = directory.resolve("inventory-" + inventories.size() + ".bin");
                Files.write(transcriptPath, transcript,
                        java.nio.file.StandardOpenOption.CREATE_NEW);
                inventories.add(new KafkaLogCapture.Inventory(requested, partition.directory(), inventory.status().name(),
                        inventory.entries().stream().map(file -> new KafkaLogCapture.File(file.basename(), file.size(),
                                file.modifiedSeconds(), file.inode(), file.log())).toList(),
                        inventory.workerFinished(), transcriptPath, inventory.sha256(), binding.containerId(),
                        identity.imageId(), binding.networkId(), binding.generation()));
                if (!inventory.workerFinished()) throw new IllegalStateException("Inventory worker unfinished; stop collection");
                if (inventory.status() != KafkaLogInventory.Status.OBSERVED) {
                    errors.add(requested + ": " + inventory.status() + ": " + inventory.detail());
                    continue;
                }
                for (var file : inventory.entries()) {
                    if (!file.log()) continue;
                    budget.check();
                    long remaining = KafkaLogCapture.MAX_BYTES - bytes;
                    if (remaining <= 0) throw new IllegalStateException("Shared capture byte budget exhausted");
                    if (file.size() > KafkaLogInventory.MAX_LOG_BYTES) {
                        errors.add(requested + "/" + file.basename() + ": file exceeds 16 MiB");
                        continue;
                    }
                    var receipt = OwnedKafkaArchiveCapture.copyLog(binding, budget, identity, inventory, file.basename(),
                            directory, "segment-" + archives.size() + ".tar.part",
                            Math.min(remaining, KafkaLogInventory.MAX_LOG_BYTES + 65536), deadline);
                    bytes += receipt.observedBytes();
                    archives.add(new KafkaLogCapture.Archive(requested, file.basename(), binding.containerId(),
                            identity.imageId(), binding.networkId(), binding.generation(), receipt.partialArchive(),
                            receipt.status().name(), receipt.observedBytes(), receipt.sha256(), receipt.workerFinished(), receipt.detail()));
                    if (!receipt.workerFinished() || receipt.status() != OwnedKafkaArchiveCapture.Status.TRANSPORT_EOF) {
                        throw new IllegalStateException("Incomplete archive; stop collection: " + receipt.status());
                    }
                }
            }
        } catch (IOException | RuntimeException failure) {
            errors.add(failure.getClass().getSimpleName() + ": " + failure.getMessage());
        }
        return new KafkaLogCapture(inventories, archives, bytes, budget.calls(), errors);
    }

    /** Reserve an upper bound before each Docker operation: find/stat use create/start/inspect. */
    private static final class Budget implements OwnedKafkaArchiveCapture.Driver {
        private final OwnedKafkaArchiveCapture.Driver driver;
        private final MonotonicDeadline deadline;
        private int calls;
        Budget(OwnedKafkaArchiveCapture.Driver driver, MonotonicDeadline deadline) {
            this.driver = driver; this.deadline = deadline;
        }
        synchronized int calls() { return calls; }
        void check() {
            if (Thread.currentThread().isInterrupted() || deadline.remaining().isZero()) {
                throw new IllegalStateException("Shared collection deadline expired or interrupted");
            }
        }
        synchronized void reserve(int count) {
            check();
            if (calls > KafkaLogCapture.MAX_CALLS - count) throw new IllegalStateException("Shared Docker call budget exhausted");
            calls += count;
        }
        public OwnedKafkaArchiveCapture.Inspection inspect() { reserve(1); return driver.inspect(); }
        public InputStream openArchive(String path) throws IOException { reserve(1); return driver.openArchive(path); }
        public KafkaLogInventory.Command listFileNames(String path, int max, ContainerOperationDeadline time, Runnable active) throws IOException {
            reserve(3); return driver.listFileNames(path, max, time, active);
        }
        public KafkaLogInventory.Command fileMetadata(String path, int max, ContainerOperationDeadline time, Runnable active) throws IOException {
            reserve(3); return driver.fileMetadata(path, max, time, active);
        }
    }
}
