package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/** Owned-broker transport facts only; EOF does not establish TAR or Kafka validity. */
public record KafkaLogCapture(List<Inventory> inventories, List<Archive> archives,
                              long retainedBytes, int reservedCalls, List<String> diagnostics) {
    public static final long MAX_BYTES = 64L * 1024 * 1024;
    public static final int MAX_CALLS = 512;
    public record Partition(String topic, int partition) {
        public Partition {
            if (topic == null || !topic.matches("[A-Za-z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")
                    || partition < 0) throw new IllegalArgumentException("Invalid topic partition");
        }
    }
    public record Inventory(Partition partition, String directory, String status,
                            List<File> files, boolean workerFinished, Path transcript, String transcriptSha256,
                            String containerId, String imageId, String networkId, long generation) {
        public Inventory { files = List.copyOf(files); }
    }
    public record File(String basename, long bytes, long modifiedSeconds, long inode, boolean log) {}
    public record Archive(Partition partition, String basename, String containerId, String imageId,
                          String networkId, long generation, Path path, String status, long bytes,
                          Optional<String> sha256, boolean workerFinished, String detail) {}
    public KafkaLogCapture {
        inventories = List.copyOf(inventories);
        archives = List.copyOf(archives);
        diagnostics = List.copyOf(diagnostics);
    }
}
