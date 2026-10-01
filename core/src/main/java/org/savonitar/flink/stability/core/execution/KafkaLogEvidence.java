package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.validation.kafka.KafkaLogArchiveReader;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Optional post-oracle diagnostics; never consulted by scenario verdicts. */
public record KafkaLogEvidence(String status, Optional<KafkaLogCapture> capture,
                               List<Decoded> decoded, List<String> diagnostics) {
    public record Decoded(Path archive, String status, long records, long batches,
                          Optional<Path> evidence, Optional<String> sha256) {}
    public KafkaLogEvidence {
        capture = java.util.Objects.requireNonNull(capture);
        decoded = List.copyOf(decoded);
        diagnostics = List.copyOf(diagnostics);
    }
    public static KafkaLogEvidence notRequested() {
        return new KafkaLogEvidence("not-requested", Optional.empty(), List.of(), List.of());
    }
    public static KafkaLogEvidence notRun() {
        return new KafkaLogEvidence("not-run", Optional.empty(), List.of(),
                List.of("No completed terminal data verification; no broker collection attempted"));
    }

    static KafkaLogEvidence collect(V1AttemptRuntime runtime, ExecutableScenarioPlan plan, Path output) {
        return collect(runtime, plan, output, MonotonicDeadline.start(Duration.ofSeconds(60), System::nanoTime));
    }

    static KafkaLogEvidence collect(V1AttemptRuntime runtime, ExecutableScenarioPlan plan, Path output,
                                    MonotonicDeadline deadline) {
        KafkaLogCapture capture = null;
        var decoded = new ArrayList<Decoded>();
        var errors = new ArrayList<String>();
        try {
            output = output.toAbsolutePath().normalize();
            Path cursor = output.getRoot();
            for (Path part : output) {
                cursor = cursor.resolve(part);
                if (Files.isSymbolicLink(cursor)) throw new IllegalArgumentException("Output symlink rejected");
            }
            // Never reuse an earlier capture directory or overwrite retained evidence.
            Files.createDirectory(output);
            List<KafkaLogCapture.Partition> partitions = new ArrayList<>();
            for (var topic : plan.kafka().topics()) {
                if (topic.partitions() > 128 - partitions.size()) throw new IllegalArgumentException("Partition capture bound exceeded");
                for (int i = 0; i < topic.partitions(); i++) partitions.add(new KafkaLogCapture.Partition(topic.name(), i));
            }
            check(deadline);
            capture = runtime.captureKafkaLogs(partitions, output, deadline);
            errors.addAll(capture.diagnostics());
            var reader = new KafkaLogArchiveReader(output);
            int recordsLeft = 100_000, batchesLeft = 100_000;
            long decodedBytesLeft = KafkaLogCapture.MAX_BYTES;
            for (var archive : capture.archives()) {
                check(deadline);
                if (recordsLeft == 0 || batchesLeft == 0) throw new IllegalStateException("Aggregate decode budget exhausted");
                var result = reader.inspectSingleLog(new KafkaLogArchiveReader.ReceiptEvidence(archive.path(), archive.bytes(),
                        archive.sha256().orElse(null), archive.status().equals("TRANSPORT_EOF"), archive.workerFinished()),
                        archive.partition().topic(), archive.partition().partition(), archive.basename(),
                        new KafkaLogArchiveReader.Limits(16 * 1024 * 1024 + 65536, 16 * 1024 * 1024,
                                1, recordsLeft, batchesLeft), deadline);
                long records = result.logs().stream().filter(KafkaLogArchiveReader.Log::decoded)
                        .mapToLong(log -> log.segment().recordCount()).sum();
                long batches = result.logs().stream().filter(KafkaLogArchiveReader.Log::decoded)
                        .mapToLong(log -> log.segment().batches().size()).sum();
                recordsLeft -= Math.toIntExact(records); batchesLeft -= Math.toIntExact(batches);
                check(deadline);
                byte[] json = new ObjectMapper().writeValueAsBytes(result);
                if (json.length > decodedBytesLeft) throw new IllegalStateException("Aggregate decoded byte budget exhausted");
                decodedBytesLeft -= json.length;
                check(deadline);
                Path evidence = output.resolve("decoded-" + decoded.size() + ".json");
                Files.write(evidence, json, java.nio.file.StandardOpenOption.CREATE_NEW);
                decoded.add(new Decoded(archive.path(), result.status().name(), records, batches,
                        Optional.of(evidence), Optional.of(Digests.sha256(json))));
                if (!result.allLogsDecoded()) errors.add(archive.basename() + ": " + result.status() + ": " + result.detail());
                check(deadline);
            }
            if (capture.archives().isEmpty()) errors.add("No Kafka log archives captured");
        } catch (Exception failure) {
            errors.add(failure.getClass().getSimpleName() + ": " + failure.getMessage());
        }
        return new KafkaLogEvidence(errors.isEmpty() ? "collected" : "partial", Optional.ofNullable(capture), decoded, errors);
    }

    private static void check(MonotonicDeadline deadline) {
        if (Thread.currentThread().isInterrupted() || deadline.remaining().isZero()) {
            throw new IllegalStateException("Shared Kafka log collection deadline expired or interrupted");
        }
    }
}
