package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.validation.kafka.KafkaLogSegmentDecoder;
import org.savonitar.flink.stability.core.validation.kafka.KafkaTransactionLogEvidence;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.Digests;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.concurrent.Callable;

/** Reads one explicit local segment; never contacts Kafka or changes the input. */
@CommandLine.Command(
        name = "inspect-kafka-log",
        mixinStandardHelpOptions = true,
        description = "Decode a retained, uncompressed Kafka v2 log segment as physical evidence.")
public final class InspectKafkaLogCommand implements Callable<Integer> {
    private static final ObjectMapper JSON = new ObjectMapper();

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec specification;

    @CommandLine.Option(names = "--input", required = true, paramLabel = "FILE",
            description = "One regular local segment file; symbolic links are rejected.")
    private Path input;

    @CommandLine.Option(names = "--max-bytes", defaultValue = "67108864", paramLabel = "N",
            description = "Maximum input bytes, from 1 to 67108864 (default: 67108864).")
    private int maximumBytes;

    @CommandLine.Option(names = "--max-records", defaultValue = "100000", paramLabel = "N",
            description = "Maximum decoded records, from 1 to 100000 (default: 100000).")
    private int maximumRecords;

    @CommandLine.Option(names = "--transaction-state", description = "Decode --input as a coordinator log (known key v0, value v0/v1 only).")
    private boolean transactionState;

    @CommandLine.Option(names = "--transaction-state-input", paramLabel = "PARTITION=FILE",
            description = "Join a coordinator segment with --input; repeatable, including multiple segments of a partition.")
    private java.util.List<String> transactionInputs = new java.util.ArrayList<>();

    @Override
    public Integer call() {
        ObjectNode result = JSON.createObjectNode();
        result.put("format", "kafka-log-segment-v1");
        result.put("complete", false);
        result.put("inputFile", input.toAbsolutePath().normalize().toString());
        result.put("scope", "Physical records in this file only; no topic identity, "
                + "partition coverage, transaction visibility or data-loss attribution is inferred.");
        try {
            if (maximumBytes < 1 || maximumBytes > KafkaLogSegmentDecoder.MAX_SEGMENT_BYTES
                    || maximumRecords < 1 || maximumRecords > KafkaLogSegmentDecoder.MAX_RECORDS) {
                throw new IllegalArgumentException("Input byte or record limit is outside the supported range");
            }
            var deadline = MonotonicDeadline.start(java.time.Duration.ofSeconds(60), System::nanoTime);
            byte[] bytes = readBounded(input, maximumBytes);
            result.put("inputBytes", bytes.length);
            result.put("inputSha256", Digests.sha256(bytes));
            var segment = new KafkaLogSegmentDecoder().decode(bytes, maximumRecords);
            result.set("segment", JSON.valueToTree(segment));
            if (transactionState || !transactionInputs.isEmpty()) {
                var transactions = new KafkaTransactionLogEvidence();
                transactions.add(new KafkaTransactionLogEvidence.Source(
                        transactionState ? KafkaTransactionLogEvidence.TOPIC : "unspecified-data-topic", -1,
                        input.toAbsolutePath().normalize().toString()), segment, deadline);
                int bytesLeft = maximumBytes - bytes.length;
                int batchesLeft = KafkaLogSegmentDecoder.MAX_BATCHES - segment.batches().size();
                int recordsLeft = maximumRecords - Math.toIntExact(segment.recordCount());
                var inputs = result.putArray("transactionStateInputs");
                for (String supplied : transactionInputs) {
                    int separator = supplied.indexOf('=');
                    if (separator < 1) throw new IllegalArgumentException("Expected PARTITION=FILE");
                    int partition = Integer.parseInt(supplied.substring(0, separator));
                    if (partition < 0) throw new IllegalArgumentException("Coordinator partition must be nonnegative");
                    Path path = Path.of(supplied.substring(separator + 1));
                    if (recordsLeft < 1 || bytesLeft < 1) throw new IllegalArgumentException("Aggregate input budget exhausted");
                    byte[] extra = readBounded(path, bytesLeft);
                    var decoded = new KafkaLogSegmentDecoder().decode(extra, recordsLeft);
                    if (decoded.batches().size() > batchesLeft) throw new IllegalArgumentException("Aggregate batch budget exhausted");
                    batchesLeft -= decoded.batches().size();
                    bytesLeft -= extra.length; recordsLeft -= Math.toIntExact(decoded.recordCount());
                    inputs.addObject().put("partition", partition).put("path", path.toAbsolutePath().normalize().toString())
                            .put("sha256", Digests.sha256(extra)).put("bytes", extra.length);
                    transactions.add(new KafkaTransactionLogEvidence.Source(KafkaTransactionLogEvidence.TOPIC,
                            partition, path.toAbsolutePath().normalize().toString()), decoded, deadline);
                }
                var summary = transactions.summary();
                result.set("transactions", JSON.valueToTree(summary));
                if (!summary.decoded()) throw new IllegalArgumentException("Transaction-log decoding failed; see transactions.diagnostics");
            }
            result.put("complete", true);
            specification.commandLine().getOut().println(result);
            return CommandLine.ExitCode.OK;
        } catch (IOException | IllegalArgumentException failure) {
            result.put("error", failure.getMessage());
            specification.commandLine().getOut().println(result);
            return CommandLine.ExitCode.SOFTWARE;
        }
    }

    private static byte[] readBounded(Path supplied, int maximumBytes) throws IOException {
        Path path = supplied.toAbsolutePath().normalize();
        Path cursor = path.getRoot();
        for (Path part : path) {
            cursor = cursor.resolve(part);
            if (Files.isSymbolicLink(cursor)) {
                throw new IOException("Symbolic links are not accepted as segment input");
            }
        }
        BasicFileAttributes before = Files.readAttributes(
                path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!before.isRegularFile() || before.size() > maximumBytes) {
            throw new IOException("Segment input must be a regular file within the byte limit");
        }
        byte[] bytes;
        try (InputStream stream = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            bytes = stream.readNBytes(maximumBytes + 1);
        }
        BasicFileAttributes after = Files.readAttributes(
                path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (bytes.length > maximumBytes) {
            throw new IOException("Segment input exceeded the byte limit while reading");
        }
        if (!after.isRegularFile() || before.size() != bytes.length
                || after.size() != bytes.length
                || !Objects.equals(before.fileKey(), after.fileKey())
                || !before.lastModifiedTime().equals(after.lastModifiedTime())) {
            throw new IOException("Segment input changed while reading; retain a stable copy first");
        }
        return bytes;
    }

}
