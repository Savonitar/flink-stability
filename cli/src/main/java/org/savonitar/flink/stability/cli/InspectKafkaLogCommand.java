package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.validation.kafka.KafkaLogSegmentDecoder;
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
            byte[] bytes = readBounded(input, maximumBytes);
            result.put("inputBytes", bytes.length);
            result.put("inputSha256", Digests.sha256(bytes));
            var segment = new KafkaLogSegmentDecoder().decode(bytes, maximumRecords);
            result.set("segment", JSON.valueToTree(segment));
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
