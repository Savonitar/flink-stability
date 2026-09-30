package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.Digests;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InspectKafkaLogCommandTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path temporaryDirectory;

    @Test
    void kafkaBuiltRecordReachesJsonWithCoordinatesAndRawPayload() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.none().build(), new SimpleRecord(123L, "7".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        Path file = temporaryDirectory.toRealPath().resolve("batch.log");
        Files.write(file, bytes);

        Invocation result = execute("inspect-kafka-log", "--input", file.toString());
        JsonNode output = JSON.readTree(result.stdout());

        assertEquals(0, result.exitCode(), result.stdout() + result.stderr());
        assertEquals(1, output.at("/segment/recordCount").asLong());
        assertEquals(0, output.at("/segment/batches/0/fileByteOffset").asLong());
        assertEquals(7, output.at("/segment/batches/0/records/0/canonicalId").asLong());
        assertEquals(123, output.at("/segment/batches/0/records/0/timestamp").asLong());
        assertEquals("Nw==", output.at("/segment/batches/0/records/0/valueBase64").asText());
        assertTrue(output.at("/segment/batches/0/records/0/keyBase64").isNull());
        assertTrue(output.at("/segment/batches/0/records/0/marker").isNull());
        assertEquals(Digests.sha256(bytes), output.path("inputSha256").asText());
        assertArrayEquals(bytes, Files.readAllBytes(file));
    }

    @Test
    void emptyFileReportsOnlyPhysicalFileCompletenessWithItsHash() throws Exception {
        Path file = temporaryDirectory.toRealPath().resolve("empty.log");
        Files.write(file, new byte[0]);

        Invocation result = execute("inspect-kafka-log", "--input", file.toString());
        JsonNode output = JSON.readTree(result.stdout());

        assertEquals(0, result.exitCode());
        assertTrue(output.path("complete").asBoolean());
        assertEquals("kafka-log-segment-v1", output.path("format").asText());
        assertEquals(Digests.sha256(new byte[0]), output.path("inputSha256").asText());
        assertEquals(0, output.path("inputBytes").asInt());
        assertEquals(0, output.at("/segment/recordCount").asLong());
        assertTrue(output.at("/segment/batches").isArray());
        assertTrue(output.path("scope").asText().contains("no topic identity"));
        assertFalse(output.has("verdict"));
        assertEquals("", result.stderr());
    }

    @Test
    void truncatedInputRetainsCapturedHashAndNeverReportsAnEmptySuccess() throws Exception {
        Path file = temporaryDirectory.toRealPath().resolve("truncated.log");
        byte[] bytes = {1, 2, 3};
        Files.write(file, bytes);

        Invocation result = execute("inspect-kafka-log", "--input", file.toString());
        JsonNode output = JSON.readTree(result.stdout());

        assertEquals(CommandLine.ExitCode.SOFTWARE, result.exitCode());
        assertFalse(output.path("complete").asBoolean());
        assertEquals(Digests.sha256(bytes), output.path("inputSha256").asText());
        assertTrue(output.hasNonNull("error"));
        assertFalse(output.has("segment"));
        assertArrayEquals(bytes, Files.readAllBytes(file));
    }

    @Test
    void inputByteLimitRejectsBeforeDecodeAndDoesNotAlterInput() throws Exception {
        Path file = temporaryDirectory.toRealPath().resolve("oversized.log");
        byte[] bytes = {1, 2, 3};
        Files.write(file, bytes);

        Invocation result = execute("inspect-kafka-log", "--input", file.toString(), "--max-bytes", "2");
        JsonNode output = JSON.readTree(result.stdout());

        assertEquals(CommandLine.ExitCode.SOFTWARE, result.exitCode());
        assertFalse(output.path("complete").asBoolean());
        assertTrue(output.path("error").asText().contains("byte limit"));
        assertFalse(output.has("inputSha256"));
        assertArrayEquals(bytes, Files.readAllBytes(file));
    }

    @Test
    void symbolicLinkAndOutOfRangeBudgetAreRejected() throws Exception {
        Path directory = temporaryDirectory.toRealPath();
        Path file = Files.write(directory.resolve("target.log"), new byte[0]);
        Path link = Files.createSymbolicLink(directory.resolve("link.log"), file);

        Invocation linked = execute("inspect-kafka-log", "--input", link.toString());
        assertEquals(CommandLine.ExitCode.SOFTWARE, linked.exitCode());
        assertTrue(JSON.readTree(linked.stdout()).path("error").asText().contains("Symbolic links"));

        Invocation invalid = execute("inspect-kafka-log", "--input", file.toString(), "--max-records", "100001");
        assertEquals(CommandLine.ExitCode.SOFTWARE, invalid.exitCode());
        assertFalse(JSON.readTree(invalid.stdout()).path("complete").asBoolean());
        assertFalse(JSON.readTree(invalid.stdout()).has("inputSha256"));
    }

    private static Invocation execute(String... arguments) {
        StringWriter stdout = new StringWriter();
        StringWriter stderr = new StringWriter();
        int code = new CommandLine(new FlinkStabilityCommand())
                .setOut(new PrintWriter(stdout))
                .setErr(new PrintWriter(stderr))
                .execute(arguments);
        return new Invocation(code, stdout.toString(), stderr.toString());
    }

    private record Invocation(int exitCode, String stdout, String stderr) {}
}
