package org.savonitar.flink.stability.core.validation.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class KafkaLogArchiveReaderTest {
    private static final String NAME = "00000000000000000000.log";
    @TempDir Path directory;

    @BeforeEach void resolveTemporaryRoot() throws java.io.IOException {
        // macOS may allocate @TempDir below /var -> /private/var; keep production path guards strict.
        directory = directory.toRealPath();
    }
    private int sequence;
    private KafkaLogArchiveReader.Result inspect(byte[] bytes, boolean finished, String hash) throws Exception {
        Path file = directory.resolve("archive-" + sequence++ + ".tar"); Files.write(file, bytes);
        return new KafkaLogArchiveReader(directory).inspectSingleLog(new KafkaLogArchiveReader.ReceiptEvidence(
                file, bytes.length, hash == null ? Digests.sha256(bytes) : hash, true, finished), "output", 0, NAME,
                new KafkaLogArchiveReader.Limits(65536, 32768, 1, 100, 100),
                MonotonicDeadline.start(Duration.ofSeconds(2), System::nanoTime));
    }
    @Test void strictSingleFileDecodesEmptyKafkaSegmentAndPreservesReceipt() throws Exception {
        byte[] archive = tar(NAME, new byte[0], '0');
        var result = inspect(archive, true, null);
        assertEquals(KafkaLogArchiveReader.Status.PARSED_AND_DECODED, result.status());
        assertTrue(result.tarComplete()); assertTrue(result.inputVerified());
        assertEquals(0, result.logs().getFirst().segment().recordCount());
        assertEquals(Digests.sha256(archive), result.archiveSha256());
    }
    @Test void decodesActualKafkaBatchWithinTar() throws Exception {
        var records = org.apache.kafka.common.record.MemoryRecords.withRecords(
                org.apache.kafka.common.compress.Compression.none().build(),
                new org.apache.kafka.common.record.SimpleRecord("42".getBytes(StandardCharsets.UTF_8)));
        byte[] segment = new byte[records.sizeInBytes()]; records.buffer().get(segment);
        var result = inspect(tar(NAME, segment, '0'), true, null);
        assertEquals(KafkaLogArchiveReader.Status.PARSED_AND_DECODED, result.status(), result.detail());
        assertEquals(42L, result.logs().getFirst().segment().batches().getFirst().records().getFirst().canonicalId());
    }
    @Test void rejectsLinksTraversalWrongNamesChecksumPaddingAndTrailer() throws Exception {
        for (var archive : new byte[][] {tar("../" + NAME, new byte[0], '0'), tar(NAME, new byte[0], '2'),
                tar("other.log", new byte[0], '0'), tar(NAME, new byte[0], 'x')}) {
            assertEquals(KafkaLogArchiveReader.Status.TAR_REJECTED, inspect(archive, true, null).status());
        }
        byte[] badChecksum = tar(NAME, new byte[0], '0'); badChecksum[0]++;
        assertEquals(KafkaLogArchiveReader.Status.TAR_REJECTED, inspect(badChecksum, true, null).status());
        byte[] padding = tar(NAME, new byte[]{1}, '0'); padding[513] = 1;
        assertEquals(KafkaLogArchiveReader.Status.TAR_REJECTED, inspect(padding, true, null).status());
        byte[] trailing = tar(NAME, new byte[0], '0'); trailing[trailing.length - 1] = 1;
        assertEquals(KafkaLogArchiveReader.Status.TAR_REJECTED, inspect(trailing, true, null).status());
        assertEquals(KafkaLogArchiveReader.Status.TAR_REJECTED, inspect(Arrays.copyOf(trailing, 1024), true, null).status());
    }
    @Test void transportEofCannotReplaceHashWorkerOrKafkaValidation() throws Exception {
        var archive = tar(NAME, new byte[0], '0');
        assertEquals(KafkaLogArchiveReader.Status.RECEIPT_REJECTED, inspect(archive, false, null).status());
        assertEquals(KafkaLogArchiveReader.Status.INPUT_REJECTED, inspect(archive, true, "0".repeat(64)).status());
        assertEquals(KafkaLogArchiveReader.Status.LOGS_UNSUPPORTED, inspect(tar(NAME, new byte[]{1,2,3}, '0'), true, null).status());
    }
    static byte[] tar(String name, byte[] payload, char type) {
        byte[] bytes = new byte[512 + ((payload.length + 511) / 512) * 512 + 1024];
        put(bytes, 0, name); put(bytes, 100, "0000644"); put(bytes, 108, "0000000"); put(bytes, 116, "0000000");
        put(bytes, 124, String.format("%011o", payload.length)); put(bytes, 136, "00000000000");
        Arrays.fill(bytes, 148, 156, (byte) ' '); bytes[156] = (byte) type;
        put(bytes, 257, "ustar"); put(bytes, 263, "00");
        long sum = 0; for (int i = 0; i < 512; i++) sum += bytes[i] & 255;
        put(bytes, 148, String.format("%06o", sum)); bytes[154] = 0; bytes[155] = 32;
        System.arraycopy(payload, 0, bytes, 512, payload.length); return bytes;
    }
    private static void put(byte[] bytes, int at, String value) {
        byte[] text = value.getBytes(StandardCharsets.US_ASCII); System.arraycopy(text, 0, bytes, at, text.length);
    }
}
