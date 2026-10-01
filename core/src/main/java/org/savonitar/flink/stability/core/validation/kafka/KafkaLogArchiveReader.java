package org.savonitar.flink.stability.core.validation.kafka;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Strict offline USTAR inventory + actual Kafka decoder; never extracts or modifies archive files. */
public final class KafkaLogArchiveReader {
    public static final long MAX_ARCHIVE_BYTES = 128L * 1024 * 1024;
    private final Path root;
    public KafkaLogArchiveReader(Path outputDirectory) {
        root = outputDirectory.toAbsolutePath().normalize();
    }
    private static final int BLOCK = 512;
    private static final Pattern SINGLE_LOG = Pattern.compile("[0-9]{20}\\.log");

    /** Caller must map the actual owned capture receipt; this public value cannot prove provenance. */
    public record ReceiptEvidence(Path archive, long observedBytes, String sha256,
                                  boolean transportEof, boolean workerFinished) {}
    public record Limits(long maxArchiveBytes, long maxFileBytes, int maxEntries,
                         int maxDecodedRecords, int maxDecodedBatches) {
        public Limits {
            if (maxArchiveBytes < 1024 || maxArchiveBytes > MAX_ARCHIVE_BYTES
                    || maxFileBytes < 1 || maxFileBytes > KafkaLogSegmentDecoder.MAX_SEGMENT_BYTES
                    || maxEntries < 1 || maxEntries > 1024 || maxDecodedRecords < 1
                    || maxDecodedRecords > KafkaLogSegmentDecoder.MAX_RECORDS || maxDecodedBatches < 1
                    || maxDecodedBatches > KafkaLogSegmentDecoder.MAX_BATCHES) {
                throw new IllegalArgumentException("Archive, entry, file, record or batch limit outside supported bounds");
            }
        }
    }
    public enum Status {
        PARSED_AND_DECODED, NO_LOG_FILES, LOGS_UNSUPPORTED, RECEIPT_REJECTED,
        INPUT_REJECTED, TAR_REJECTED, IO_FAILURE, DEADLINE, INTERRUPTED
    }
    public record Entry(String path, boolean directory, long size, long modifiedEpochSeconds,
                        String payloadSha256, int headerByteOffset, int payloadByteOffset) {}
    public record Log(String path, long fileBaseOffset, boolean decoded, String detail,
                      KafkaLogSegmentDecoder.Segment segment) {}
    public record Result(Status status, String detail, String expectedPartitionRoot,
                         boolean inputVerified, boolean tarComplete, boolean allLogsDecoded,
                         long archiveBytes, String archiveSha256, List<Entry> entries, List<Log> logs,
                         String expectedArchiveRoot) {
        public Result { entries = List.copyOf(entries); logs = List.copyOf(logs); }
    }

    /** Exact root file only; caller must bind the basename to its real owned partition inventory. */
    public Result inspectSingleLog(ReceiptEvidence receipt, String declaredTopic, int partition,
                                   String expectedBasename, Limits limits, MonotonicDeadline deadline) {
        if (expectedBasename == null || !SINGLE_LOG.matcher(expectedBasename).matches()) {
            throw new IllegalArgumentException("One exact 20-digit .log basename required");
        }
        try { Long.parseLong(expectedBasename.substring(0, 20)); }
        catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("Expected segment base offset exceeds signed long", invalid);
        }
        return inspect(receipt, declaredTopic, partition, expectedBasename, limits, deadline);
    }

    private Result inspect(ReceiptEvidence receipt, String declaredTopic, int partition,
                           String expectedBasename, Limits limits, MonotonicDeadline deadline) {
        Objects.requireNonNull(receipt); Objects.requireNonNull(limits); Objects.requireNonNull(deadline);
        if (declaredTopic == null || !declaredTopic.matches("[A-Za-z0-9._-]{1,249}")
                || declaredTopic.equals(".") || declaredTopic.equals("..") || partition < 0
                || deadline.timeout().compareTo(Duration.ofSeconds(60)) > 0) {
            throw new IllegalArgumentException("Exact topic/partition and an existing deadline <=60s required");
        }
        String root = declaredTopic + "-" + partition;
        String archiveRoot = expectedBasename;
        List<Entry> entries = new ArrayList<>();
        List<Log> logs = new ArrayList<>();
        boolean verified = false, tarComplete = false;
        byte[] bytes = null;
        String archiveHash = null;
        try {
            check(deadline);
            require(receipt.transportEof() && receipt.workerFinished(), Status.RECEIPT_REJECTED,
                    "Only a finished TRANSPORT_EOF owner receipt may be inspected; partial workers may still write");
            require(receipt.archive() != null && receipt.observedBytes() >= 0 && receipt.observedBytes() <= limits.maxArchiveBytes()
                            && receipt.sha256() != null && receipt.sha256().matches("[0-9a-f]{64}"),
                    Status.RECEIPT_REJECTED, "Receipt size/hash is absent or outside the archive cap");
            bytes = readStable(receipt, limits, deadline);
            archiveHash = hash(bytes, 0, bytes.length, deadline);
            require(archiveHash.equals(receipt.sha256()), Status.INPUT_REJECTED, "Archive bytes differ from owner receipt SHA256");
            verified = true;
            inventory(bytes, archiveRoot, limits, deadline, entries);
            tarComplete = true;
            long records = 0, batches = 0;
            for (Entry entry : entries) {
                check(deadline);
                if (entry.directory() || !entry.path().endsWith(".log")) continue;
                String name = entry.path();
                long base = decimalOffset(name.substring(0, 20));
                try {
                    byte[] segment = Arrays.copyOfRange(bytes, entry.payloadByteOffset(),
                            entry.payloadByteOffset() + Math.toIntExact(entry.size()));
                    check(deadline);
                    var decoded = new KafkaLogSegmentDecoder().decode(segment,
                            (int) Math.max(1, limits.maxDecodedRecords() - records));
                    check(deadline); // Existing decoder is synchronous; no preemption is claimed.
                    if (decoded.recordCount() > limits.maxDecodedRecords() - records
                            || decoded.batches().size() > limits.maxDecodedBatches() - batches) {
                        throw new IllegalArgumentException("Aggregate decoded record/batch limit");
                    }
                    // A compacted first batch can start after the filename's base offset.
                    if (!decoded.batches().isEmpty() && decoded.batches().getFirst().baseOffset() < base) {
                        throw new IllegalArgumentException("Batch precedes segment filename base offset");
                    }
                    records += decoded.recordCount(); batches += decoded.batches().size();
                    logs.add(new Log(entry.path(), base, true, "Physical magic-2 NONE bytes decoded", decoded));
                } catch (IllegalArgumentException unsupported) {
                    logs.add(new Log(entry.path(), base, false, unsupported.getMessage(), null));
                }
            }
            check(deadline);
            boolean decoded = !logs.isEmpty() && logs.stream().allMatch(Log::decoded);
            return new Result(logs.isEmpty() ? Status.NO_LOG_FILES : decoded ? Status.PARSED_AND_DECODED : Status.LOGS_UNSUPPORTED,
                    "Complete strict TAR inventory only; live copy is not atomic and partition/transaction coverage is unproven",
                    root, true, true, decoded, bytes.length, archiveHash, entries, logs, archiveRoot);
        } catch (Rejected rejected) {
            return new Result(rejected.status, rejected.getMessage(), root, verified, tarComplete, false,
                    bytes == null ? 0 : bytes.length, archiveHash, entries, logs, archiveRoot);
        } catch (IOException io) {
            return new Result(Status.IO_FAILURE, io.getClass().getSimpleName() + ": " + io.getMessage(), root,
                    verified, tarComplete, false, bytes == null ? 0 : bytes.length, archiveHash, entries, logs, archiveRoot);
        }
    }

    private byte[] readStable(ReceiptEvidence receipt, Limits limits, MonotonicDeadline deadline) throws IOException {
        Path path = Objects.requireNonNull(receipt.archive());
        require(path.isAbsolute() && path.equals(path.normalize()) && path.startsWith(root) && !path.equals(root),
                Status.INPUT_REJECTED, "Archive must be an exact normalized output-directory child");
        Path cursor = root.getRoot();
        require(!Files.isSymbolicLink(cursor), Status.INPUT_REJECTED, "Output path symlink rejected");
        for (Path part : cursor.relativize(path)) {
            check(deadline); cursor = cursor.resolve(part);
            require(!Files.isSymbolicLink(cursor), Status.INPUT_REJECTED, "Archive path symlink rejected");
        }
        BasicFileAttributes before = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        require(before.isRegularFile() && before.size() == receipt.observedBytes()
                        && before.size() <= limits.maxArchiveBytes(), Status.INPUT_REJECTED, "Archive size/type differs from receipt");
        byte[] bytes = new byte[Math.toIntExact(before.size())];
        try (InputStream stream = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            int position = 0;
            while (position < bytes.length) {
                check(deadline);
                int n = stream.read(bytes, position, Math.min(65536, bytes.length - position));
                require(n > 0, Status.INPUT_REJECTED, "Archive truncated or non-progressing while reading");
                position += n;
            }
            check(deadline);
            require(stream.read() == -1, Status.INPUT_REJECTED, "Archive grew while reading");
        }
        check(deadline);
        BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        require(after.isRegularFile() && after.size() == before.size()
                        && Objects.equals(before.fileKey(), after.fileKey())
                        && before.lastModifiedTime().equals(after.lastModifiedTime()),
                Status.INPUT_REJECTED, "Archive identity/metadata changed while reading");
        return bytes;
    }

    private static void inventory(byte[] bytes, String root, Limits limits, MonotonicDeadline deadline,
                                  List<Entry> entries) {
        require(bytes.length >= 2 * BLOCK && bytes.length % BLOCK == 0, Status.TAR_REJECTED,
                "Uncompressed TAR must contain whole 512-byte blocks and two end blocks");
        boolean rootSeen = false;
        int position = 0;
        while (position < bytes.length) {
            check(deadline);
            if (zero(bytes, position, BLOCK, deadline)) {
                require(bytes.length - position >= 2 * BLOCK && zero(bytes, position, bytes.length - position, deadline),
                        Status.TAR_REJECTED, "Missing second end block or nonzero data after TAR end");
                require(rootSeen, Status.TAR_REJECTED, "Expected single log entry absent");
                return;
            }
            require(entries.size() < limits.maxEntries(), Status.TAR_REJECTED, "TAR entry count limit");
            require(entries.isEmpty(), Status.TAR_REJECTED,
                    "Single-log archive contains an extra member");
            require(ascii(bytes, position + 257, 6).equals("ustar")
                            && bytes[position + 262] == 0
                            && bytes[position + 263] == '0' && bytes[position + 264] == '0',
                    Status.TAR_REJECTED, "Only strict POSIX USTAR is supported; compression/GNU/PAX extensions rejected");
            long expectedChecksum = octal(bytes, position + 148, 8);
            long actualChecksum = 0;
            for (int i = 0; i < BLOCK; i++) actualChecksum += i >= 148 && i < 156 ? 32 : bytes[position + i] & 255;
            require(expectedChecksum == actualChecksum, Status.TAR_REJECTED, "TAR header checksum mismatch");
            int type = bytes[position + 156] & 255;
            require(type == 0 || type == '0' || type == '5', Status.TAR_REJECTED,
                    "Only regular files/root directory accepted; links/devices/FIFO/PAX/GNU/sparse entries rejected");
            require(ascii(bytes, position + 157, 100).isEmpty(), Status.TAR_REJECTED, "Link target field must be empty");
            long size = octal(bytes, position + 124, 12);
            long mtime = octal(bytes, position + 136, 12);
            octal(bytes, position + 100, 8); octal(bytes, position + 108, 8); octal(bytes, position + 116, 8);
            require(octal(bytes, position + 329, 8) == 0 && octal(bytes, position + 337, 8) == 0
                            && zero(bytes, position + 500, 12, deadline), Status.TAR_REJECTED,
                    "Device/sparse/reserved header metadata is unsupported");
            require(size <= limits.maxFileBytes(), Status.TAR_REJECTED, "TAR entry exceeds per-file byte cap");
            String name = ascii(bytes, position, 100);
            String prefix = ascii(bytes, position + 345, 155);
            String path = prefix.isEmpty() ? name : prefix + "/" + name;
            boolean directory = type == '5';
            if (directory && path.endsWith("/")) path = path.substring(0, path.length() - 1);
            require(!path.startsWith("/") && !path.contains("\\") && !path.contains("//")
                            && Arrays.stream(path.split("/", -1)).noneMatch(p -> p.isEmpty() || p.equals(".") || p.equals("..")),
                    Status.TAR_REJECTED, "Absolute, ambiguous or traversal TAR path rejected");
            require(!directory && prefix.isEmpty() && path.equals(root), Status.TAR_REJECTED,
                    "Single-log archive requires exactly the expected regular root basename");
            rootSeen = true;
            int data = position + BLOCK;
            long padded = ((size + BLOCK - 1) / BLOCK) * BLOCK;
            require(padded <= bytes.length - data, Status.TAR_REJECTED, "Truncated entry payload/padding");
            require(zero(bytes, data + (int) size, (int) (padded - size), deadline), Status.TAR_REJECTED,
                    "Nonzero TAR payload padding rejected");
            entries.add(new Entry(path, directory, size, mtime, hash(bytes, data, (int) size, deadline), position, data));
            position = data + (int) padded;
        }
        throw new Rejected(Status.TAR_REJECTED, "TAR end blocks absent");
    }

    private static String ascii(byte[] bytes, int start, int length) {
        int end = start;
        while (end < start + length && bytes[end] != 0) {
            require(bytes[end] >= 32 && bytes[end] <= 126, Status.TAR_REJECTED, "Non-ASCII/control header name rejected"); end++;
        }
        for (int i = end; i < start + length; i++) require(bytes[i] == 0, Status.TAR_REJECTED, "Nonzero field bytes after NUL");
        return new String(bytes, start, end - start, StandardCharsets.US_ASCII);
    }
    private static long octal(byte[] bytes, int start, int length) {
        long value = 0;
        boolean terminated = false, digit = false;
        for (int i = start; i < start + length; i++) {
            int b = bytes[i] & 255;
            if (b == 0 || b == 32) { if (digit || b == 0) terminated = true; continue; }
            require(!terminated && b >= '0' && b <= '7', Status.TAR_REJECTED, "Invalid/binary/signed TAR numeric field");
            require(value <= (Long.MAX_VALUE - (b - '0')) / 8, Status.TAR_REJECTED, "TAR numeric overflow");
            value = value * 8 + b - '0'; digit = true;
        }
        return value;
    }
    private static long decimalOffset(String name) {
        try { return Long.parseLong(name); }
        catch (NumberFormatException invalid) { throw new Rejected(Status.TAR_REJECTED, "Segment base offset exceeds signed long"); }
    }
    private static boolean zero(byte[] bytes, int offset, int length, MonotonicDeadline deadline) {
        for (int i = 0; i < length; i++) {
            if ((i & 65535) == 0) check(deadline);
            if (bytes[offset + i] != 0) return false;
        }
        return true;
    }
    private static String hash(byte[] bytes, int offset, int length, MonotonicDeadline deadline) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (int n = 0; n < length; n += Math.min(65536, length - n)) {
                check(deadline); digest.update(bytes, offset + n, Math.min(65536, length - n));
            }
            check(deadline);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    private static void check(MonotonicDeadline deadline) {
        require(!Thread.currentThread().isInterrupted(), Status.INTERRUPTED, "Offline inspection interrupted; flag retained");
        require(!deadline.remaining().isZero(), Status.DEADLINE, "Offline inspection deadline expired");
    }
    private static void require(boolean condition, Status status, String detail) {
        if (!condition) throw new Rejected(status, detail);
    }
    private static final class Rejected extends RuntimeException {
        private final Status status;
        private Rejected(Status status, String detail) { super(detail); this.status = status; }
    }
}
