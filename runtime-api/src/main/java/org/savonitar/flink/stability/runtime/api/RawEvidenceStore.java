package org.savonitar.flink.stability.runtime.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

/** Attempt-owned bounded immutable response captures. A write/bound failure is never ignored. */
public final class RawEvidenceStore {
    public static final int MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
    public static final int MAX_CAPTURES = 4096;
    public static final long MAX_TOTAL_BYTES = 64L * 1024 * 1024;
    private final Path directory;
    private final List<RawEvidenceRef> references = new ArrayList<>();
    private long totalBytes;
    private boolean failed;

    public RawEvidenceStore(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();
        for (Path parent = this.directory.getParent(); parent != null; parent = parent.getParent()) {
            if (Files.isSymbolicLink(parent)) throw new IOException("Raw evidence parent is a symlink");
        }
        Files.createDirectory(this.directory, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwx------")));
    }

    public synchronized long cursor() { return references.size(); }

    public synchronized List<RawEvidenceRef> since(long cursor) {
        if (cursor < 0 || cursor > references.size()) throw new IllegalArgumentException("Invalid capture cursor");
        return List.copyOf(references.subList((int) cursor, references.size()));
    }

    public synchronized RawEvidenceRef retain(byte[] body, String kind, String group, String method,
            String endpoint, int status, long startedNanos, long completedNanos) throws IOException {
        if (failed) throw new IOException("Raw evidence store previously failed");
        if (body == null || body.length == 0 || body.length > MAX_RESPONSE_BYTES
                || references.size() >= MAX_CAPTURES || totalBytes + body.length > MAX_TOTAL_BYTES) {
            failed = true;
            throw new IOException("Raw evidence capture bound exceeded or response is empty");
        }
        long sequence = references.size() + 1L;
        Path path = directory.resolve(String.format(java.util.Locale.ROOT, "%06d.json", sequence));
        try {
            Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(path, body, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            RawEvidenceRef reference = new RawEvidenceRef(path.toString(), Digests.sha256(path, LinkOption.NOFOLLOW_LINKS),
                    body.length, kind, sequence, startedNanos, completedNanos, group, method, endpoint, status);
            references.add(reference);
            totalBytes += body.length;
            return reference;
        } catch (IOException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }
}
