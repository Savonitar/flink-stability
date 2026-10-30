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
    private Batch activeBatch;

    /** Committed captures only; discarded polling candidates consume no persistent budget. */
    public record Utilisation(long captures, long bytes, long maxCaptures, long maxBytes, boolean complete) {
        public Utilisation {
            if (maxCaptures < 1 || maxBytes < 1 || captures < 0 || captures > maxCaptures
                    || bytes < 0 || bytes > maxBytes)
                throw new IllegalArgumentException("Invalid raw evidence utilisation");
        }
    }

    public synchronized Utilisation utilisation() {
        return new Utilisation(references.size(), totalBytes, MAX_CAPTURES, MAX_TOTAL_BYTES, !failed);
    }

    /** Stage one bounded observation without publishing files until the caller selects it. */
    public synchronized Batch batch() throws IOException {
        if (failed) throw new IOException("Raw evidence store previously failed");
        if (activeBatch != null) throw new IllegalStateException("A raw evidence batch is already open");
        activeBatch = new Batch();
        return activeBatch;
    }

    public final class Batch implements AutoCloseable {
        private final List<Pending> pending = new ArrayList<>();
        private long bytes;
        private boolean committed;
        private boolean closed;

        public List<RawEvidenceRef> references() {
            synchronized (RawEvidenceStore.this) {
                return pending.stream().map(Pending::reference).toList();
            }
        }

        public RawEvidenceRef retain(byte[] body, String kind, String group, String method,
                String endpoint, int status, long startedNanos, long completedNanos) throws IOException {
            synchronized (RawEvidenceStore.this) {
                requireOpen();
                if (committed) throw new IllegalStateException("Raw evidence batch already committed");
                requireBounds(body, references.size() + pending.size(), totalBytes + bytes);
                long sequence = references.size() + pending.size() + 1L;
                var reference = new RawEvidenceRef(path(sequence).toString(), Digests.sha256(body), body.length,
                        kind, sequence, startedNanos, completedNanos, group, method, endpoint, status);
                pending.add(new Pending(reference, body.clone()));
                bytes += body.length;
                return reference;
            }
        }

        public void commit() throws IOException {
            synchronized (RawEvidenceStore.this) {
                requireOpen();
                if (committed) return;
                for (var item : pending) write(item.reference(), item.body());
                committed = true;
            }
        }

        private void requireOpen() {
            if (closed || activeBatch != this) throw new IllegalStateException("Raw evidence batch is closed");
        }

        @Override public void close() {
            synchronized (RawEvidenceStore.this) {
                if (!closed) { closed = true; activeBatch = null; pending.clear(); }
            }
        }
    }

    private record Pending(RawEvidenceRef reference, byte[] body) {}

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
        if (activeBatch != null) throw new IllegalStateException("Use the open raw evidence batch");
        requireBounds(body, references.size(), totalBytes);
        long sequence = references.size() + 1L;
        RawEvidenceRef reference = new RawEvidenceRef(path(sequence).toString(), Digests.sha256(body),
                body.length, kind, sequence, startedNanos, completedNanos, group, method, endpoint, status);
        write(reference, body);
        return reference;
    }

    private void requireBounds(byte[] body, long captures, long bytes) throws IOException {
        if (body == null || body.length > MAX_RESPONSE_BYTES
                || captures >= MAX_CAPTURES || bytes + body.length > MAX_TOTAL_BYTES) {
            failed = true;
            throw new IOException("Raw evidence capture bound exceeded or response is unavailable");
        }
    }

    private Path path(long sequence) {
        return directory.resolve(String.format(java.util.Locale.ROOT, "%06d.json", sequence));
    }

    private void write(RawEvidenceRef reference, byte[] body) throws IOException {
        if (failed) throw new IOException("Raw evidence store previously failed");
        Path path = Path.of(reference.path());
        try {
            Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(path, body, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            if (!reference.sha256().equals(Digests.sha256(path, LinkOption.NOFOLLOW_LINKS)))
                throw new IOException("Raw evidence bytes changed while being retained");
            references.add(reference);
            totalBytes += body.length;
        } catch (IOException | RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }
}
