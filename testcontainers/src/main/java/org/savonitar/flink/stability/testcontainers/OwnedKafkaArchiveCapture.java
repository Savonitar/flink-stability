package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Opt-in transport of one declared partition from one retained owner; no archive decoding. */
final class OwnedKafkaArchiveCapture {
    static final long MAX_ARCHIVE_BYTES = 128L * 1024 * 1024;

    /** Bound to one already captured container ID; callers cannot select a different container. */
    interface Driver {
        Inspection inspect();
        InputStream openArchive(String partitionDirectory) throws IOException;
        default KafkaLogInventory.Command listFileNames(String directory, int maximumBytes,
                ContainerOperationDeadline deadline, Runnable checkActive) throws IOException {
            throw new IOException("Owned file inventory is unavailable");
        }
        default KafkaLogInventory.Command fileMetadata(String path, int maximumBytes,
                ContainerOperationDeadline deadline, Runnable checkActive) throws IOException {
            throw new IOException("Owned file metadata is unavailable");
        }
    }

    record Binding(String containerId, String networkId, String networkAlias, String clusterAlias,
                   String configuredImage, long generation) {
        Binding {
            fullId(containerId); fullId(networkId);
            Objects.requireNonNull(networkAlias); Objects.requireNonNull(clusterAlias);
            Objects.requireNonNull(configuredImage);
            if (generation < 1) throw new IllegalArgumentException("No running owner generation");
        }
    }
    record Inspection(String containerId, String imageId, String networkId,
                      List<String> aliases, boolean running) {
        Inspection { aliases = List.copyOf(aliases); }
    }
    record Identity(Binding owner, String imageId, Instant observedAt) {}

    /** All roots are explicitly frozen by the experiment; the owner supplies no guessed default. */
    record Partition(Set<String> approvedLogRoots, String resolvedLogRoot,
                     String declaredOutputTopic, int partition) {
        Partition {
            approvedLogRoots = Set.copyOf(approvedLogRoots);
            if (approvedLogRoots.isEmpty() || approvedLogRoots.size() > 8) {
                throw new IllegalArgumentException("One to eight explicit log roots required");
            }
            approvedLogRoots.forEach(OwnedKafkaArchiveCapture::logRoot);
            logRoot(resolvedLogRoot);
            if (!approvedLogRoots.contains(resolvedLogRoot)
                    || declaredOutputTopic == null || declaredOutputTopic.length() > 249
                    || !declaredOutputTopic.matches("[A-Za-z0-9._-]+")
                    || declaredOutputTopic.equals(".") || declaredOutputTopic.equals("..")
                    || partition < 0) {
                throw new IllegalArgumentException("Unapproved log root or invalid declared topic/partition");
            }
        }
        String directory() { return resolvedLogRoot + "/" + declaredOutputTopic + "-" + partition; }
    }

    enum Status { TRANSPORT_EOF, BYTE_LIMIT, IDENTITY_MISMATCH, IO_FAILURE, ABANDONED_PARTIAL }
    record Receipt(Status status, Path partialArchive, long observedBytes, Optional<String> sha256,
                   boolean workerFinished, Instant startedAt, Instant returnedAt, String detail) {
        Receipt {
            if (sha256.isPresent() != (status == Status.TRANSPORT_EOF)) {
                throw new IllegalArgumentException("Only completed transport has a final archive hash");
            }
        }
    }

    static Identity observe(Binding binding, Driver driver, MonotonicDeadline budget) {
        var deadline = ContainerOperationDeadline.shared("observing owned Kafka identity", budget);
        check(deadline, null);
        return ContainerDriverCallBoundary.call(deadline, "inspecting owned Kafka container", () -> {
            check(deadline, null);
            Identity identity = verified(binding, driver.inspect());
            check(deadline, null);
            return identity;
        });
    }

    /** The owner does not hold its monitor or a cleanup lock while this bounded worker runs.
     * The caller alone may publish a TRANSPORT_EOF archive; it is not validated/atomic tar evidence.
     */
    static Receipt copy(Binding binding, Driver driver, Identity startup, Partition partition,
                        Path evidenceDirectory, String partialName, long maximumBytes,
                        MonotonicDeadline budget) {
        Objects.requireNonNull(partition);
        return copyPath(binding, driver, startup, partition.directory(), evidenceDirectory,
                partialName, maximumBytes, budget);
    }

    static Receipt copyLog(Binding binding, Driver driver, Identity startup,
                          KafkaLogInventory inventory, String basename,
                          Path evidenceDirectory, String partialName, long maximumBytes,
                          MonotonicDeadline budget) {
        var entry = inventory.requireLog(startup, basename);
        if (entry.size() > KafkaLogInventory.MAX_LOG_BYTES) {
            throw new IllegalArgumentException("Listed log exceeds the 16 MiB payload limit");
        }
        if (maximumBytes > KafkaLogInventory.MAX_LOG_BYTES + 64 * 1024) {
            throw new IllegalArgumentException("Single-file transfer exceeds payload plus framing allowance");
        }
        return copyPath(binding, driver, startup, inventory.partition().directory() + "/" + basename,
                evidenceDirectory, partialName, maximumBytes, budget);
    }

    private static Receipt copyPath(Binding binding, Driver driver, Identity startup, String sourcePath,
                        Path evidenceDirectory, String partialName, long maximumBytes,
                        MonotonicDeadline budget) {
        Objects.requireNonNull(driver); Objects.requireNonNull(startup);
        Objects.requireNonNull(budget);
        if (!binding.equals(startup.owner())) throw new IllegalArgumentException("Startup owner identity changed");
        if (maximumBytes < 1 || maximumBytes > MAX_ARCHIVE_BYTES) {
            throw new IllegalArgumentException("Archive limit must be between 1 byte and 128 MiB");
        }
        Path destination = destination(evidenceDirectory, partialName);
        var deadline = ContainerOperationDeadline.shared("copying owned Kafka archive", budget);
        Session session = new Session(destination);
        try {
            check(deadline, session);
            Receipt receipt = ContainerDriverCallBoundary.call(deadline, "copying declared Kafka partition", () ->
                    transfer(binding, driver, startup, sourcePath, maximumBytes, deadline, session));
            check(deadline, session);
            return receipt;
        } catch (ContainerOperationTimeoutException failure) {
            session.abandon();
            return session.receipt(Status.ABANDONED_PARTIAL, false,
                    "Deadline expired; partial file may change until abandoned worker exits", Optional.empty());
        } catch (RuntimeException failure) {
            session.abandon();
            if (Thread.currentThread().isInterrupted()) {
                return session.receipt(Status.ABANDONED_PARTIAL, false,
                        "Caller interrupted; flag retained and partial file may change", Optional.empty());
            }
            throw failure;
        }
    }

    private static Receipt transfer(Binding binding, Driver driver, Identity startup,
            String sourcePath, long maximumBytes, ContainerOperationDeadline deadline, Session session) {
        try {
            check(deadline, session);
            Identity before = verified(binding, driver.inspect());
            if (!before.imageId().equals(startup.imageId())) throw new IdentityMismatch();
            check(deadline, session);
            // Recheck the caller-owned directory just before exclusive creation. No extraction.
            destination(session.path.getParent(), session.path.getFileName().toString());
            try (OutputStream out = Files.newOutputStream(session.path,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                check(deadline, session);
                InputStream stream = Objects.requireNonNull(driver.openArchive(sourcePath));
                session.stream.set(stream);
                Throwable primary = null;
                try {
                    check(deadline, session);
                    MessageDigest digest = sha256();
                    byte[] buffer = new byte[16 * 1024];
                    while (true) {
                        check(deadline, session);
                        // One extra byte proves a cap violation; it is not written past the cap.
                        int wanted = (int) Math.min(buffer.length, maximumBytes - session.bytes.get() + 1);
                        int count = stream.read(buffer, 0, wanted);
                        check(deadline, session);
                        if (count == -1) break;
                        if (count < 0 || count > wanted) throw new IOException("Invalid archive stream count");
                        if (count == 0) continue;
                        int accepted = (int) Math.min(count, maximumBytes - session.bytes.get());
                        if (accepted > 0) {
                            out.write(buffer, 0, accepted);
                            digest.update(buffer, 0, accepted);
                            session.bytes.addAndGet(accepted);
                        }
                        check(deadline, session);
                        if (accepted < count) {
                            return session.receipt(Status.BYTE_LIMIT, true,
                                    "Archive exceeded byte cap; retained exact bounded prefix", Optional.empty());
                        }
                    }
                    out.flush();
                    check(deadline, session);
                    Identity after = verified(binding, driver.inspect());
                    if (!after.imageId().equals(startup.imageId())) throw new IdentityMismatch();
                    check(deadline, session);
                    // Close both streams before reporting a final hash; close failures stay partial.
                    session.closeStream();
                    out.close();
                    check(deadline, session);
                    return session.receipt(Status.TRANSPORT_EOF, true,
                            "Transport EOF only; live archive is neither atomic nor tar-validated",
                            Optional.of(HexFormat.of().formatHex(digest.digest())));
                } catch (IOException | RuntimeException | Error failure) {
                    primary = failure;
                    throw failure;
                } finally {
                    if (primary == null) {
                        session.closeStream();
                    } else {
                        try { session.closeStream(); }
                        catch (IOException | RuntimeException | Error closeFailure) {
                            if (closeFailure != primary) primary.addSuppressed(closeFailure);
                        }
                    }
                }
            }
        } catch (IdentityMismatch failure) {
            return session.receipt(Status.IDENTITY_MISMATCH, true,
                    "Owned container/image/network/alias no longer matches startup", Optional.empty());
        } catch (IOException failure) {
            return session.receipt(Status.IO_FAILURE, true,
                    "Archive transport/file IO failed: " + failure.getClass().getSimpleName(), Optional.empty());
        } catch (ContainerOperationTimeoutException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            return session.receipt(Status.IO_FAILURE, true,
                    "Owned Docker driver failed: " + failure.getClass().getSimpleName(), Optional.empty());
        }
    }

    static Identity verified(Binding binding, Inspection inspection) {
        if (inspection == null || !inspection.running()
                || !binding.containerId().equals(inspection.containerId())
                || !binding.networkId().equals(inspection.networkId())
                || !inspection.aliases().contains(binding.networkAlias())
                || inspection.imageId() == null || !inspection.imageId().matches("sha256:[0-9a-f]{64}")) {
            throw new IdentityMismatch();
        }
        return new Identity(binding, inspection.imageId(), Instant.now());
    }

    private static Path destination(Path directory, String filename) {
        Objects.requireNonNull(directory);
        if (!directory.isAbsolute() || !directory.normalize().equals(directory)
                || filename == null || !filename.matches("[A-Za-z0-9_-][A-Za-z0-9._-]*\\.part")) {
            throw new IllegalArgumentException("Absolute normalized evidence directory and simple .part name required");
        }
        Path current = directory.getRoot();
        for (Path component : directory) {
            current = current.resolve(component);
            if (Files.isSymbolicLink(current) || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("Evidence directory must exist without symbolic links");
            }
        }
        Path path = directory.resolve(filename);
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Archive destination must be new and exclusive");
        }
        return path;
    }

    private static void logRoot(String root) {
        if (root == null || !root.startsWith("/") || root.equals("/") || root.endsWith("/")
                || root.length() > 1024 || root.contains("\\")) {
            throw new IllegalArgumentException("Explicit canonical container log root required");
        }
        for (String component : root.substring(1).split("/", -1)) {
            if (!component.matches("[A-Za-z0-9._-]+") || component.equals(".") || component.equals("..")) {
                throw new IllegalArgumentException("Unsafe container log root");
            }
        }
    }
    private static void fullId(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Full Docker ID required");
    }
    private static void check(ContainerOperationDeadline deadline, Session session) {
        if (Thread.currentThread().isInterrupted() || (session != null && session.abandoned.get())) {
            throw deadline.timedOut("interrupted or abandoned archive", null);
        }
        deadline.remaining("copying owned Kafka bytes");
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    private static final class IdentityMismatch extends RuntimeException {}
    private static final class Session {
        final Path path;
        final Instant startedAt = Instant.now();
        final AtomicLong bytes = new AtomicLong();
        final AtomicBoolean abandoned = new AtomicBoolean();
        final AtomicReference<InputStream> stream = new AtomicReference<>();
        Session(Path path) { this.path = path; }
        Receipt receipt(Status status, boolean finished, String detail, Optional<String> hash) {
            return new Receipt(status, path, bytes.get(), hash, finished, startedAt, Instant.now(), detail);
        }
        void closeStream() throws IOException {
            InputStream current = stream.getAndSet(null);
            if (current != null) current.close();
        }
        void abandon() {
            abandoned.set(true);
            // Closing a misbehaving driver stream may itself block; never pin caller/owner cleanup.
            Thread closer = new Thread(() -> {
                try { closeStream(); } catch (IOException | RuntimeException ignored) { }
            }, "owned-kafka-archive-close");
            closer.setDaemon(true); closer.start();
        }
    }
}
