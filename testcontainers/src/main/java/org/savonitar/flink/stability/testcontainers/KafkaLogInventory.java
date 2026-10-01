package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Bounded observations of an exact owned partition; equal metadata is not an atomic snapshot. */
final class KafkaLogInventory {
    static final int MAX_INVENTORY_BYTES = 64 * 1024;
    static final int MAX_ENTRIES = 64;
    static final int MAX_LOGS = 32;
    static final long MAX_LOG_BYTES = 16L * 1024 * 1024;

    enum Status { OBSERVED, UNSTABLE, INVALID, IO_FAILURE, BYTE_LIMIT, ABANDONED }
    enum Comparison { UNCHANGED_METADATA, CHANGED, UNAVAILABLE }
    record Entry(String basename, long size, long modifiedSeconds, long inode, boolean log) {}
    record Command(byte[] stdout, byte[] stderr, long exitCode, boolean finished) {
        Command { stdout = stdout.clone(); stderr = stderr.clone(); }
        @Override public byte[] stdout() { return stdout.clone(); }
        @Override public byte[] stderr() { return stderr.clone(); }
    }

    private final OwnedKafkaArchiveCapture.Identity startup;
    private final OwnedKafkaArchiveCapture.Partition partition;
    private final Status status;
    private final List<Entry> entries;
    private final byte[] transcript;
    private final boolean workerFinished;
    private final Instant startedAt;
    private final Instant returnedAt;
    private final String detail;

    private KafkaLogInventory(OwnedKafkaArchiveCapture.Identity startup,
            OwnedKafkaArchiveCapture.Partition partition, Status status, List<Entry> entries,
            byte[] transcript, boolean workerFinished, Instant startedAt, String detail) {
        this.startup = startup;
        this.partition = partition;
        this.status = status;
        this.entries = List.copyOf(entries);
        this.transcript = transcript.clone();
        this.workerFinished = workerFinished;
        this.startedAt = startedAt;
        this.returnedAt = Instant.now();
        this.detail = detail;
    }

    Status status() { return status; }
    OwnedKafkaArchiveCapture.Partition partition() { return partition; }
    List<Entry> entries() { return entries; }
    byte[] transcript() { return transcript.clone(); }
    boolean workerFinished() { return workerFinished; }
    Instant startedAt() { return startedAt; }
    Instant returnedAt() { return returnedAt; }
    String detail() { return detail; }
    String sha256() { return Digests.sha256(transcript); }

    Entry requireLog(OwnedKafkaArchiveCapture.Identity expected, String basename) {
        if (startup != expected || status != Status.OBSERVED) {
            throw new IllegalArgumentException("Exact startup identity and stable observed inventory required");
        }
        return entries.stream().filter(entry -> entry.log() && entry.basename().equals(basename))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Log is not in the observed inventory"));
    }

    Comparison compare(KafkaLogInventory after) {
        if (after == null || startup != after.startup || !partition.equals(after.partition)
                || status != Status.OBSERVED || after.status != Status.OBSERVED) {
            return Comparison.UNAVAILABLE;
        }
        return entries.equals(after.entries) ? Comparison.UNCHANGED_METADATA : Comparison.CHANGED;
    }

    static KafkaLogInventory capture(OwnedKafkaArchiveCapture.Binding binding,
            OwnedKafkaArchiveCapture.Driver driver, OwnedKafkaArchiveCapture.Identity startup,
            OwnedKafkaArchiveCapture.Partition partition, int maximumBytes, MonotonicDeadline budget) {
        Objects.requireNonNull(driver); Objects.requireNonNull(startup); Objects.requireNonNull(partition);
        if (!binding.equals(startup.owner())) throw new IllegalArgumentException("Startup owner changed");
        if (maximumBytes < 1 || maximumBytes > MAX_INVENTORY_BYTES) {
            throw new IllegalArgumentException("Inventory byte limit must be 1..65536");
        }
        var session = new Transcript(maximumBytes);
        Instant startedAt = Instant.now();
        var deadline = ContainerOperationDeadline.shared("listing owned Kafka partition", budget);
        try {
            var inventory = ContainerDriverCallBoundary.call(deadline, "listing owned Kafka partition", () -> {
                try {
                    verify(binding, driver, startup, deadline, session);
                    session.check(deadline);
                    byte[] names = session.command(driver.listFileNames(partition.directory(), session.remaining(), deadline, () -> session.check(deadline)));
                    session.check(deadline);
                    List<String> paths = names(names, partition.directory());
                    var entries = new ArrayList<Entry>();
                    boolean unstable = false;
                    for (String path : paths) {
                        session.check(deadline);
                        byte[] raw = session.command(driver.fileMetadata(path, session.remaining(), deadline, () -> session.check(deadline)));
                        session.check(deadline);
                        String[] fields = ascii(raw).strip().split(" ", -1);
                        if (fields.length != 4 || !fields[0].matches("[0-9a-fA-F]{4,8}")
                                || !fields[1].matches("[0-9]{1,19}") || !fields[2].matches("-?[0-9]{1,19}")
                                || !fields[3].matches("[0-9]{1,19}")) throw new IllegalArgumentException("Invalid stat response");
                        long type = Long.parseLong(fields[0], 16) & 0170000;
                        long size = Long.parseLong(fields[1]), modified = Long.parseLong(fields[2]);
                        long inode = Long.parseLong(fields[3]);
                        if (path.equals(partition.directory())) {
                            if (type != 0040000) throw new IllegalArgumentException("Partition is not a directory");
                            continue;
                        }
                        if (type != 0100000) throw new IllegalArgumentException("Non-regular partition entry");
                        String name = path.substring(partition.directory().length() + 1);
                        boolean log = name.matches("[0-9]{20}\\.log");
                        if (log) Long.parseLong(name.substring(0, 20));
                        else if (name.endsWith(".log")) throw new IllegalArgumentException("Invalid log basename");
                        unstable |= name.endsWith(".swap") || name.endsWith(".cleaned") || name.endsWith(".deleted");
                        entries.add(new Entry(name, size, modified, inode, log));
                    }
                    if (entries.stream().filter(Entry::log).count() > MAX_LOGS) throw new IllegalArgumentException("Too many selected logs");
                    entries.sort(java.util.Comparator.comparing(Entry::basename));
                    verify(binding, driver, startup, deadline, session);
                    return result(startup, partition, unstable ? Status.UNSTABLE : Status.OBSERVED,
                            entries, session, true, startedAt, "Bounded metadata only; concurrent changes may escape observation");
                } catch (ContainerOperationTimeoutException timeout) {
                    throw timeout;
                } catch (UnfinishedCommand unfinished) {
                    return result(startup, partition, Status.ABANDONED, List.of(), session, false, startedAt,
                            "Inventory command termination unconfirmed; stop collection and clean up the owner");
                } catch (InventoryLimit limit) {
                    return result(startup, partition, Status.BYTE_LIMIT, List.of(), session, true, startedAt, "Inventory byte limit reached");
                } catch (IllegalArgumentException invalid) {
                    return result(startup, partition, Status.INVALID, List.of(), session, true, startedAt, invalid.getMessage());
                } catch (IOException | RuntimeException failure) {
                    return result(startup, partition, Status.IO_FAILURE, List.of(), session, true, startedAt, failure.getClass().getSimpleName());
                }
            });
            session.check(deadline);
            return inventory;
        } catch (RuntimeException failure) {
            if (!(failure instanceof ContainerOperationTimeoutException) && !Thread.currentThread().isInterrupted()) throw failure;
            session.abandoned.set(true);
            return result(startup, partition, Status.ABANDONED, List.of(), session, false, startedAt,
                    "Deadline/interruption; stop collecting further targets and clean up the owned container");
        }
    }

    private static KafkaLogInventory result(OwnedKafkaArchiveCapture.Identity startup,
            OwnedKafkaArchiveCapture.Partition partition, Status status, List<Entry> entries,
            Transcript session, boolean finished, Instant startedAt, String detail) {
        return new KafkaLogInventory(startup, partition, status, entries, session.bytes(), finished, startedAt, detail);
    }

    private static void verify(OwnedKafkaArchiveCapture.Binding binding, OwnedKafkaArchiveCapture.Driver driver,
            OwnedKafkaArchiveCapture.Identity startup, ContainerOperationDeadline deadline, Transcript session) {
        session.check(deadline);
        var current = OwnedKafkaArchiveCapture.verified(binding, driver.inspect());
        if (!current.imageId().equals(startup.imageId())) throw new IllegalArgumentException("Startup image changed");
        session.check(deadline);
    }

    private static List<String> names(byte[] bytes, String directory) {
        String text = ascii(bytes);
        if (!text.endsWith("\0")) throw new IllegalArgumentException("Unterminated file listing");
        String[] names = text.substring(0, text.length() - 1).split("\0", -1);
        if (names.length < 1 || names.length > MAX_ENTRIES + 1 || !names[0].equals(directory)) {
            throw new IllegalArgumentException("Missing root or excessive inventory entries");
        }
        var unique = new HashSet<String>();
        for (int i = 1; i < names.length; i++) {
            if (!names[i].startsWith(directory + "/")) throw new IllegalArgumentException("Foreign inventory path");
            String name = names[i].substring(directory.length() + 1);
            if (!name.matches("[A-Za-z0-9._-]{1,255}") || name.equals(".") || name.equals("..") || !unique.add(name)) {
                throw new IllegalArgumentException("Unsafe or duplicate inventory name");
            }
        }
        return List.of(names);
    }

    private static String ascii(byte[] bytes) {
        for (byte value : bytes) if (value < 0) throw new IllegalArgumentException("Non-ASCII metadata");
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static final class InventoryLimit extends RuntimeException {}
    private static final class UnfinishedCommand extends RuntimeException {}
    private static final class Transcript {
        final int maximum;
        final AtomicBoolean abandoned = new AtomicBoolean();
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Transcript(int maximum) { this.maximum = maximum; }
        void check(ContainerOperationDeadline deadline) {
            if (abandoned.get() || Thread.currentThread().isInterrupted()) {
                throw deadline.timedOut("abandoned inventory worker", null);
            }
            deadline.remaining("observing owned Kafka inventory");
        }
        synchronized int remaining() {
            int remaining = maximum - bytes.size() - 3 * Integer.BYTES - Long.BYTES;
            if (remaining < 1) throw new InventoryLimit();
            return remaining;
        }
        synchronized void append(byte[] raw) {
            Objects.requireNonNull(raw);
            int remaining = maximum - bytes.size() - Integer.BYTES;
            if (remaining < 0) throw new InventoryLimit();
            int count = Math.min(raw.length, remaining);
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(raw.length).array());
            bytes.write(raw, 0, count);
            if (raw.length > remaining) throw new InventoryLimit();
        }
        synchronized byte[] command(Command command) throws IOException {
            append(ByteBuffer.allocate(Long.BYTES).putLong(command.exitCode()).array());
            append(command.stdout());
            append(command.stderr());
            if (!command.finished()) throw new UnfinishedCommand();
            if (command.exitCode() != 0 || command.stderr().length != 0) throw new IOException("Inventory command failed");
            return command.stdout();
        }
        synchronized byte[] bytes() { return bytes.toByteArray(); }
    }
}
