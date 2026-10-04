package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.Digests;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.*;

/** An explicit post-fence copy; unsafe or incomplete trees are reported, never silently skipped. */
public record CheckpointRetentionEvidence(String sourceRoot, String outputRoot, String checkpointRoot,
                                          String haRoot, String status, List<File> files, long bytes,
                                          List<String> diagnostics) {
    public static final String INCOMPLETE = "checkpoint.retention-incomplete";
    public record File(String path, long size, String sha256) {}
    public CheckpointRetentionEvidence { files = List.copyOf(files); diagnostics = List.copyOf(diagnostics); }

    public static CheckpointRetentionEvidence copy(Path source, Path output, boolean fenced) {
        Path root = source.toAbsolutePath().normalize();
        Path target = output.toAbsolutePath().normalize();
        var files = new ArrayList<File>();
        var diagnostics = new ArrayList<String>();
        Path copied = target.resolve("checkpoints");
        boolean destinationCreated = false;
        if (!fenced) return new CheckpointRetentionEvidence(root.toString(), target.toString(), null, null,
                "not-copied", files, 0, List.of("Process fence is unconfirmed; checkpoint state may still be changing"));
        try {
            rejectSymlinks(root);
            rejectSymlinks(target);
            if (target.startsWith(root) || root.startsWith(target))
                throw new IOException("Checkpoint source and destination must not overlap");
            if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Attempt checkpoint root is unavailable");
            Files.createDirectories(target.getParent());
            Files.createDirectory(target); // Never merge into or overwrite retained evidence.
            destinationCreated = true;
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes) throws IOException {
                    if (attributes.isSymbolicLink()) throw new IOException("Symbolic checkpoint directory rejected: " + directory);
                    Files.createDirectory(copied.resolve(root.relativize(directory)));
                    return FileVisitResult.CONTINUE;
                }
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    if (!attributes.isRegularFile() || attributes.isSymbolicLink())
                        throw new IOException("Nonregular checkpoint entry rejected: " + file);
                    Path destination = copied.resolve(root.relativize(file));
                    var digest = sha256();
                    long written = 0;
                    try (var input = FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                         var outputFile = FileChannel.open(destination, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                        ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
                        while (input.read(buffer) != -1) {
                            if (Thread.currentThread().isInterrupted()) throw new IOException("Checkpoint copy interrupted");
                            buffer.flip();
                            digest.update(buffer.asReadOnlyBuffer());
                            while (buffer.hasRemaining()) written += outputFile.write(buffer);
                            buffer.clear();
                        }
                    }
                    var after = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (written != attributes.size() || !Objects.equals(attributes.fileKey(), after.fileKey())
                            || after.size() != written || !after.lastModifiedTime().equals(attributes.lastModifiedTime()))
                        throw new IOException("Checkpoint source changed during copy: " + file);
                    String hash = HexFormat.of().formatHex(digest.digest());
                    try (var readBack = Files.newInputStream(destination, LinkOption.NOFOLLOW_LINKS)) {
                        if (!hash.equals(Digests.sha256(readBack))) throw new IOException("Copied checkpoint checksum mismatch: " + destination);
                    }
                    files.add(new File(target.relativize(destination).toString(), written, hash));
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException | RuntimeException failure) { diagnostics.add(failure.getClass().getSimpleName() + ": " + failure.getMessage()); }
        return new CheckpointRetentionEvidence(root.toString(), target.toString(),
                destinationCreated && Files.isDirectory(copied, LinkOption.NOFOLLOW_LINKS) ? copied.toString() : null,
                destinationCreated && Files.isDirectory(copied.resolve("ha"), LinkOption.NOFOLLOW_LINKS) ? copied.resolve("ha").toString() : null,
                diagnostics.isEmpty() ? "complete" : "incomplete", files,
                files.stream().mapToLong(File::size).sum(), diagnostics);
    }

    private static void rejectSymlinks(Path path) throws IOException {
        for (Path cursor = path; cursor != null; cursor = cursor.getParent())
            if (Files.isSymbolicLink(cursor)) throw new IOException("Checkpoint path contains a symbolic link: " + cursor);
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
