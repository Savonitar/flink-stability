package org.savonitar.flink.stability.core.spec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/** One private, per-validation artifact snapshot tree with explicit ownership. */
final class ArtifactWorkspace {
    private final Path artifactRoot;
    private final Path preparationRoot;
    private boolean closed;

    ArtifactWorkspace(Path artifactRoot, Path preparationRoot) {
        this.artifactRoot = Objects.requireNonNull(artifactRoot, "artifactRoot")
                .toAbsolutePath().normalize();
        this.preparationRoot = Objects.requireNonNull(preparationRoot, "preparationRoot")
                .toAbsolutePath().normalize();
        if (!this.preparationRoot.startsWith(this.artifactRoot)
                || this.preparationRoot.equals(this.artifactRoot)
                || !this.preparationRoot.getFileName().toString().startsWith("plan-")) {
            throw new IllegalArgumentException(
                    "preparationRoot must be a private plan directory under artifactRoot");
        }
    }

    Path artifactRoot() {
        return artifactRoot;
    }

    Path preparationRoot() {
        return preparationRoot;
    }

    synchronized void close() {
        if (closed) {
            return;
        }
        try {
            if (Files.exists(preparationRoot, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                Files.walkFileTree(preparationRoot, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path directory, IOException failure)
                            throws IOException {
                        if (failure != null) {
                            throw failure;
                        }
                        Files.delete(directory);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            closed = true;
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Could not delete prepared artifact workspace " + preparationRoot,
                    exception);
        }
    }
}
