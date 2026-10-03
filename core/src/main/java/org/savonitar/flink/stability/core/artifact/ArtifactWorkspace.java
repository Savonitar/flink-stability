package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
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
    private static final Path STAGING_DIRECTORY = Path.of(
            ".flink-stability", "artifacts", "prepared");

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

    static Path canonicalArtifactRoot(Path artifactRoot, Path source) {
        if (!Files.isDirectory(artifactRoot)) {
            throw new SpecificationException(Stage.ARTIFACT, List.of(new Diagnostic(
                    source,
                    ResolutionScope.COMMON,
                    "artifact.root.not-directory",
                    "$",
                    "Artifact root is not an existing directory: " + artifactRoot)));
        }
        try {
            return artifactRoot.toRealPath();
        } catch (IOException exception) {
            throw new SpecificationException(Stage.ARTIFACT, List.of(new Diagnostic(
                    source,
                    ResolutionScope.COMMON,
                    "artifact.root.unavailable",
                    "$",
                    "Artifact root is unavailable: " + safeMessage(exception))));
        }
    }

    static ArtifactWorkspace createWorkspace(Path artifactRoot, Path source) {
        try {
            Path realParent = artifactRoot;
            for (Path component : STAGING_DIRECTORY) {
                Path next = realParent.resolve(component.toString());
                if (Files.exists(next, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    if (Files.isSymbolicLink(next)
                            || !Files.isDirectory(next, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException(
                                "Private artifact staging component is not a real directory: "
                                        + next);
                    }
                } else {
                    try {
                        Files.createDirectory(next);
                    } catch (FileAlreadyExistsException ignored) {
                        if (Files.isSymbolicLink(next)
                                || !Files.isDirectory(
                                        next, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                            throw new IOException(
                                    "Private artifact staging component is not a real directory: "
                                            + next);
                        }
                    }
                }
                Path realNext = next.toRealPath();
                if (!realNext.getParent().equals(realParent)
                        || !realNext.startsWith(artifactRoot)) {
                    throw new IOException(
                            "Private artifact staging directory resolves outside artifact root");
                }
                realParent = realNext;
            }
            Path directory = Files.createTempDirectory(realParent, "plan-").toRealPath();
            if (!directory.getParent().equals(realParent)
                    || !directory.startsWith(artifactRoot)) {
                throw new IOException(
                        "Private artifact plan directory resolves outside artifact root");
            }
            return new ArtifactWorkspace(artifactRoot, directory);
        } catch (IOException exception) {
            throw new SpecificationException(Stage.ARTIFACT, List.of(new Diagnostic(
                    source,
                    ResolutionScope.COMMON,
                    "artifact.staging.failed",
                    "$",
                    "Could not create private artifact staging: "
                            + safeMessage(exception))));
        }
    }

    void closeAfterFailure(RuntimeException failure) {
        try {
            close();
        } catch (RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    static Optional<StagedArtifact> stage(
            ArtifactRole role,
            Path resolvedPath,
            Path artifactRoot,
            Path stagingDirectory,
            BiFunction<String, String, Diagnostic> diagnostic,
            List<Diagnostic> issues) {
        Path temporary = null;
        try {
            temporary = Files.createTempFile(stagingDirectory, ".copy-", ".tmp");
            try (InputStream input = Files.newInputStream(
                    resolvedPath, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                Files.copy(input, temporary, StandardCopyOption.REPLACE_EXISTING);
            }
            String sha256 = uncheckedSha256(temporary);
            String suffix = role.isJar() ? ".jar" : ".data";
            Path target = stagingDirectory.resolve(sha256 + suffix);
            if (!Files.exists(target)) {
                try {
                    Files.move(temporary, target);
                    temporary = null;
                } catch (FileAlreadyExistsException ignored) {
                    // Another resolver materialized the same content concurrently.
                }
            }
            if (Files.isSymbolicLink(target)) {
                issues.add(diagnostic.apply("artifact.staging.invalid",
                        "Prepared artifact must not be a symbolic link"));
                return Optional.empty();
            }
            Path staged = target.toRealPath();
            if (!staged.getParent().equals(stagingDirectory)
                    || !staged.startsWith(artifactRoot)
                    || !Files.isRegularFile(staged)) {
                issues.add(diagnostic.apply("artifact.staging.invalid",
                        "Prepared artifact is not a regular file under the artifact root"));
                return Optional.empty();
            }
            String stagedHash = uncheckedSha256(staged);
            if (!sha256.equals(stagedHash)) {
                issues.add(diagnostic.apply("artifact.staging.corrupt",
                        "Content-addressed artifact cache contains bytes with the wrong SHA-256"));
                return Optional.empty();
            }
            return Optional.of(new StagedArtifact(staged, sha256));
        } catch (IOException | ChecksumFailure exception) {
            Throwable detail = exception instanceof ChecksumFailure
                    ? exception.getCause()
                    : exception;
            issues.add(diagnostic.apply("artifact.staging.failed",
                    "Could not materialize a private artifact copy: " + safeMessage(detail)));
            return Optional.empty();
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException ignored) {
                    // Best-effort cleanup of a private temporary copy.
                }
            }
        }
    }

    static String uncheckedSha256(Path path) {
        try {
            return Digests.sha256(path);
        } catch (IOException exception) {
            throw new ChecksumFailure(exception);
        }
    }

    static String safeMessage(Throwable throwable) {
        return throwable.getMessage() == null
                ? throwable.getClass().getSimpleName()
                : throwable.getMessage();
    }

    record StagedArtifact(Path path, String sha256) {}

    static final class ChecksumFailure extends RuntimeException {
        private ChecksumFailure(Throwable cause) {
            super(cause);
        }
    }
}
