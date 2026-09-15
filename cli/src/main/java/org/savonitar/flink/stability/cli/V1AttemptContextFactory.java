package org.savonitar.flink.stability.cli;

import org.savonitar.flink.stability.core.execution.V1AttemptContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/** Allocates one isolated, harness-owned checkpoint namespace for a CLI attempt. */
final class V1AttemptContextFactory {
    private static final int ATTEMPT_ORDINAL = 1;
    private static final int MAX_NONCE_ATTEMPTS = 32;

    private final Path checkpointBase;
    private final Supplier<String> nonceSource;

    V1AttemptContextFactory() {
        this(defaultCheckpointBase(), V1AttemptContextFactory::randomNonce);
    }

    V1AttemptContextFactory(Path checkpointBase, Supplier<String> nonceSource) {
        this.checkpointBase = Objects.requireNonNull(checkpointBase, "checkpointBase")
                .toAbsolutePath()
                .normalize();
        this.nonceSource = Objects.requireNonNull(nonceSource, "nonceSource");
    }

    V1AttemptContext create() throws IOException {
        Files.createDirectories(checkpointBase);
        Path realBase = checkpointBase.toRealPath();
        if (!Files.isDirectory(realBase) || !Files.isWritable(realBase)) {
            throw new IOException(
                    "Checkpoint base is not a writable directory: " + realBase);
        }

        for (int attempt = 0; attempt < MAX_NONCE_ATTEMPTS; attempt++) {
            String nonce = Objects.requireNonNull(
                    nonceSource.get(), "nonceSource returned null");
            V1AttemptContext context = new V1AttemptContext(
                    ATTEMPT_ORDINAL,
                    nonce,
                    realBase.resolve("attempt-" + ATTEMPT_ORDINAL + "-" + nonce));
            Path root = context.checkpointStorageRoot();
            if (!root.getParent().equals(realBase)) {
                throw new IOException(
                        "Attempt checkpoint root escaped its checkpoint base: " + root);
            }
            // FlinkContainer creates the selected directory with container-writable permissions.
            // Leaving it absent here preserves that ownership hand-off while the random, exact
            // child name keeps attempts isolated from retained prior evidence.
            if (Files.notExists(root, LinkOption.NOFOLLOW_LINKS)) {
                return context;
            }
        }
        throw new IOException(
                "Could not allocate a unique attempt checkpoint root below " + realBase);
    }

    private static Path defaultCheckpointBase() {
        String reactorRoot = System.getProperty("maven.multiModuleProjectDirectory");
        Path workingRoot = reactorRoot == null || reactorRoot.isBlank()
                ? Path.of("")
                : Path.of(reactorRoot);
        return workingRoot.resolve("checkpoints");
    }

    private static String randomNonce() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
}
