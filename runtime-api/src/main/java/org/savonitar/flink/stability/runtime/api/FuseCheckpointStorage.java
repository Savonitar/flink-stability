package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Prepared, immutable PR3 helper bundle; no scenario-selected container filesystem paths. */
public record FuseCheckpointStorage(Path bundleRoot, String manifestSha256, String imageId,
                                    String platform, Map<String, String> files) {
    public static final String SUPPORTED_PLATFORM = "linux/arm64";
    public static final Set<String> REQUIRED_FILES = Set.of("checkpoint-fuse", "checkpoint-fuse-selftest", "probe.jar");

    /** Process-level values owned by this opt-in storage capability, observed in every incarnation. */
    public static final Map<String, String> PROCESS_CONFIGURATION = Map.of(
            "execution.checkpointing.data-inline-threshold", "0 b",
            "execution.checkpointing.max-concurrent-checkpoints", "1",
            "execution.checkpointing.tolerable-failed-checkpoints", "10");

    public static Map<String, String> processConfiguration(Map<String, String> custom) {
        var result = new java.util.TreeMap<>(FlinkConfiguration.validate(custom));
        PROCESS_CONFIGURATION.forEach((key, value) -> {
            if (result.putIfAbsent(key, value) != null)
                throw new IllegalArgumentException("FUSE-owned configuration conflicts: " + key);
        });
        return java.util.Collections.unmodifiableMap(result);
    }

    public FuseCheckpointStorage {
        Objects.requireNonNull(bundleRoot, "bundleRoot");
        if (!bundleRoot.isAbsolute() || !bundleRoot.normalize().equals(bundleRoot))
            throw new IllegalArgumentException("Prepared helper bundle must be an absolute normalized path");
        manifestSha256 = Checks.requireSha256(manifestSha256, "manifestSha256");
        imageId = Checks.requireDockerImageId(imageId, "imageId");
        if (!SUPPORTED_PLATFORM.equals(platform))
            throw new IllegalArgumentException("FUSE checkpoint storage requires the linux/arm64 platform");
        files = Map.copyOf(files);
        if (!files.keySet().equals(REQUIRED_FILES)) throw new IllegalArgumentException("Incomplete or unexpected helper bundle files");
        files.forEach((name, digest) -> Checks.requireSha256(digest, "helper " + name));
    }
}
