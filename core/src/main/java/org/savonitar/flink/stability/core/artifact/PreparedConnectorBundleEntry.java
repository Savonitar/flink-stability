package org.savonitar.flink.stability.core.artifact;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** One unique, verified JAR in the deterministic cluster-level connector bundle. */
public final class PreparedConnectorBundleEntry {
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    private final int globalClasspathIndex;
    private final String fileName;
    private final String containerPath;
    private final Path stagedPath;
    private final String sha256;
    private final List<ConnectorBundleContribution> contributions;

    PreparedConnectorBundleEntry(
            int globalClasspathIndex,
            String fileName,
            String containerPath,
            Path stagedPath,
            String sha256,
            List<ConnectorBundleContribution> contributions) {
        if (globalClasspathIndex < 0) {
            throw new IllegalArgumentException(
                    "globalClasspathIndex must be non-negative");
        }
        this.globalClasspathIndex = globalClasspathIndex;
        this.fileName = requireNonBlank(fileName, "fileName");
        if (fileName.indexOf('/') >= 0) {
            throw new IllegalArgumentException("fileName must be a single path segment");
        }
        this.containerPath = requireNonBlank(containerPath, "containerPath");
        if (!containerPath.equals("/opt/flink/lib/" + fileName)) {
            throw new IllegalArgumentException(
                    "containerPath must place fileName in /opt/flink/lib");
        }
        this.stagedPath = Objects.requireNonNull(stagedPath, "stagedPath")
                .toAbsolutePath().normalize();
        this.sha256 = Objects.requireNonNull(sha256, "sha256");
        if (!SHA_256.matcher(sha256).matches()) {
            throw new IllegalArgumentException(
                    "sha256 must be 64 lowercase hexadecimal characters");
        }
        if (!fileName.endsWith("-" + sha256 + ".jar")) {
            throw new IllegalArgumentException(
                    "fileName must include the verified SHA-256");
        }
        this.contributions = List.copyOf(Objects.requireNonNull(
                contributions, "contributions"));
        if (this.contributions.isEmpty()) {
            throw new IllegalArgumentException("A bundle entry needs a contributor");
        }
        if (this.contributions.stream()
                .anyMatch(contribution -> !sha256.equals(contribution.closureEntry().sha256()))) {
            throw new IllegalArgumentException(
                    "Every contributor must name the bundle entry SHA-256");
        }
    }

    public int globalClasspathIndex() {
        return globalClasspathIndex;
    }

    public String fileName() {
        return fileName;
    }

    public String containerPath() {
        return containerPath;
    }

    public Path stagedPath() {
        return stagedPath;
    }

    public String sha256() {
        return sha256;
    }

    public List<ConnectorBundleContribution> contributions() {
        return contributions;
    }
}
