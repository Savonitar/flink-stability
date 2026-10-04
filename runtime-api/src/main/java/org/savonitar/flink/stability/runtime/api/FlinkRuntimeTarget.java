package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.runtime.api.Checks.requireDockerImageId;
import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;
import static org.savonitar.flink.stability.runtime.api.Checks.requireSha256;
/** One effective Flink image and its mandatory verified connector-bundle installation. */
public final class FlinkRuntimeTarget {
    public static final int TASK_SLOTS_PER_TASK_MANAGER = 2;
    /** Bounds eager topology allocation and local Docker fan-out before provisioning. */
    public static final int MAX_TASK_MANAGERS = 16;

    private final String imageReference;
    private final FlinkConnectorBundleInstallation connectorBundle;
    private final Optional<String> expectedImageId;
    private final Optional<RuntimeJar> expectedRuntimeJar;
    private final int taskManagers;
    private final Optional<HighAvailability> highAvailability;
    private final Optional<TokenProvider> tokenProvider;
    private final Optional<String> declaredLine;
    private final Map<String, String> config;
    private final java.util.List<FlinkLogMarker> logMarkers;

    private FlinkRuntimeTarget(
            String imageReference,
            FlinkConnectorBundleInstallation connectorBundle,
            Optional<String> expectedImageId,
            Optional<RuntimeJar> expectedRuntimeJar,
            int taskManagers,
            Optional<HighAvailability> highAvailability,
            Optional<TokenProvider> tokenProvider,
            Optional<String> declaredLine, Map<String, String> config, java.util.List<FlinkLogMarker> logMarkers) {
        this.imageReference = requireNonBlank(imageReference, "imageReference");
        this.connectorBundle = Objects.requireNonNull(connectorBundle, "connectorBundle");
        this.expectedImageId = Objects.requireNonNull(expectedImageId, "expectedImageId");
        this.expectedRuntimeJar = Objects.requireNonNull(expectedRuntimeJar, "expectedRuntimeJar");
        if (taskManagers < 1 || taskManagers > MAX_TASK_MANAGERS) {
            throw new IllegalArgumentException("taskManagers must be between 1 and "
                    + MAX_TASK_MANAGERS);
        }
        this.taskManagers = taskManagers;
        this.highAvailability = Objects.requireNonNull(highAvailability, "highAvailability");
        this.tokenProvider = Objects.requireNonNull(tokenProvider, "tokenProvider");
        this.declaredLine = Objects.requireNonNull(declaredLine, "declaredLine");
        this.config = FlinkConfiguration.validate(config);
        this.logMarkers = FlinkLogMarker.validate(logMarkers);
    }

    /**
     * v1 execution path, including a valid manifest that may contain zero entries. The supplied
     * image must exactly equal the image committed by the structured target binding.
     */
    public static FlinkRuntimeTarget withConnectorBundle(
            String imageReference,
            FlinkConnectorBundleInstallation connectorBundle) {
        imageReference = requireNonBlank(imageReference, "imageReference");
        connectorBundle = Objects.requireNonNull(connectorBundle, "connectorBundle");
        if (!imageReference.equals(connectorBundle.targetFlinkImageReference())) {
            throw new IllegalArgumentException(
                    "Runtime image must exactly match connector bundle target image: "
                            + imageReference + " versus "
                            + connectorBundle.targetFlinkImageReference());
        }
        return new FlinkRuntimeTarget(imageReference, connectorBundle, Optional.empty(), Optional.empty(),
                1, Optional.empty(), Optional.empty(), Optional.empty(), Map.of(), java.util.List.of());
    }

    /** Requires every physical Flink process to use this local Docker image identity. */
    public FlinkRuntimeTarget withExpectedImageId(String imageId) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle,
                Optional.of(requireDockerImageId(imageId, "expectedImageId")), expectedRuntimeJar,
                taskManagers, highAvailability, tokenProvider, declaredLine, config, logMarkers);
    }

    /** Requires the selected runtime JAR bytes in every physical Flink process. */
    public FlinkRuntimeTarget withExpectedRuntimeJar(RuntimeJar runtimeJar) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId,
                Optional.of(Objects.requireNonNull(runtimeJar, "runtimeJar")), taskManagers,
                highAvailability, tokenProvider, declaredLine, config, logMarkers);
    }

    /** Keeps one JobManager and provisions this many named TaskManager slots. */
    public FlinkRuntimeTarget withTaskManagers(int count) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId,
                expectedRuntimeJar, count, highAvailability, tokenProvider, declaredLine, config, logMarkers);
    }

    public FlinkRuntimeTarget withHighAvailability(HighAvailability configuration) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId,
                expectedRuntimeJar, taskManagers, Optional.of(configuration), tokenProvider, declaredLine, config, logMarkers);
    }

    public FlinkRuntimeTarget withTokenProvider(TokenProvider configuration) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId,
                expectedRuntimeJar, taskManagers, highAvailability, Optional.of(configuration), declaredLine, config, logMarkers);
    }

    public FlinkRuntimeTarget withDeclaredLine(String line) {
        if (line == null || !line.matches("(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)")) {
            throw new IllegalArgumentException("Declared Flink line must be major.minor");
        }
        if (expectedImageId.isEmpty() || expectedRuntimeJar.isEmpty()) {
            throw new IllegalArgumentException("Declared Flink line requires image_id and runtime_jar pins");
        }
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId,
                expectedRuntimeJar, taskManagers, highAvailability, tokenProvider, Optional.of(line), config, logMarkers);
    }

    public FlinkRuntimeTarget withConfig(Map<String, String> configuration) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId,
                expectedRuntimeJar, taskManagers, highAvailability, tokenProvider, declaredLine, configuration, logMarkers);
    }

    public Optional<String> declaredLine() { return declaredLine; }
    public Map<String, String> config() { return config; }
    public java.util.List<FlinkLogMarker> logMarkers() { return logMarkers; }
    public FlinkRuntimeTarget withLogMarkers(java.util.List<FlinkLogMarker> markers) {
        return new FlinkRuntimeTarget(imageReference, connectorBundle, expectedImageId, expectedRuntimeJar,
                taskManagers, highAvailability, tokenProvider, declaredLine, config, markers);
    }

    public Optional<HighAvailability> highAvailability() {
        return highAvailability;
    }

    public Optional<TokenProvider> tokenProvider() {
        return tokenProvider;
    }

    public int jobManagers() {
        return highAvailability.isPresent() ? 2 : 1;
    }

    public record HighAvailability(String zookeeperImage, Duration sessionTimeout) {
        public HighAvailability {
            zookeeperImage = requireNonBlank(zookeeperImage, "zookeeperImage");
            requirePositive(sessionTimeout, "sessionTimeout");
            if (sessionTimeout.toMillis() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("sessionTimeout exceeds ZooKeeper's millisecond range");
            }
        }
    }

    /** Explicit proof requirements; omission preserves the existing service-wide evidence contract. */
    public enum TokenProofScope { BOOTSTRAP, SUBMITTED_JOB }

    public record TokenProvider(Duration renewalInterval, Optional<Duration> retryBackoff,
                                Optional<TokenProofScope> proofScope) {
        public TokenProvider(Duration renewalInterval) {
            this(renewalInterval, Optional.empty(), Optional.empty());
        }

        public TokenProvider(Duration renewalInterval, Optional<Duration> retryBackoff) {
            this(renewalInterval, retryBackoff, Optional.empty());
        }

        public TokenProvider {
            Objects.requireNonNull(proofScope, "proofScope");
            requirePositive(renewalInterval, "renewalInterval");
            if (renewalInterval.compareTo(Duration.ofMillis(50)) < 0
                    || renewalInterval.compareTo(Duration.ofMinutes(30)) > 0) {
                throw new IllegalArgumentException("renewalInterval must be between 50 ms and 30 minutes");
            }
            Objects.requireNonNull(retryBackoff, "retryBackoff").ifPresent(backoff -> {
                if (backoff.compareTo(Duration.ofSeconds(1)) < 0
                        || backoff.compareTo(Duration.ofMinutes(5)) > 0) {
                    throw new IllegalArgumentException("retryBackoff must be between 1 second and 5 minutes");
                }
            });
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (Objects.requireNonNull(value, name).isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    public int taskManagers() {
        return taskManagers;
    }

    public String imageReference() {
        return imageReference;
    }

    public FlinkConnectorBundleInstallation connectorBundle() {
        return connectorBundle;
    }

    public Optional<String> expectedImageId() {
        return expectedImageId;
    }

    public Optional<RuntimeJar> expectedRuntimeJar() {
        return expectedRuntimeJar;
    }

    /** One directly addressed distribution JAR, with no host path or shell interpretation. */
    public record RuntimeJar(String containerPath, String sha256) {
        private static final Pattern PATH = Pattern.compile(
                "/opt/flink/lib/flink-dist(?:_[A-Za-z0-9][A-Za-z0-9._+-]*)?-[A-Za-z0-9][A-Za-z0-9._+-]*\\.jar");

        public RuntimeJar {
            containerPath = requireNonBlank(containerPath, "containerPath");
            sha256 = requireSha256(sha256, "Runtime JAR sha256");
            if (!PATH.matcher(containerPath).matches()) {
                throw new IllegalArgumentException(
                        "Runtime JAR path must directly name /opt/flink/lib/flink-dist-<version>.jar"
                                + " or /opt/flink/lib/flink-dist_<scala>-<version>.jar"
                                + " using only letters, digits, dots, underscores, pluses, and hyphens");
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof FlinkRuntimeTarget target
                && imageReference.equals(target.imageReference)
                && Objects.equals(connectorBundle, target.connectorBundle)
                && expectedImageId.equals(target.expectedImageId)
                && expectedRuntimeJar.equals(target.expectedRuntimeJar)
                && taskManagers == target.taskManagers
                && highAvailability.equals(target.highAvailability)
                && tokenProvider.equals(target.tokenProvider)
                && declaredLine.equals(target.declaredLine) && config.equals(target.config) && logMarkers.equals(target.logMarkers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(imageReference, connectorBundle, expectedImageId, expectedRuntimeJar,
                taskManagers, highAvailability, tokenProvider, declaredLine, config, logMarkers);
    }

    @Override
    public String toString() {
        return imageReference + " (TaskManagers " + taskManagers
                + ", binding " + connectorBundle.targetBindingSha256()
                + expectedImageId.map(value -> ", expected image " + value).orElse("")
                + expectedRuntimeJar.map(value -> ", runtime JAR " + value).orElse("") + ")";
    }
}
