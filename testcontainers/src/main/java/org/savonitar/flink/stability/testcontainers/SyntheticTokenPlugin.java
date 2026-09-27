package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Installation bytes stay separate from the subject connector and workload classpaths. */
final class SyntheticTokenPlugin {
    static final String RESOURCE = "token-plugin/flink-stability-token.jar";
    static final String CONTAINER_PATH =
            "/opt/flink/plugins/flink-stability-token/flink-stability-token.jar";
    static final List<String> CLASSES = List.of(
            "org.savonitar.flink.stability.token.SyntheticDelegationTokenProvider",
            "org.savonitar.flink.stability.token.SyntheticDelegationTokenReceiver");
    private final byte[] bytes;
    private final String sha256;

    SyntheticTokenPlugin() {
        try (var input = Objects.requireNonNull(SyntheticTokenPlugin.class.getClassLoader()
                .getResourceAsStream(RESOURCE), "Missing embedded synthetic token plugin")) {
            bytes = input.readAllBytes();
            sha256 = Digests.sha256(new java.io.ByteArrayInputStream(bytes));
        } catch (IOException failure) {
            throw new UncheckedIOException("Could not read embedded synthetic token plugin", failure);
        }
    }

    byte[] bytes() {
        return bytes.clone();
    }

    String sha256() {
        return sha256;
    }

    /** Call against the stopped container after copies, before starting its entrypoint. */
    void verify(Function<String, String> hashInContainer) {
        String actual = hashInContainer.apply(CONTAINER_PATH);
        if (!sha256.equals(actual)) {
            throw new IllegalStateException("Synthetic token plugin checksum mismatch: expected "
                    + sha256 + ", actual " + actual);
        }
    }

    static String configuration(int port, String process, String role) {
        return configuration(port, process, role, Optional.empty());
    }

    static String configuration(int port, String process, String role, Optional<Duration> retryBackoff) {
        if (port < 1 || port > 65535
                || !process.matches("(?:jobmanager|taskmanager)-[1-9][0-9]*#[1-9][0-9]*")
                || !List.of("jobmanager", "taskmanager").contains(role)
                || !process.startsWith(role + "-")) {
            throw new IllegalArgumentException("Invalid synthetic token endpoint or process identity");
        }
        String backoffConfiguration = retryBackoff.map(backoff -> {
            String value = backoff.toMillis() + " ms";
            // 2.2 uses the legacy key; the pinned 2.4 PR has separate initial and maximum keys.
            // Equal bounds deliberately select a fixed retry interval in both implementations.
            return "\nsecurity.delegation.tokens.renewal.retry.backoff: " + value
                    + "\nsecurity.delegation.tokens.renewal.retry.initial.backoff: " + value
                    + "\nsecurity.delegation.tokens.renewal.retry.max.backoff: " + value;
        }).orElse("");
        return "\nsecurity.delegation.tokens.enabled: true"
                + "\nsecurity.delegation.token.provider.flink-stability-synthetic.enabled: true"
                + "\nflink-stability.token-service.endpoint: http://host.testcontainers.internal:" + port
                + "\nflink-stability.token-service.process: " + process
                + "\nflink-stability.token-service.role: " + role
                + backoffConfiguration;
    }
}
