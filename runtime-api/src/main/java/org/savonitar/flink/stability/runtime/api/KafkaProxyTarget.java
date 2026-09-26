package org.savonitar.flink.stability.runtime.api;

import java.util.Objects;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/**
 * One protocol-aware Kafka proxy in front of a started cluster (SPEC-004 K2). Clients that route
 * through it bootstrap from {@code listenHost:listenPort}; the proxy connects upstream to the
 * cluster's in-network listener.
 */
public record KafkaProxyTarget(
        String proxyAlias,
        String listenHost,
        int listenPort,
        String upstreamBootstrapServers) {
    private static final Pattern KEBAB = Pattern.compile("^[a-z0-9]+(?:-[a-z0-9]+)*$");

    public KafkaProxyTarget {
        Objects.requireNonNull(proxyAlias, "proxyAlias");
        Objects.requireNonNull(listenHost, "listenHost");
        if (!KEBAB.matcher(proxyAlias).matches() || !KEBAB.matcher(listenHost).matches()) {
            throw new IllegalArgumentException(
                    "Proxy alias and listen host must be lower-kebab-case");
        }
        if (listenPort < 1 || listenPort > 65_535) {
            throw new IllegalArgumentException("listenPort must be a TCP port: " + listenPort);
        }
        requireNonBlank(upstreamBootstrapServers, "upstreamBootstrapServers");
    }

    public String bootstrapServers() {
        return listenHost + ":" + listenPort;
    }
}
