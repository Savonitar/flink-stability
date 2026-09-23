package org.savonitar.flink.stability.core.spec.resolution;

import java.util.Map;
import java.util.Set;

/** Fail-closed v1 Kafka API matching and safe error-response profiles. */
final class KafkaFaultApiRegistry {
    enum EndpointKind {
        SOURCE,
        SINK,
        INPUT
    }

    enum TopicBinding {
        ENDPOINT,
        SOURCE,
        FORBIDDEN
    }

    record Profile(
            Set<EndpointKind> endpointKinds,
            TopicBinding topicBinding,
            boolean transactional,
            Set<String> allowedErrors) {
        Profile {
            endpointKinds = Set.copyOf(endpointKinds);
            allowedErrors = Set.copyOf(allowedErrors);
        }
    }

    private static final Set<String> LEADER_ERRORS = Set.of(
            "not-leader-or-follower",
            "request-timed-out");
    private static final Set<String> COORDINATOR_ERRORS = Set.of(
            "coordinator-not-available",
            "request-timed-out");

    private static final Map<String, Profile> PROFILES = Map.ofEntries(
            Map.entry("produce", new Profile(
                    Set.of(EndpointKind.SINK, EndpointKind.INPUT),
                    TopicBinding.ENDPOINT,
                    false,
                    LEADER_ERRORS)),
            Map.entry("fetch", new Profile(
                    Set.of(EndpointKind.SOURCE),
                    TopicBinding.ENDPOINT,
                    false,
                    LEADER_ERRORS)),
            Map.entry("metadata", new Profile(
                    Set.of(EndpointKind.SOURCE, EndpointKind.SINK, EndpointKind.INPUT),
                    TopicBinding.ENDPOINT,
                    false,
                    Set.of())),
            Map.entry("init-producer-id", new Profile(
                    Set.of(EndpointKind.SINK),
                    TopicBinding.FORBIDDEN,
                    true,
                    COORDINATOR_ERRORS)),
            Map.entry("add-partitions-to-txn", new Profile(
                    Set.of(EndpointKind.SINK),
                    TopicBinding.ENDPOINT,
                    true,
                    LEADER_ERRORS)),
            Map.entry("add-offsets-to-txn", new Profile(
                    Set.of(EndpointKind.SINK),
                    TopicBinding.FORBIDDEN,
                    true,
                    COORDINATOR_ERRORS)),
            Map.entry("txn-offset-commit", new Profile(
                    Set.of(EndpointKind.SINK),
                    TopicBinding.SOURCE,
                    true,
                    COORDINATOR_ERRORS)),
            Map.entry("end-txn", new Profile(
                    Set.of(EndpointKind.SINK),
                    TopicBinding.FORBIDDEN,
                    true,
                    COORDINATOR_ERRORS)));

    private KafkaFaultApiRegistry() {}

    static Profile profile(String api) {
        return PROFILES.get(api);
    }

    static Set<String> apis() {
        return PROFILES.keySet();
    }
}
