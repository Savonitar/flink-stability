package org.savonitar.flink.stability.runtime.api;

import java.util.Map;
import java.util.Set;

/** Wire-fault allowlist shared by planning and the standalone proxy plugin (SPEC-004). */
public final class KafkaProtocolFaultPolicy {
    public static final Set<String> APIS = Set.of("end-txn", "init-producer-id", "produce",
            "add-partitions-to-txn", "add-offsets-to-txn", "txn-offset-commit", "find-coordinator");
    public static final long MAX_DELAY_MILLIS = 5_000;
    private static final Set<String> TRANSACTION_ERRORS = Set.of("concurrent-transactions",
            "coordinator-load-in-progress", "coordinator-not-available", "not-coordinator");
    private static final Map<String, Set<String>> ERRORS = Map.of(
            "end-txn", TRANSACTION_ERRORS,
            "init-producer-id", TRANSACTION_ERRORS,
            "add-partitions-to-txn", TRANSACTION_ERRORS,
            "add-offsets-to-txn", TRANSACTION_ERRORS,
            "txn-offset-commit", Set.of("coordinator-load-in-progress", "coordinator-not-available", "not-coordinator"),
            "find-coordinator", Set.of("coordinator-not-available"),
            "produce", Set.of("not-leader-or-follower", "request-timed-out", "not-enough-replicas"));
    private KafkaProtocolFaultPolicy() {}
    public static Set<String> errors(String api) { return ERRORS.getOrDefault(api, Set.of()); }
}
