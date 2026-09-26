package org.savonitar.flink.stability.faultproxy;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One armed fault: which EndTxn messages it matches and what it does to the first
 * {@code occurrences} of them (SPEC-004 K5.1a). The harness writes it as a JSON file.
 */
record FaultRule(
        String faultId,
        Result result,
        Optional<String> transactionalIdPrefix,
        Action action,
        int occurrences,
        long triggerDeadlineNanos) {
    private static final Set<String> FIELDS = Set.of(
            "faultId", "api", "result", "transactionalIdPrefix", "action", "occurrences",
            "triggerDeadlineNanos");

    FaultRule {
        Objects.requireNonNull(faultId, "faultId");
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(transactionalIdPrefix, "transactionalIdPrefix");
        Objects.requireNonNull(action, "action");
        if (occurrences < 1) {
            throw new IllegalArgumentException("occurrences must be positive");
        }
        if (triggerDeadlineNanos < 1) {
            throw new IllegalArgumentException("triggerDeadlineNanos must be positive");
        }
    }

    /** The EndTxn outcome a rule selects. */
    enum Result {
        COMMIT,
        ABORT,
        ANY;

        String wireName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** What happens to a matching message. */
    enum Action {
        /** The request never reaches the broker, and the client never hears back. */
        DROP_REQUEST,
        /** The broker acts on the request, but the client never sees the response. */
        DROP_RESPONSE;

        String wireName() {
            return name().toLowerCase(Locale.ROOT).replace('_', '-');
        }
    }

    /** Parses a rule strictly; anything unknown is an error rather than a silent no-op. */
    static FaultRule parse(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("rule must be a JSON object");
        }
        node.fieldNames().forEachRemaining(field -> {
            if (!FIELDS.contains(field)) {
                throw new IllegalArgumentException("unknown rule field '" + field + "'");
            }
        });
        String api = text(node, "api");
        if (!"end-txn".equals(api)) {
            throw new IllegalArgumentException("unsupported api '" + api + "'");
        }
        Result result = node.has("result")
                ? Result.valueOf(text(node, "result").toUpperCase(Locale.ROOT))
                : Result.ANY;
        Optional<String> prefix = node.has("transactionalIdPrefix")
                ? Optional.of(text(node, "transactionalIdPrefix"))
                : Optional.empty();
        Action action = Action.valueOf(
                text(node, "action").toUpperCase(Locale.ROOT).replace('-', '_'));
        JsonNode occurrences = node.get("occurrences");
        if (occurrences == null || !occurrences.isIntegralNumber() || !occurrences.canConvertToInt()) {
            throw new IllegalArgumentException("occurrences must be an integer");
        }
        JsonNode deadline = node.get("triggerDeadlineNanos");
        if (deadline == null || !deadline.isIntegralNumber() || !deadline.canConvertToLong()) {
            throw new IllegalArgumentException("triggerDeadlineNanos must be an integer");
        }
        return new FaultRule(text(node, "faultId"), result, prefix, action,
                occurrences.intValue(), deadline.longValue());
    }

    boolean matches(String transactionalId, boolean committed) {
        boolean resultMatches = switch (result) {
            case COMMIT -> committed;
            case ABORT -> !committed;
            case ANY -> true;
        };
        return resultMatches && transactionalIdPrefix
                .map(prefix -> transactionalId != null && transactionalId.startsWith(prefix))
                .orElse(true);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException(field + " must be a non-blank string");
        }
        return value.textValue();
    }
}
