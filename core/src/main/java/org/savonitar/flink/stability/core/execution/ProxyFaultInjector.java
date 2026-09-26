package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.KafkaProxyEndpoint;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Injects counted EndTxn faults through the control directory that the proxy's fault filter
 * shares with the harness (SPEC-004 K6.11). It writes a rule, waits for the filter to arm it,
 * waits until every occurrence completed or the trigger deadline passed, heals the rule, and
 * returns what the filter recorded.
 */
final class ProxyFaultInjector implements ExecutablePhaseExecutor.NetworkFaults {
    /** How long the filter may take to confirm that it armed or healed a rule. */
    static final Duration ACKNOWLEDGEMENT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String proxyImage;
    private final Path rules;
    private final Path events;
    private final LongSupplier nanoTime;
    private final PhaseSleeper sleeper;

    ProxyFaultInjector(KafkaProxyEndpoint proxy, LongSupplier nanoTime, PhaseSleeper sleeper) {
        Objects.requireNonNull(proxy, "proxy");
        this.proxyImage = proxy.imageReference();
        this.rules = proxy.controlDirectory().resolve("rules");
        this.events = proxy.controlDirectory().resolve("events");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    @Override
    public PhaseExecutionEvidence.NetworkFault inject(
            String path,
            ExecutableScenarioPlan.EndTxnFault fault) throws IOException, InterruptedException {
        String faultId = path.substring("$/".length()).replace('/', '-');
        Path rule = rules.resolve(faultId + ".json");
        writeRule(faultId, fault);
        try {
            return armTriggerAndHeal(path, faultId, rule, fault);
        } catch (IOException | InterruptedException | RuntimeException failure) {
            // The runner's own heal (SPEC-004 K7.2): never leave a rule armed after a failure.
            try {
                Files.deleteIfExists(rule);
            } catch (IOException healFailure) {
                failure.addSuppressed(healFailure);
            }
            throw failure;
        }
    }

    private PhaseExecutionEvidence.NetworkFault armTriggerAndHeal(
            String path,
            String faultId,
            Path rule,
            ExecutableScenarioPlan.EndTxnFault fault) throws IOException, InterruptedException {
        List<JsonNode> seen = await(faultId, ACKNOWLEDGEMENT_TIMEOUT,
                lines -> first(lines, "armed").isPresent()
                        || first(lines, "rejected").isPresent());
        Optional<JsonNode> rejected = first(seen, "rejected");
        if (rejected.isPresent()) {
            throw new IOException("The proxy rejected fault " + faultId + ": "
                    + rejected.get().path("reason").asText());
        }
        JsonNode armed = first(seen, "armed").orElseThrow(() -> new IOException(
                "The proxy did not arm fault " + faultId + " within " + ACKNOWLEDGEMENT_TIMEOUT));

        boolean requestsDropped =
                fault.action() == ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST;
        String droppedEvent = requestsDropped ? "request-dropped" : "response-dropped";
        // The filter measures this budget from arm on its own monotonic clock. Harness
        // scheduling and delayed file visibility cannot turn a late drop into a trigger.
        await(faultId, fault.triggerDeadline(), lines -> all(lines, droppedEvent).stream()
                .filter(drop -> qualified(drop, requestsDropped))
                .count() >= fault.occurrences());

        Files.delete(rule);
        List<JsonNode> recorded = await(faultId, ACKNOWLEDGEMENT_TIMEOUT,
                lines -> first(lines, "healed").isPresent());
        JsonNode healed = first(recorded, "healed").orElseThrow(() -> new IOException(
                "The proxy did not heal fault " + faultId + " within " + ACKNOWLEDGEMENT_TIMEOUT));

        List<PhaseExecutionEvidence.DroppedMessage> dropped = new ArrayList<>();
        for (JsonNode drop : all(recorded, droppedEvent)) {
            // A dropped response carries the broker's answer; its request fields were recorded
            // when the same claim was forwarded.
            JsonNode request = requestsDropped
                    ? drop
                    : all(recorded, "request-forwarded").stream()
                            .filter(forwarded -> forwarded.path("claim").intValue()
                                    == drop.path("claim").intValue())
                            .findFirst()
                            .orElseThrow(() -> new IOException("The proxy dropped a response of "
                                    + faultId + " whose request it never recorded"));
            dropped.add(new PhaseExecutionEvidence.DroppedMessage(
                    drop.path("occurrence").intValue(),
                    drop.path("claim").intValue(),
                    drop.path("timeMillis").longValue(),
                    drop.path("beforeDeadline").isBoolean()
                            && drop.path("beforeDeadline").booleanValue(),
                    request.path("transactionalId").asText(),
                    request.path("producerId").longValue(),
                    (short) request.path("producerEpoch").intValue(),
                    request.path("committed").booleanValue(),
                    requestsDropped
                            ? Optional.empty()
                            : Optional.of(new PhaseExecutionEvidence.BrokerAnswer(
                                    drop.path("error").asText(),
                                    drop.path("producerId").longValue(),
                                    (short) drop.path("producerEpoch").intValue())),
                    Optional.empty()));
        }
        return new PhaseExecutionEvidence.NetworkFault(
                path,
                faultId,
                fault.proxy(),
                proxyImage,
                fault.action(),
                fault.occurrences(),
                fault.triggerDeadline(),
                armed.path("timeMillis").longValue(),
                healed.path("timeMillis").longValue(),
                dropped,
                all(recorded, "response-forwarded").stream()
                        .map(forwarded -> forwarded.path("error").asText())
                        .toList());
    }

    /**
     * Adds, for each dropped message, the retry the proxy saw after the fault healed (SPEC-004
     * K6.12). A retry typically comes a whole client request timeout after the drop, long after
     * the step ended, so the runner calls this after the process fence, when no client can send
     * again.
     */
    @Override
    public PhaseExecutionEvidence.NetworkFault withObservedRetries(
            PhaseExecutionEvidence.NetworkFault fault) throws IOException {
        List<JsonNode> retries = all(read(fault.faultId()), "retry-observed");
        return fault.withDropped(fault.dropped().stream()
                .map(message -> retries.stream()
                        .filter(retry -> retry.path("claim").intValue() == message.claim())
                        .findFirst()
                        .map(retry -> message.withRetry(new PhaseExecutionEvidence.Retry(
                                retry.path("timeMillis").longValue(),
                                retry.path("clientId").asText())))
                        .orElse(message))
                .toList());
    }

    /** Moves a complete rule into place, so the filter never reads a partial file. */
    private void writeRule(String faultId, ExecutableScenarioPlan.EndTxnFault fault)
            throws IOException {
        ObjectNode rule = JSON.createObjectNode();
        rule.put("faultId", faultId);
        rule.put("api", "end-txn");
        fault.result().ifPresent(result -> rule.put("result",
                result.name().toLowerCase(Locale.ROOT)));
        fault.transactionalIdPrefix().ifPresent(prefix ->
                rule.put("transactionalIdPrefix", prefix));
        rule.put("action", fault.action().name().toLowerCase(Locale.ROOT).replace('_', '-'));
        rule.put("occurrences", fault.occurrences());
        long deadlineNanos;
        try {
            deadlineNanos = fault.triggerDeadline().toNanos();
        } catch (ArithmeticException tooLong) {
            deadlineNanos = Long.MAX_VALUE;
        }
        rule.put("triggerDeadlineNanos", deadlineNanos);
        Path staged = rules.resolve("." + faultId + ".tmp");
        Files.writeString(staged, JSON.writeValueAsString(rule), StandardCharsets.UTF_8);
        Files.move(staged, rules.resolve(faultId + ".json"), StandardCopyOption.ATOMIC_MOVE);
    }

    /** Polls the fault's evidence until the condition holds or the timeout passes. */
    private List<JsonNode> await(
            String faultId,
            Duration timeout,
            Predicate<List<JsonNode>> condition) throws IOException, InterruptedException {
        MonotonicDeadline deadline = MonotonicDeadline.start(timeout, nanoTime);
        while (true) {
            List<JsonNode> lines = read(faultId);
            Duration remaining = deadline.remaining();
            if (condition.test(lines) || remaining.isZero()) {
                return lines;
            }
            sleeper.sleep(remaining.compareTo(POLL_INTERVAL) < 0 ? remaining : POLL_INTERVAL);
        }
    }

    private List<JsonNode> read(String faultId) throws IOException {
        Path file = events.resolve(faultId + ".jsonl");
        if (!Files.exists(file)) {
            return List.of();
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        List<JsonNode> parsed = new ArrayList<>();
        for (int index = 0; index < lines.size(); index++) {
            try {
                parsed.add(JSON.readTree(lines.get(index)));
            } catch (IOException partial) {
                // The filter may still be writing the last line; the next poll reads it whole.
                if (index < lines.size() - 1) {
                    throw partial;
                }
            }
        }
        return parsed;
    }

    private static Optional<JsonNode> first(List<JsonNode> lines, String event) {
        return lines.stream().filter(line -> event.equals(line.path("event").asText())).findFirst();
    }

    private static List<JsonNode> all(List<JsonNode> lines, String event) {
        return lines.stream().filter(line -> event.equals(line.path("event").asText())).toList();
    }

    private static boolean qualified(JsonNode drop, boolean requestsDropped) {
        return drop.path("beforeDeadline").isBoolean()
                && drop.path("beforeDeadline").booleanValue()
                && (requestsDropped || "NONE".equals(drop.path("error").asText()));
    }
}
