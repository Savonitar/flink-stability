package org.savonitar.flink.stability.faultproxy;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The faults armed in one proxy, shared by every client connection.
 *
 * <p>The harness arms a fault by writing {@code rules/<faultId>.json} into the control directory
 * and heals it by deleting that file. The proxy answers with one JSON line per lifecycle event in
 * {@code events/<faultId>.jsonl}: {@code armed}, one line per affected message,
 * {@code healed}, and at most one {@code retry-observed} per dropped message after heal.
 *
 * <p>A connection first claims a matching message. The claim holds one of the rule's occurrences
 * until the connection either completes it (the message was dropped) or releases it (a response
 * that must not be dropped arrived instead), so no more messages are claimed than requested. A
 * claim made while the rule was armed may complete after the rule is healed.
 */
final class FaultRuleBook implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Path rules;
    private final Path events;
    private final LongSupplier clockMillis;
    private final LongSupplier nanoTime;
    /** Armed rules by fault ID, in ID order so that overlapping rules resolve the same way. */
    private final Map<String, ArmedRule> armed = new TreeMap<>();
    /** Rule files that failed to parse, so each is reported once. */
    private final Set<String> rejected = new HashSet<>();
    /** One possible retry witness per completed drop, shared across client connections. */
    private final Set<Claim> awaitingRetries = new LinkedHashSet<>();
    private ScheduledExecutorService poller;
    private boolean closed;

    FaultRuleBook(Path controlDirectory, LongSupplier clockMillis) {
        this(controlDirectory, clockMillis, System::nanoTime);
    }

    FaultRuleBook(Path controlDirectory, LongSupplier clockMillis, LongSupplier nanoTime) {
        Objects.requireNonNull(controlDirectory, "controlDirectory");
        this.rules = controlDirectory.resolve("rules");
        this.events = controlDirectory.resolve("events");
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /** Starts polling the rules directory. */
    static FaultRuleBook start(Path controlDirectory, Duration pollInterval) {
        FaultRuleBook book = new FaultRuleBook(controlDirectory, System::currentTimeMillis);
        book.poller = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "flink-stability-fault-rules");
            thread.setDaemon(true);
            return thread;
        });
        book.poller.scheduleWithFixedDelay(
                book::refreshQuietly, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
        return book;
    }

    /** Arms new rule files and heals rules whose file is gone. */
    synchronized void refresh() throws IOException {
        Set<String> present = new HashSet<>();
        if (Files.isDirectory(rules)) {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(rules, "*.json")) {
                for (Path file : files) {
                    String fileName = file.getFileName().toString();
                    String faultId = fileName.substring(0, fileName.length() - ".json".length());
                    present.add(faultId);
                    if (!armed.containsKey(faultId) && !rejected.contains(faultId)) {
                        arm(faultId, file);
                    }
                }
            }
        }
        for (String faultId : Set.copyOf(armed.keySet())) {
            if (!present.contains(faultId)) {
                ArmedRule healed = armed.remove(faultId);
                healed.healed = true;
                Map<String, Object> fields = new LinkedHashMap<>();
                fields.put("completed", healed.completed);
                fields.put("pending", healed.pending);
                append(faultId, "healed", fields);
            }
        }
        rejected.retainAll(present);
    }

    /** Claims a message for the first armed rule that matches and has an occurrence left. */
    synchronized Optional<Claim> claim(EndTxnIdentity request) {
        for (ArmedRule candidate : armed.values()) {
            if (candidate.rule.matches(request.transactionalId(), request.committed())
                    && candidate.beforeDeadline(nanoTime.getAsLong())
                    && candidate.completed + candidate.pending < candidate.rule.occurrences()) {
                candidate.pending++;
                return Optional.of(new Claim(candidate, ++candidate.claims, request));
            }
        }
        return Optional.empty();
    }

    /** The claimed message was dropped; returns its one-based occurrence. */
    synchronized int complete(Claim claim) {
        claim.armed.pending--;
        return ++claim.armed.completed;
    }

    /** Observes a matching request, without deciding whether it will be forwarded or dropped. */
    synchronized void observeRetry(EndTxnIdentity request, Map<String, Object> details) {
        var pending = awaitingRetries.iterator();
        while (pending.hasNext()) {
            Claim claim = pending.next();
            if (claim.armed.healed && claim.request.equals(request)) {
                try {
                    record(claim, "retry-observed", details);
                    pending.remove();
                } catch (UncheckedIOException unavailable) {
                    // Optional observation must not disrupt the client's next request. Keep
                    // the witness available for a later match if the event path recovers.
                }
            }
        }
    }

    /** The claimed message will not be dropped, so its occurrence is free again. */
    synchronized void release(Claim claim) {
        claim.armed.pending--;
    }

    /** Appends one evidence line for a claimed message. */
    synchronized void record(Claim claim, String event, Map<String, Object> details) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("claim", claim.sequence());
        fields.put("action", claim.rule().action().wireName());
        fields.putAll(details);
        boolean dropped = "request-dropped".equals(event) || "response-dropped".equals(event);
        if (dropped) {
            fields.put("beforeDeadline", claim.armed.beforeDeadline(nanoTime.getAsLong()));
        }
        appendQuietly(claim.rule().faultId(), event, fields);
        if (dropped && !closed) {
            awaitingRetries.add(claim);
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        awaitingRetries.clear();
        if (poller != null) {
            poller.shutdownNow();
        }
    }

    private void arm(String faultId, Path file) throws IOException {
        FaultRule rule;
        try {
            rule = FaultRule.parse(JSON.readTree(Files.readString(file, StandardCharsets.UTF_8)));
            if (!rule.faultId().equals(faultId)) {
                throw new IllegalArgumentException(
                        "faultId '" + rule.faultId() + "' does not match its file name");
            }
        } catch (IOException | RuntimeException invalid) {
            // A half-written file is never read: the harness moves complete files into place.
            rejected.add(faultId);
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("reason", String.valueOf(invalid.getMessage()));
            append(faultId, "rejected", fields);
            return;
        }
        armed.put(faultId, new ArmedRule(rule, nanoTime.getAsLong()));
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("result", rule.result().wireName());
        rule.transactionalIdPrefix().ifPresent(prefix -> fields.put("transactionalIdPrefix", prefix));
        fields.put("action", rule.action().wireName());
        fields.put("occurrences", rule.occurrences());
        fields.put("triggerDeadlineNanos", rule.triggerDeadlineNanos());
        append(faultId, "armed", fields);
    }

    private void refreshQuietly() {
        try {
            refresh();
        } catch (IOException | RuntimeException failure) {
            // The next poll retries; a rule that never arms is caught by the harness deadline.
        }
    }

    private void appendQuietly(String faultId, String event, Map<String, Object> fields) {
        try {
            append(faultId, event, fields);
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private void append(String faultId, String event, Map<String, Object> details)
            throws IOException {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("event", event);
        line.put("faultId", faultId);
        line.put("timeMillis", clockMillis.getAsLong());
        line.putAll(details);
        Files.createDirectories(events);
        Files.writeString(
                events.resolve(faultId + ".jsonl"),
                JSON.writeValueAsString(line) + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    /** A message claimed by the connection it arrived on; its sequence is unique per rule. */
    static final class Claim {
        private final ArmedRule armed;
        private final int sequence;
        private final EndTxnIdentity request;

        private Claim(ArmedRule armed, int sequence, EndTxnIdentity request) {
            this.armed = armed;
            this.sequence = sequence;
            this.request = request;
        }

        FaultRule rule() {
            return armed.rule;
        }

        int sequence() {
            return sequence;
        }
    }

    /** Original request identity; a response may return a different producer epoch. */
    record EndTxnIdentity(
            String transactionalId, long producerId, short producerEpoch, boolean committed) {}

    /** A rule's counts; guarded by the rule book's lock. */
    private static final class ArmedRule {
        private final FaultRule rule;
        private final long armedAtNanos;
        private int claims;
        private int pending;
        private int completed;
        private boolean expired;
        private boolean healed;

        private ArmedRule(FaultRule rule, long armedAtNanos) {
            this.rule = rule;
            this.armedAtNanos = armedAtNanos;
        }

        private boolean beforeDeadline(long now) {
            // This standalone plugin runs without runtime-api on its classpath. Subtraction
            // handles nanoTime wrap; a backward clock fails closed and cannot reopen a rule.
            long elapsed = now - armedAtNanos;
            expired |= elapsed < 0 || elapsed >= rule.triggerDeadlineNanos();
            return !expired;
        }
    }
}
