package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/** Bounded component observations, deliberately independent of every verdict predicate. */
public record ComponentErrorEvidence(List<Event> events, List<String> diagnostics) {
    public static final int MAX_EVENTS = 512;
    private static final long MAX_READ_BYTES = 32L * 1024 * 1024;
    private static final Pattern HEADER = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[,.]\\d{3})\\s+(?:\\[[^]]*]\\s+)?(ERROR|WARN|INFO)\\s+(\\S+)\\s+.*? - (.*)$");
    private static final Pattern PRODUCER = Pattern.compile("\\bproducerId[= :]+(-?\\d+)");
    private static final Pattern EPOCH = Pattern.compile("\\b(?:producerEpoch|epoch)[= :]+(-?\\d+)");
    private static final Pattern TRANSACTION = Pattern.compile("\\btransactionalId[= :]+(?:'([^']*)'|\\\"([^\\\"]*)\\\"|([^,}\\]\\s]+))");

    public record Event(String process, String timestamp, String level, String logger, Optional<String> producerId,
                        Optional<String> epoch, Optional<String> transactionalId, List<String> kinds, String message) {
        public Event(String process, String timestamp, String level, String logger, Optional<String> producerId,
                     Optional<String> epoch, Optional<String> transactionalId, List<String> kinds) {
            this(process, timestamp, level, logger, producerId, epoch, transactionalId, kinds, "");
        }
        public Event { kinds = List.copyOf(kinds); }
    }
    private record Key(String process, String timestamp, String level, String logger, Optional<String> producerId,
                       Optional<String> epoch, Optional<String> transactionalId, String message) {}

    public ComponentErrorEvidence {
        events = List.copyOf(events);
        diagnostics = List.copyOf(diagnostics);
    }

    public static ComponentErrorEvidence empty() { return new ComponentErrorEvidence(List.of(), List.of("Component logs not collected")); }

    public static ComponentErrorEvidence collect(List<FlinkComponentLog> logs) {
        if (logs.isEmpty()) return empty();
        var found = new LinkedHashMap<Key, LinkedHashSet<String>>();
        var issues = new LinkedHashSet<String>();
        long remaining = MAX_READ_BYTES;
        int files = 0;
        for (var log : logs) {
            if (++files > 128 || remaining == 0) { issues.add("Component log read budget exhausted"); break; }
            log.error().ifPresent(error -> issues.add(log.process() + ": " + error));
            if (log.truncated()) issues.add(log.process() + ": output capture truncated");
            if (!Files.isRegularFile(log.path(), LinkOption.NOFOLLOW_LINKS)) {
                issues.add(log.process() + ": retained output unavailable");
                continue;
            }
            int limit = (int) Math.min(remaining, FlinkComponentLog.MAX_BYTES);
            try (var input = Files.newInputStream(log.path(), LinkOption.NOFOLLOW_LINKS)) {
                byte[] bytes = input.readNBytes(limit + 1);
                remaining -= Math.min(limit, bytes.length);
                if (bytes.length > limit) issues.add(log.process() + ": read byte limit exceeded");
                String text = new String(bytes, 0, Math.min(bytes.length, limit), StandardCharsets.UTF_8);
                if (!text.isEmpty() && !text.endsWith("\n")) {
                    issues.add(log.process() + ": unfinished line omitted");
                    text = text.substring(0, text.lastIndexOf('\n') + 1);
                }
                try (var reader = new BufferedReader(new StringReader(text))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.length() > 8192) { issues.add(log.process() + ": oversized line omitted"); continue; }
                        var match = HEADER.matcher(line);
                        if (!match.matches()) continue; // Stack traces have no independent log header.
                        String logger = match.group(3), message = match.group(4);
                        if (!logger.toLowerCase(Locale.ROOT).contains("kafka")) continue;
                        List<String> kinds = kinds(logger, message);
                        if (kinds.isEmpty()) continue;
                        var tx = TRANSACTION.matcher(message);
                        Optional<String> transaction = Optional.empty();
                        if (tx.find()) for (int i = 1; i <= 3; i++) if (tx.group(i) != null) transaction = Optional.of(tx.group(i));
                        if (transaction.isEmpty() && kinds.contains("commit-retriable")) {
                            String beginning = "Encountered retriable exception while committing ";
                            if (message.startsWith(beginning) && message.endsWith("."))
                                transaction = Optional.of(message.substring(beginning.length(), message.length() - 1));
                        }
                        var key = new Key(log.process(), match.group(1).replace('.', ','), match.group(2), logger, field(PRODUCER, message),
                                field(EPOCH, message), transaction, message);
                        if (!found.containsKey(key) && found.size() == MAX_EVENTS) {
                            issues.add("Component event retention limit exceeded");
                            continue;
                        }
                        found.computeIfAbsent(key, ignored -> new LinkedHashSet<>()).addAll(kinds);
                    }
                }
            } catch (IOException | RuntimeException failure) {
                issues.add(log.process() + ": " + failure.getClass().getSimpleName());
            }
        }
        return new ComponentErrorEvidence(found.entrySet().stream().map(entry -> {
            var key = entry.getKey();
            return new Event(key.process(), key.timestamp(), key.level(), key.logger(), key.producerId(), key.epoch(), key.transactionalId(),
                    List.copyOf(entry.getValue()), key.message());
        }).toList(), List.copyOf(issues));
    }

    private static Optional<String> field(Pattern pattern, String text) {
        var match = pattern.matcher(text);
        return match.find() ? Optional.of(match.group(1)) : Optional.empty();
    }

    private static List<String> kinds(String logger, String message) {
        var result = new ArrayList<String>();
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("already fenced") || message.contains("ProducerFenced")) result.add("producer-fenced");
        if (message.contains("InvalidPidMapping")) result.add("invalid-pid-mapping");
        if (message.contains("InvalidTxnState")) result.add("invalid-txn-state");
        if (lower.contains("transaction") && lower.contains("aborted")) result.add("transaction-aborted");
        if (lower.contains("transaction") && lower.contains("expired")) result.add("transaction-expired");
        if (logger.equals("org.apache.flink.connector.kafka.sink.internal.KafkaCommitter")) {
            if (message.startsWith("Encountered retriable exception while committing ")) result.add("commit-retriable");
            else if (result.isEmpty() && (message.startsWith("Unable to commit transaction (")
                    || message.startsWith("Transaction (") && lower.contains("encountered error"))) result.add("commit-failed");
            else if (message.startsWith("Committing transaction (") && lower.contains("was interrupted")) result.add("commit-interrupted");
        }
        return result;
    }
}
