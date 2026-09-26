package org.savonitar.flink.stability.faultproxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FaultRuleBookTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path control;

    @Test
    void armsClaimsTheFirstOccurrencesAndHealsWhenTheRuleFileIsGone() throws IOException {
        FaultRuleBook book = new FaultRuleBook(control, () -> 1_000L);
        writeRule("f1", """
                {"faultId": "f1", "api": "end-txn", "result": "commit",
                 "transactionalIdPrefix": "eos", "action": "drop-response", "occurrences": 1}
                """);
        book.refresh();

        assertTrue(book.claim(identity("other-0-1", true)).isEmpty(), "prefix must match");
        assertTrue(book.claim(identity("eos-0-1", false)).isEmpty(), "an abort is not a commit");
        FaultRuleBook.Claim claim = book.claim(identity("eos-0-1", true)).orElseThrow();
        assertEquals(1, claim.sequence());
        book.record(claim, "request-forwarded", Map.of("correlationId", 7));
        assertEquals(1, book.complete(claim));
        assertTrue(book.claim(identity("eos-0-2", true)).isEmpty(), "only one occurrence was requested");

        Files.delete(control.resolve("rules/f1.json"));
        book.refresh();

        List<JsonNode> events = events("f1");
        assertEquals(List.of("armed", "request-forwarded", "healed"),
                events.stream().map(event -> event.get("event").textValue()).toList());
        assertEquals("drop-response", events.get(0).get("action").textValue());
        assertEquals(1, events.get(1).get("claim").intValue());
        assertEquals(7, events.get(1).get("correlationId").intValue());
        assertEquals(1, events.get(2).get("completed").intValue());
        assertEquals(0, events.get(2).get("pending").intValue());
        assertEquals(1_000L, events.get(2).get("timeMillis").longValue());
    }

    @Test
    void aPendingClaimHoldsItsOccurrenceUntilCompletedOrReleased() throws IOException {
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L);
        writeRule("f4", """
                {"faultId": "f4", "api": "end-txn", "action": "drop-response", "occurrences": 1}
                """);
        book.refresh();

        FaultRuleBook.Claim first = book.claim(identity("eos-0-1", true)).orElseThrow();
        assertTrue(book.claim(identity("eos-0-1", true)).isEmpty(),
                "a concurrent message must not claim the occurrence a pending one holds");
        book.release(first);
        FaultRuleBook.Claim second = book.claim(identity("eos-0-1", true)).orElseThrow();
        assertEquals(2, second.sequence());
        assertEquals(1, book.complete(second), "a released claim does not count");
        assertTrue(book.claim(identity("eos-0-1", true)).isEmpty());
    }

    @Test
    void theBudgetStartsWhenTheFilterArmsAndExpiresWithoutWaitingForFileDeletion() throws IOException {
        AtomicLong clock = new AtomicLong(50);
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L, clock::get);
        writeRule("deadline", """
                {"faultId":"deadline","api":"end-txn","action":"drop-request",
                 "occurrences":3,"triggerDeadlineNanos":10}
                """);
        clock.set(100);
        book.refresh();
        clock.set(109);
        FaultRuleBook.Claim claim = book.claim(identity("eos-0-1", true)).orElseThrow();
        book.record(claim, "request-dropped", Map.of("occurrence", book.complete(claim)));
        assertTrue(events("deadline").getLast().path("beforeDeadline").booleanValue());
        clock.set(110);
        assertTrue(book.claim(identity("eos-0-2", true)).isEmpty(), "expiry is exclusive");
        assertTrue(Files.exists(control.resolve("rules/deadline.json")),
                "expiry must not depend on the harness healing the rule");
        clock.set(109);
        assertTrue(book.claim(identity("eos-0-3", true)).isEmpty(), "an expired rule cannot reopen");
    }

    @Test
    void aBackwardClockFailsClosedAndWrappingNanoTimeKeepsItsBudget() throws IOException {
        AtomicLong clock = new AtomicLong(Long.MAX_VALUE - 4);
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L, clock::get);
        writeRule("clock", """
                {"faultId":"clock","api":"end-txn","action":"drop-request",
                 "occurrences":3,"triggerDeadlineNanos":10}
                """);
        book.refresh();
        clock.set(Long.MIN_VALUE + 1);
        FaultRuleBook.Claim claim = book.claim(identity("eos-0-1", true)).orElseThrow();
        book.complete(claim);
        clock.set(Long.MAX_VALUE - 5);
        assertTrue(book.claim(identity("eos-0-2", true)).isEmpty());
        clock.set(Long.MAX_VALUE - 3);
        assertTrue(book.claim(identity("eos-0-3", true)).isEmpty());
    }

    @Test
    void concurrentConnectionsCannotExceedTheOccurrenceCount() throws Exception {
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L, () -> 0L);
        writeRule("concurrent", """
                {"faultId":"concurrent","api":"end-txn","action":"drop-response",
                 "occurrences":3}
                """);
        book.refresh();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Optional<FaultRuleBook.Claim>>> attempts = new ArrayList<>();
        try (var executor = Executors.newFixedThreadPool(8)) {
            for (int index = 0; index < 8; index++) {
                attempts.add(executor.submit(() -> {
                    start.await();
                    return book.claim(identity("eos-0-1", true));
                }));
            }
            start.countDown();
            List<Integer> completed = new ArrayList<>();
            for (Future<Optional<FaultRuleBook.Claim>> attempt : attempts) {
                attempt.get().ifPresent(claim -> completed.add(book.complete(claim)));
            }
            assertEquals(List.of(1, 2, 3), completed);
            assertTrue(book.claim(identity("eos-0-1", true)).isEmpty());
        }
    }

    @Test
    void reportsAnInvalidRuleOnceAndNeverArmsIt() throws IOException {
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L);
        writeRule("f2", """
                {"faultId": "f2", "api": "produce", "action": "drop-request", "occurrences": 1}
                """);
        book.refresh();
        book.refresh();

        assertTrue(book.claim(identity("eos-0-1", true)).isEmpty());
        List<JsonNode> events = events("f2");
        assertEquals(1, events.size());
        assertEquals("rejected", events.getFirst().get("event").textValue());
        assertTrue(events.getFirst().get("reason").textValue().contains("produce"));
    }

    @Test
    void concurrentRetriesProduceOnlyOneWitnessPerCompletedDrop() throws Exception {
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L, () -> 0L);
        writeRule("retry", """
                {"faultId":"retry","api":"end-txn","action":"drop-request","occurrences":2}
                """);
        book.refresh();
        FaultRuleBook.EndTxnIdentity request = identity("eos-0-1", true);
        for (int index = 0; index < 2; index++) {
            FaultRuleBook.Claim claim = book.claim(request).orElseThrow();
            book.record(claim, "request-dropped", Map.of("occurrence", book.complete(claim)));
        }
        Files.delete(control.resolve("rules/retry.json"));
        book.refresh();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> observations = new ArrayList<>();
        try (var executor = Executors.newFixedThreadPool(8)) {
            for (int index = 0; index < 8; index++) {
                int correlationId = index;
                observations.add(executor.submit(() -> {
                    start.await();
                    book.observeRetry(request, Map.of("correlationId", correlationId));
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> observation : observations) {
                observation.get();
            }
        }
        List<JsonNode> retries = events("retry").stream()
                .filter(event -> "retry-observed".equals(event.path("event").textValue())).toList();
        assertEquals(List.of(1, 2), retries.stream().map(event -> event.path("claim").intValue()).toList());
        assertEquals(retries.getFirst().path("correlationId"), retries.getLast().path("correlationId"),
                "the first observed matching request witnesses both preceding drops");
    }

    @Test
    void parsesRulesStrictly() throws IOException {
        FaultRule rule = FaultRule.parse(JSON.readTree("""
                {"faultId": "f3", "api": "end-txn", "action": "drop-request", "occurrences": 2,
                 "triggerDeadlineNanos": 1000}
                """));
        assertEquals(FaultRule.Result.ANY, rule.result());
        assertTrue(rule.matches("anything", false));

        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(JSON.readTree("""
                {"faultId": "f3", "api": "end-txn", "action": "drop-request",
                 "occurrences": 1, "triggerDeadlineNanos": 1000, "probability": 0.5}
                """)));
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(JSON.readTree("""
                {"faultId": "f3", "api": "end-txn", "action": "drop-request", "occurrences": 0,
                 "triggerDeadlineNanos": 1000}
                """)));
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(JSON.readTree("""
                {"faultId":"f3","api":"end-txn","action":"drop-request","occurrences":1}
                """)), "an old filter protocol cannot silently omit the deadline");
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(JSON.readTree("""
                {"faultId":"f3","api":"end-txn","action":"drop-request","occurrences":1,
                 "triggerDeadlineNanos":0}
                """)));
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(JSON.readTree("""
                {"faultId":"f3","api":"end-txn","action":"drop-request","occurrences":1.5,
                 "triggerDeadlineNanos":1000}
                """)));
    }

    private static FaultRuleBook.EndTxnIdentity identity(String transactionalId, boolean committed) {
        return new FaultRuleBook.EndTxnIdentity(transactionalId, 1000L, (short) 3, committed);
    }

    private void writeRule(String faultId, String json) throws IOException {
        Files.createDirectories(control.resolve("rules"));
        ObjectNode rule = (ObjectNode) JSON.readTree(json);
        if (!rule.has("triggerDeadlineNanos")) {
            rule.put("triggerDeadlineNanos", 10_000_000_000L);
        }
        Files.writeString(control.resolve("rules/" + faultId + ".json"), JSON.writeValueAsString(rule));
    }

    private List<JsonNode> events(String faultId) throws IOException {
        return Files.readAllLines(control.resolve("events/" + faultId + ".jsonl")).stream()
                .map(line -> {
                    try {
                        return JSON.readTree(line);
                    } catch (IOException invalid) {
                        throw new AssertionError(invalid);
                    }
                })
                .toList();
    }
}
