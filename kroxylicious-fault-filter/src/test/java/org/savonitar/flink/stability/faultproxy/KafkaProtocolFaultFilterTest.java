package org.savonitar.flink.stability.faultproxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaProtocolFaultFilterTest {
    private static final short VERSION = 5;
    private static final FilterContext CONTEXT = fakeContext();

    @TempDir
    Path control;

    @Test
    void dropsTheFirstMatchingCommitRequestAndForwardsItsRetry() throws IOException {
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book("drop-request"));

        assertTrue(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(11), commit("eos-0-1"), CONTEXT)));
        assertFalse(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(12), commit("eos-0-1"), CONTEXT)));

        List<String> events = Files.readAllLines(control.resolve("events/f1.jsonl"));
        assertEquals(2, events.size());
        assertTrue(events.get(1).contains("\"event\":\"request-dropped\""), events.get(1));
        assertTrue(events.get(1).contains("\"occurrence\":1"), events.get(1));
        assertTrue(events.get(1).contains("\"transactionalId\":\"eos-0-1\""), events.get(1));
        assertTrue(events.get(1).contains("\"producerEpoch\":3"), events.get(1));
        assertTrue(events.get(1).contains("\"committed\":true"), events.get(1));
    }

    @Test
    void forwardsAMatchingRequestAndDropsOnlyItsResponse() throws IOException {
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book("drop-response"));

        assertFalse(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(21), commit("eos-0-1"), CONTEXT)));
        assertFalse(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, new ResponseHeaderData().setCorrelationId(20),
                new EndTxnResponseData(), CONTEXT)));
        assertTrue(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, new ResponseHeaderData().setCorrelationId(21),
                new EndTxnResponseData().setProducerEpoch((short) 4), CONTEXT)));

        List<String> events = Files.readAllLines(control.resolve("events/f1.jsonl"));
        assertEquals(3, events.size());
        assertTrue(events.get(1).contains("\"event\":\"request-forwarded\""), events.get(1));
        assertTrue(events.get(2).contains("\"event\":\"response-dropped\""), events.get(2));
        assertTrue(events.get(2).contains("\"claim\":1"), events.get(2));
        assertTrue(events.get(2).contains("\"occurrence\":1"), events.get(2));
        assertTrue(events.get(2).contains("\"error\":\"NONE\""), events.get(2));
        assertTrue(events.get(2).contains("\"producerEpoch\":4"), events.get(2));
    }

    @Test
    void passesAnErrorResponseToTheClientWithoutUsingTheOccurrence() throws IOException {
        FaultRuleBook book = book("drop-response");
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);

        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(41), commit("eos-0-1"), CONTEXT);
        assertFalse(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, new ResponseHeaderData().setCorrelationId(41),
                new EndTxnResponseData().setErrorCode(Errors.NOT_COORDINATOR.code()),
                CONTEXT)), "an error response proves no commit, so the client must see it");
        // The client's retry is claimed again, and its successful response is the occurrence.
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(42), commit("eos-0-1"), CONTEXT);
        assertTrue(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, new ResponseHeaderData().setCorrelationId(42),
                new EndTxnResponseData(), CONTEXT)));

        List<String> events = Files.readAllLines(control.resolve("events/f1.jsonl"));
        assertTrue(events.get(2).contains("\"event\":\"response-forwarded\""), events.get(2));
        assertTrue(events.get(2).contains("\"error\":\"NOT_COORDINATOR\""), events.get(2));
        assertTrue(events.get(4).contains("\"event\":\"response-dropped\""), events.get(4));
        assertTrue(events.get(4).contains("\"claim\":2"), events.get(4));
        assertTrue(events.get(4).contains("\"occurrence\":1"), events.get(4));
        heal(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(43), commit("eos-0-1"), CONTEXT);
        assertEquals(1, retries().size(), "a released error-response claim has no retry witness");
        assertEquals(2, retries().getFirst().path("claim").intValue());
    }

    @Test
    void aResponseClaimedBeforeHealIsStillDroppedAfterHeal() throws IOException {
        FaultRuleBook book = book("drop-response");
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(51), commit("eos-0-1"), CONTEXT);
        Files.delete(control.resolve("rules/f1.json"));
        book.refresh();

        assertTrue(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(51), new EndTxnResponseData(), CONTEXT)));
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(52), commit("eos-0-2"), CONTEXT);
        assertFalse(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(52), new EndTxnResponseData(), CONTEXT)));

        List<String> events = Files.readAllLines(control.resolve("events/f1.jsonl"));
        assertTrue(events.get(2).contains("\"event\":\"healed\""), events.get(2));
        assertTrue(events.get(2).contains("\"pending\":1"), events.get(2));
        assertTrue(events.get(3).contains("\"event\":\"response-dropped\""), events.get(3));
    }

    @Test
    void aPendingResponseAfterTheDeadlineIsDroppedButCannotProveTheTrigger() throws IOException {
        AtomicLong clock = new AtomicLong(100);
        FaultRuleBook book = book("drop-response", clock, 10);
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(61), commit("eos-0-1"), CONTEXT);
        clock.set(110);

        assertTrue(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(61), new EndTxnResponseData(), CONTEXT)));

        List<String> events = Files.readAllLines(control.resolve("events/f1.jsonl"));
        assertTrue(events.getLast().contains("\"beforeDeadline\":false"), events.getLast());
    }

    @Test
    void theSameCorrelationIdOnAnotherConnectionCannotConsumeAClaimedResponse() throws IOException {
        FaultRuleBook book = book("drop-response");
        KafkaProtocolFaultFilter first = new KafkaProtocolFaultFilter(book);
        KafkaProtocolFaultFilter second = new KafkaProtocolFaultFilter(book);
        first.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(71), commit("eos-0-1"), CONTEXT);
        second.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(71), commit("eos-0-2"), CONTEXT);

        assertFalse(dropped(second.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(71), new EndTxnResponseData(), CONTEXT)));
        assertTrue(dropped(first.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(71), new EndTxnResponseData(), CONTEXT)));
        assertFalse(dropped(first.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(71), new EndTxnResponseData(), CONTEXT)),
                "a completed claim cannot be applied twice");
    }

    @Test
    void forwardsEverythingWhileNoRuleIsArmed() throws IOException {
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L);
        book.refresh();
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);

        assertFalse(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(31), commit("eos-0-1"), CONTEXT)));
        assertFalse(Files.exists(control.resolve("events")));
    }

    @Test
    void observesTheOriginalRequestIdentityOnceAcrossConnectionsAfterResponseLossAndHeal()
            throws IOException {
        FaultRuleBook book = book("drop-response");
        KafkaProtocolFaultFilter original = new KafkaProtocolFaultFilter(book);
        original.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(81), commit("eos-0-1"), CONTEXT);
        assertTrue(dropped(original.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(81),
                new EndTxnResponseData().setProducerEpoch((short) 4), CONTEXT)));
        heal(book);

        KafkaProtocolFaultFilter reconnected = new KafkaProtocolFaultFilter(book);
        FilterContext newContext = fakeContext("client-2");
        // A later epoch (including the broker's unseen answer) is not this exact retry.
        for (EndTxnRequestData unrelated : List.of(
                commit("eos-0-2"), commit("eos-0-1").setProducerId(1001L),
                commit("eos-0-1").setProducerEpoch((short) 4),
                commit("eos-0-1").setCommitted(false))) {
            assertFalse(dropped(reconnected.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                    VERSION, requestHeader(82), unrelated, newContext)));
        }
        assertTrue(retries().isEmpty());

        assertFalse(dropped(reconnected.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                (short) 4, requestHeader(83).setClientId(null), commit("eos-0-1"), newContext)));
        reconnected.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(84), commit("eos-0-1"), newContext);

        assertEquals(1, retries().size(), "each dropped claim gets at most one witness");
        JsonNode retry = retries().getFirst();
        assertEquals("f1", retry.path("faultId").textValue());
        assertEquals(1, retry.path("claim").intValue());
        assertEquals("drop-response", retry.path("action").textValue());
        assertEquals("eos-0-1", retry.path("transactionalId").textValue());
        assertEquals(1000L, retry.path("producerId").longValue());
        assertEquals(3, retry.path("producerEpoch").intValue());
        assertTrue(retry.path("committed").booleanValue());
        assertEquals(4, retry.path("apiVersion").intValue());
        assertEquals(83, retry.path("correlationId").intValue());
        assertEquals("client-2", retry.path("channel").textValue());
        assertTrue(retry.path("clientId").isNull(), "nullable metadata does not prevent observation");
        assertFalse(retry.has("beforeDeadline"), "observations do not confirm new drops");
        assertFalse(retry.has("occurrence"));
    }

    @Test
    void aMatchingRequestBeforeHealDoesNotConsumeThePostHealWitness() throws IOException {
        FaultRuleBook book = book("drop-request");
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);
        assertTrue(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(91), commit("eos-0-1"), CONTEXT)));
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(92), commit("eos-0-1"), CONTEXT);
        assertTrue(retries().isEmpty());
        heal(book);

        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(93), commit("eos-0-1"), CONTEXT);

        assertEquals(1, retries().size());
        assertEquals(93, retries().getFirst().path("correlationId").intValue());
        assertEquals("drop-request", retries().getFirst().path("action").textValue());
    }

    @Test
    void aRequestAfterHealIsNotARetryWitnessUntilThePendingResponseWasDropped() throws IOException {
        FaultRuleBook book = book("drop-response");
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(101), commit("eos-0-1"), CONTEXT);
        heal(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(102), commit("eos-0-1"), CONTEXT);
        assertTrue(retries().isEmpty());
        assertTrue(dropped(filter.onResponse(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION,
                new ResponseHeaderData().setCorrelationId(101), new EndTxnResponseData(), CONTEXT)));

        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(103), commit("eos-0-1"), CONTEXT);

        assertEquals(1, retries().size());
        assertEquals(103, retries().getFirst().path("correlationId").intValue());
    }

    @Test
    void observingARetryDoesNotStopAnotherFaultFromDroppingIt() throws IOException {
        FaultRuleBook book = book("drop-request");
        String rule = Files.readString(control.resolve("rules/f1.json"));
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(111), commit("eos-0-1"), CONTEXT);
        heal(book);
        Files.writeString(control.resolve("rules/f2.json"), rule.replace("\"f1\"", "\"f2\""));
        book.refresh();

        assertTrue(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(112), commit("eos-0-1"), CONTEXT)));

        assertEquals(1, retries().size());
        assertEquals(112, retries().getFirst().path("correlationId").intValue());
        assertTrue(Files.readString(control.resolve("events/f2.jsonl"))
                .contains("\"event\":\"request-dropped\""));
    }

    @Test
    void closingTheBookForgetsUnobservedRetries() throws IOException {
        FaultRuleBook book = book("drop-request");
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(121), commit("eos-0-1"), CONTEXT);
        heal(book);
        book.close();

        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(122), commit("eos-0-1"), CONTEXT);

        assertTrue(retries().isEmpty());
    }

    @Test
    void aFailedRetryEventWriteDoesNotPreventForwardingOrConsumeTheWitness() throws IOException {
        FaultRuleBook book = book("drop-request");
        KafkaProtocolFaultFilter filter = new KafkaProtocolFaultFilter(book);
        filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,VERSION, requestHeader(131), commit("eos-0-1"), CONTEXT);
        heal(book);
        Path eventFile = control.resolve("events/f1.jsonl");
        String previousEvents = Files.readString(eventFile);
        Files.delete(eventFile);
        Files.createDirectory(eventFile); // deterministic append failure, even with elevated privileges

        assertFalse(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(132), commit("eos-0-1"), CONTEXT)));

        Files.delete(eventFile);
        Files.writeString(eventFile, previousEvents);
        assertFalse(dropped(filter.onRequest(org.apache.kafka.common.protocol.ApiKeys.END_TXN,
                VERSION, requestHeader(133), commit("eos-0-1"), CONTEXT)));
        assertEquals(1, retries().size());
        assertEquals(133, retries().getFirst().path("correlationId").intValue());
    }

    private void heal(FaultRuleBook book) throws IOException {
        Files.delete(control.resolve("rules/f1.json"));
        book.refresh();
    }

    private List<JsonNode> retries() throws IOException {
        ObjectMapper json = new ObjectMapper();
        return Files.readAllLines(control.resolve("events/f1.jsonl")).stream()
                .map(line -> {
                    try {
                        return json.readTree(line);
                    } catch (IOException invalid) {
                        throw new AssertionError(invalid);
                    }
                })
                .filter(event -> "retry-observed".equals(event.path("event").textValue()))
                .toList();
    }

    private FaultRuleBook book(String action) throws IOException {
        return book(action, new AtomicLong(), 10_000_000_000L);
    }

    private FaultRuleBook book(String action, AtomicLong nanoTime, long deadlineNanos) throws IOException {
        Files.createDirectories(control.resolve("rules"));
        Files.writeString(control.resolve("rules/f1.json"), """
                {"faultId": "f1", "api": "end-txn", "result": "commit",
                 "transactionalIdPrefix": "eos", "action": "%s", "occurrences": 1,
                 "triggerDeadlineNanos": %d}
                """.formatted(action, deadlineNanos));
        FaultRuleBook book = new FaultRuleBook(control, () -> 0L, nanoTime::get);
        book.refresh();
        return book;
    }

    private static RequestHeaderData requestHeader(int correlationId) {
        return new RequestHeaderData().setCorrelationId(correlationId).setClientId("producer-1");
    }

    private static EndTxnRequestData commit(String transactionalId) {
        return new EndTxnRequestData()
                .setTransactionalId(transactionalId)
                .setProducerId(1000L)
                .setProducerEpoch((short) 3)
                .setCommitted(true);
    }

    private static boolean dropped(CompletionStage<? extends FilterResult> result) {
        return result.toCompletableFuture().join().drop();
    }

    /** Kroxylicious supplies the context at runtime; the filter needs only these methods. */
    private static FilterContext fakeContext() {
        return fakeContext("client-1");
    }

    private static FilterContext fakeContext(String channel) {
        return fake(FilterContext.class, (method, args) -> switch (method) {
            case "channelDescriptor" -> channel;
            case "forwardRequest" -> CompletableFuture.completedFuture(
                    result(RequestFilterResult.class, false));
            case "forwardResponse" -> CompletableFuture.completedFuture(
                    result(ResponseFilterResult.class, false));
            case "requestFilterResultBuilder" -> dropBuilder(
                    RequestFilterResultBuilder.class, RequestFilterResult.class);
            case "responseFilterResultBuilder" -> dropBuilder(
                    ResponseFilterResultBuilder.class, ResponseFilterResult.class);
            default -> throw new UnsupportedOperationException(method);
        });
    }

    private static <B> B dropBuilder(Class<B> builder, Class<? extends FilterResult> result) {
        TerminalStage<?> terminal = fake(TerminalStage.class, (method, args) -> switch (method) {
            case "completed" -> CompletableFuture.completedFuture(result(result, true));
            default -> throw new UnsupportedOperationException(method);
        });
        return fake(builder, (method, args) -> switch (method) {
            case "drop" -> terminal;
            default -> throw new UnsupportedOperationException(method);
        });
    }

    private static <R extends FilterResult> R result(Class<R> type, boolean drop) {
        return fake(type, (method, args) -> switch (method) {
            case "drop" -> drop;
            case "closeConnection", "shortCircuitResponse" -> false;
            default -> null;
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T fake(Class<T> type, Answer answer) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> answer.answer(method.getName(), args));
    }

    @FunctionalInterface
    private interface Answer {
        Object answer(String method, Object[] args);
    }
}
