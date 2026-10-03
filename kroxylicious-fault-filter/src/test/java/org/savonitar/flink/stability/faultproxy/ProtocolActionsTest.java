package org.savonitar.flink.stability.faultproxy;

import com.fasterxml.jackson.databind.*;
import io.kroxylicious.proxy.filter.*;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.errors.ApiException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.KafkaProtocolFaultPolicy;

import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolActionsTest {
    @TempDir Path control;
    final ObjectMapper json = new ObjectMapper();
    final AtomicInteger forwarded = new AtomicInteger();
    final AtomicReference<ApiMessage> synthetic = new AtomicReference<>();

    @Test void allApisDropOnlyTwoMatchingMessagesAcrossConnections() throws Exception {
        for (var fixture : ProtocolMessagesTest.fixtures()) {
            try (var book = arm(fixture, "drop-request", null, 0, System::nanoTime, Long.MAX_VALUE)) {
                var first = new KafkaProtocolFaultFilter(book);
                var second = new KafkaProtocolFaultFilter(book);
                for (int i = 1; i <= 3; i++) assertEquals(i <= 2, (i == 1 ? first : second).onRequest(fixture.api(), fixture.version(), header(fixture, i), ProtocolMessagesTest.wire(fixture), context()).toCompletableFuture().join().drop(), fixture.toString());
                assertEquals(2, events().stream().filter(e -> e.path("event").asText().equals("request-dropped")).count());
            }
            cleanRule();
        }
    }

    @Test void permittedErrorsAreKafkaSerializableAndNeverReachTheBroker() throws Exception {
        for (var fixture : ProtocolMessagesTest.fixtures()) {
            String api = fixture.api().name().toLowerCase(Locale.ROOT).replace('_', '-');
            for (String error : KafkaProtocolFaultPolicy.errors(api)) {
                forwarded.set(0); synthetic.set(null);
                try (var book = arm(fixture, "error-response", error, 0, System::nanoTime, Long.MAX_VALUE)) {
                    var filter = new KafkaProtocolFaultFilter(book);
                    filter.onRequest(fixture.api(), fixture.version(), header(fixture, 1), ProtocolMessagesTest.wire(fixture), context()).toCompletableFuture().join();
                    if (fixture.api() == ApiKeys.TXN_OFFSET_COMMIT && fixture.version() < 2 && error.equals("coordinator-load-in-progress")) {
                        assertEquals(1, forwarded.get()); assertNull(synthetic.get());
                    } else {
                        assertEquals(0, forwarded.get());
                        assertNotNull(synthetic.get(), fixture + " " + error);
                        short code = Errors.valueOf(error.toUpperCase(Locale.ROOT).replace('-', '_')).code();
                        assertEquals(Set.of(code), Set.copyOf(ProtocolMessages.errors(fixture.version(), synthetic.get()).values()));
                        JsonNode event = events().getLast();
                        assertTrue(event.path("originalErrorCode").isNull());
                        assertEquals(code, event.path("substitutedErrorCode").shortValue());
                        assertEquals("synthetic-before-broker", event.path("errorOrigin").asText());
                    }
                }
                cleanRule();
            }
        }
    }

    @Test void delayIsAsynchronousAndLateCompletionCannotSatisfyDeadline() throws Exception {
        var fixture = ProtocolMessagesTest.fixtures().getFirst();
        AtomicLong clock = new AtomicLong();
        try (var book = arm(fixture, "delay", null, 100, clock::get, 50_000_000)) {
            var stage = new KafkaProtocolFaultFilter(book).onRequest(fixture.api(), fixture.version(), header(fixture, 1), fixture.request(), context());
            assertFalse(stage.toCompletableFuture().isDone());
            clock.set(150_000_000);
            assertFalse(stage.toCompletableFuture().get(2, TimeUnit.SECONDS).drop());
            JsonNode event = events().getLast();
            assertEquals("request-delayed", event.path("event").asText());
            assertFalse(event.path("beforeDeadline").booleanValue());
            assertEquals(150_000_000, event.path("actualDelayNanos").longValue());
        }
    }

    @Test void closingAFilterCancelsPendingDelayInsteadOfForwardingIt() throws Exception {
        var fixture = ProtocolMessagesTest.fixtures().getFirst();
        var book = arm(fixture, "delay", null, 5000, System::nanoTime, Long.MAX_VALUE);
        var stage = new KafkaProtocolFaultFilter(book).onRequest(fixture.api(), fixture.version(), header(fixture, 1), fixture.request(), context());
        book.close();
        assertTrue(stage.toCompletableFuture().isCompletedExceptionally());
        assertEquals(0, events().stream().filter(e -> e.path("event").asText().equals("request-delayed")).count());
    }

    @Test void rejectsFencingWrongApiErrorsAndUnboundedDelays() throws Exception {
        var rule = json.createObjectNode().put("faultId", "f").put("api", "end-txn").put("action", "error-response").put("error", "producer-fenced").put("occurrences", 1).put("triggerDeadlineNanos", 1000);
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(rule));
        rule.put("error", "not-enough-replicas");
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(rule));
        rule.remove("error"); rule.put("action", "delay").put("latencyMillis", 5001);
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(rule));
    }

    private FaultRuleBook arm(ProtocolMessagesTest.Fixture fixture, String action, String error, long delay, java.util.function.LongSupplier clock, long deadline) throws Exception {
        Files.createDirectories(control.resolve("rules"));
        var rule = json.createObjectNode().put("faultId", "f").put("api", fixture.api().name().toLowerCase(Locale.ROOT).replace('_', '-')).put("action", action)
            .put("transactionalIdPrefix", "eos-").put("occurrences", 2).put("triggerDeadlineNanos", deadline);
        if (error != null) rule.put("error", error);
        if (delay > 0) rule.put("latencyMillis", delay);
        Files.writeString(control.resolve("rules/f.json"), rule.toString());
        var book = new FaultRuleBook(control, () -> 0L, clock); book.refresh(); return book;
    }
    private void cleanRule() throws Exception { Files.delete(control.resolve("rules/f.json")); Files.delete(control.resolve("events/f.jsonl")); }
    private List<JsonNode> events() throws Exception {
        List<JsonNode> events = new ArrayList<>();
        for (String line : Files.readAllLines(control.resolve("events/f.jsonl"))) events.add(json.readTree(line));
        return events;
    }
    private RequestHeaderData header(ProtocolMessagesTest.Fixture fixture, int correlation) {
        return new RequestHeaderData().setRequestApiKey(fixture.api().id).setRequestApiVersion(fixture.version()).setCorrelationId(correlation).setClientId("test");
    }
    private FilterContext context() {
        return fake(FilterContext.class, (name, args) -> switch (name) {
            case "channelDescriptor" -> "test";
            case "forwardRequest" -> { forwarded.incrementAndGet(); yield CompletableFuture.completedFuture(result(false)); }
            case "requestFilterResultBuilder" -> fake(RequestFilterResultBuilder.class, (method, values) -> {
                if (method.equals("errorResponse")) {
                    var header = (RequestHeaderData) values[0]; var request = (ApiMessage) values[1];
                    var parsed = AbstractRequest.parseRequest(ApiKeys.forId(header.requestApiKey()), header.requestApiVersion(), new ByteBufferAccessor(ProtocolMessagesTest.bytes(request, header.requestApiVersion()))).request;
                    var response = parsed.getErrorResponse((ApiException) values[2]).data();
                    var bytes = ProtocolMessagesTest.bytes(response, header.requestApiVersion());
                    var decoded = ApiMessageType.fromApiKey(header.requestApiKey()).newResponse();
                    decoded.read(new ByteBufferAccessor(bytes), header.requestApiVersion()); synthetic.set(decoded);
                } else if (!method.equals("drop")) throw new UnsupportedOperationException(method);
                return fake(io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage.class, (terminal, unused) -> CompletableFuture.completedFuture(result(method.equals("drop"))));
            });
            default -> throw new UnsupportedOperationException(name);
        });
    }
    private static RequestFilterResult result(boolean dropped) { return fake(RequestFilterResult.class, (name, args) -> name.equals("drop") ? dropped : false); }
    @SuppressWarnings("unchecked") private static <T> T fake(Class<T> type, Answer answer) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> answer.call(method.getName(), args));
    }
    interface Answer { Object call(String name, Object[] args) throws Exception; }
}
