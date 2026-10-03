package org.savonitar.flink.stability.faultproxy;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.savonitar.flink.stability.runtime.api.KafkaProtocolFaultPolicy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Per-connection correlation state; rules/counts/deadlines are shared across connections. */
final class KafkaProtocolFaultFilter implements RequestFilter, ResponseFilter {
    private final FaultRuleBook book;
    private final Map<Integer, Pending> responses = new HashMap<>();
    private record Pending(FaultRuleBook.Claim claim, Map<String, Object> request) {}

    KafkaProtocolFaultFilter(FaultRuleBook book) { this.book = Objects.requireNonNull(book); }
    private static String alias(ApiKeys api) { return api.name().toLowerCase(Locale.ROOT).replace('_', '-'); }
    @Override public boolean shouldHandleRequest(ApiKeys api, short version) { return KafkaProtocolFaultPolicy.APIS.contains(alias(api)); }
    @Override public boolean shouldHandleResponse(ApiKeys api, short version) { return shouldHandleRequest(api, version); }

    @Override public CompletionStage<RequestFilterResult> onRequest(ApiKeys api, short version,
            RequestHeaderData header, ApiMessage request, FilterContext context) {
        var identity = ProtocolMessages.identity(version, request);
        if (identity.isEmpty() || !alias(api).equals(identity.get().api())) return context.forwardRequest(header, request);
        Map<String, Object> details = details(identity.get(), version, header, context);
        book.observeRetry(identity.get(), details);
        var selected = book.claim(identity.get());
        if (selected.isEmpty()) return context.forwardRequest(header, request);
        var claim = selected.get();
        // Kafka maps this error for pre-v2 TxnOffsetCommit clients (KAFKA-7296).
        if (api == ApiKeys.TXN_OFFSET_COMMIT && version < 2
                && claim.rule().error().filter("coordinator-load-in-progress"::equals).isPresent()) {
            book.release(claim);
            return context.forwardRequest(header, request);
        }
        if (request instanceof org.apache.kafka.common.message.ProduceRequestData produce
                && produce.acks() != -1 && claim.rule().error().filter("not-enough-replicas"::equals).isPresent()) {
            book.release(claim);
            return context.forwardRequest(header, request);
        }
        switch (claim.rule().action()) {
            case DROP_REQUEST -> {
                details.put("forwardedToBroker", false);
                details.put("occurrence", book.complete(claim)); book.record(claim, "request-dropped", details);
                return context.requestFilterResultBuilder().drop().completed();
            }
            case DROP_RESPONSE -> {
                responses.put(header.correlationId(), new Pending(claim, details));
                details.put("forwardedToBroker", true); book.record(claim, "request-forwarded", details);
                return context.forwardRequest(header, request);
            }
            case DELAY -> {
                long started = book.nanoTime();
                long millis = claim.rule().latencyMillis();
                // Construct the continuation on the connection thread. The scheduler only completes its future.
                var forwarded = context.forwardRequest(header, request);
                details.put("requestedDelayMillis", millis); details.put("forwardedToBroker", false);
                book.record(claim, "request-held", details);
                return book.delay(millis).thenCompose(ignored -> {
                    details.put("actualDelayNanos", book.nanoTime() - started);
                    details.put("forwardedToBroker", true);
                    details.put("occurrence", book.complete(claim)); book.record(claim, "request-delayed", details);
                    return forwarded;
                });
            }
            case ERROR_RESPONSE -> {
                Errors error = Errors.valueOf(claim.rule().error().orElseThrow().toUpperCase(Locale.ROOT).replace('-', '_'));
                var result = context.requestFilterResultBuilder().errorResponse(header, request,
                        error.exception("Injected transient broker rejection")).completed();
                details.put("forwardedToBroker", false); details.put("originalErrorCode", null);
                details.put("substitutedErrorCode", error.code()); details.put("errorOrigin", "synthetic-before-broker");
                details.put("occurrence", book.complete(claim)); book.record(claim, "response-substituted", details);
                return result;
            }
            default -> throw new IllegalStateException("Unimplemented protocol action");
        }
    }

    @Override public CompletionStage<ResponseFilterResult> onResponse(ApiKeys api, short version,
            ResponseHeaderData header, ApiMessage response, FilterContext context) {
        Pending pending = responses.remove(header.correlationId());
        if (pending == null) return context.forwardResponse(header, response);
        var claim = pending.claim();
        Map<String, Object> details = new LinkedHashMap<>(pending.request());
        Map<String, Short> codes = ProtocolMessages.errors(version, response);
        details.put("originalErrorCodes", codes); details.put("substitutedErrorCode", null);
        details.put("error", codes.isEmpty() ? "UNAVAILABLE" : codes.values().stream().filter(code -> code != 0)
                .map(code -> Errors.forCode(code).name()).sorted().findFirst().orElse("NONE"));
        // Preserve the old EndTxn response fields as well as the request identity in request-forwarded.
        if (response instanceof EndTxnResponseData end) {
            details.put("producerId", end.producerId()); details.put("producerEpoch", end.producerEpoch());
        }
        if (!alias(api).equals(claim.request.api()) || codes.isEmpty() || codes.values().stream().anyMatch(code -> code != 0)) {
            book.release(claim); book.record(claim, "response-forwarded", details);
            return context.forwardResponse(header, response);
        }
        details.put("occurrence", book.complete(claim)); book.record(claim, "response-dropped", details);
        return context.responseFilterResultBuilder().drop().completed();
    }

    private static Map<String, Object> details(FaultRuleBook.RequestIdentity identity, short version,
            RequestHeaderData header, FilterContext context) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("api", identity.api()); fields.put("apiVersion", version);
        fields.put("correlationId", header.correlationId()); fields.put("clientId", header.clientId());
        fields.put("channel", context.channelDescriptor()); fields.put("transactionalId", identity.transactionalId());
        fields.put("producerId", identity.producerId()); fields.put("producerEpoch", identity.producerEpoch());
        fields.put("requestProducerId", identity.producerId()); fields.put("requestProducerEpoch", identity.producerEpoch());
        fields.put("committed", identity.committed()); fields.put("topics", identity.topics().stream().sorted().toList());
        fields.put("originalErrorCodes", Map.of()); fields.put("originalErrorCode", null); fields.put("substitutedErrorCode", null);
        return fields;
    }
}
