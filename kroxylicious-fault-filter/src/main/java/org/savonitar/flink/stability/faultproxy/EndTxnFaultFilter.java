package org.savonitar.flink.stability.faultproxy;

import io.kroxylicious.proxy.filter.EndTxnRequestFilter;
import io.kroxylicious.proxy.filter.EndTxnResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Drops EndTxn requests or responses that an armed rule claims. Kroxylicious creates one instance
 * per client connection and calls it on that connection's thread, so its own state needs no locks.
 *
 * <p>A response is dropped only if the broker succeeded: only then has the broker acted on the
 * request while the client believes it pending (SPEC-004 K5.1a). An error response goes to the
 * client, and its occurrence is released for the next matching request.
 */
final class EndTxnFaultFilter implements EndTxnRequestFilter, EndTxnResponseFilter {
    private final FaultRuleBook book;
    /** Claims whose request went to the broker, by correlation ID. */
    private final Map<Integer, FaultRuleBook.Claim> responsesToDrop = new HashMap<>();

    EndTxnFaultFilter(FaultRuleBook book) {
        this.book = Objects.requireNonNull(book, "book");
    }

    @Override
    public CompletionStage<RequestFilterResult> onEndTxnRequest(
            short apiVersion,
            RequestHeaderData header,
            EndTxnRequestData request,
            FilterContext context) {
        FaultRuleBook.EndTxnIdentity identity = new FaultRuleBook.EndTxnIdentity(
                request.transactionalId(), request.producerId(), request.producerEpoch(),
                request.committed());
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("apiVersion", apiVersion);
        details.put("correlationId", header.correlationId());
        details.put("clientId", header.clientId());
        details.put("transactionalId", request.transactionalId());
        details.put("producerId", request.producerId());
        details.put("producerEpoch", request.producerEpoch());
        details.put("committed", request.committed());
        details.put("channel", context.channelDescriptor());
        book.observeRetry(identity, details);
        Optional<FaultRuleBook.Claim> claim = book.claim(identity);
        if (claim.isEmpty()) {
            return context.forwardRequest(header, request);
        }
        if (claim.get().rule().action() == FaultRule.Action.DROP_REQUEST) {
            details.put("occurrence", book.complete(claim.get()));
            book.record(claim.get(), "request-dropped", details);
            return context.requestFilterResultBuilder().drop().completed();
        }
        responsesToDrop.put(header.correlationId(), claim.get());
        book.record(claim.get(), "request-forwarded", details);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onEndTxnResponse(
            short apiVersion,
            ResponseHeaderData header,
            EndTxnResponseData response,
            FilterContext context) {
        FaultRuleBook.Claim claim = responsesToDrop.remove(header.correlationId());
        if (claim == null) {
            return context.forwardResponse(header, response);
        }
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("apiVersion", apiVersion);
        details.put("correlationId", header.correlationId());
        details.put("error", Errors.forCode(response.errorCode()).name());
        details.put("producerId", response.producerId());
        details.put("producerEpoch", response.producerEpoch());
        details.put("channel", context.channelDescriptor());
        if (response.errorCode() != Errors.NONE.code()) {
            book.release(claim);
            book.record(claim, "response-forwarded", details);
            return context.forwardResponse(header, response);
        }
        details.put("occurrence", book.complete(claim));
        book.record(claim, "response-dropped", details);
        return context.responseFilterResultBuilder().drop().completed();
    }
}
