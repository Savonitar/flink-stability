package org.savonitar.flink.stability.faultproxy;

import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ProduceResponseFaultTest {
    @TempDir Path control;

    @Test void forwardsAppendThenSubstitutesOnlyTwoSuccessfulResponsesAcrossConnections() throws Exception {
        for (short version : new short[]{3, 8, 12}) {
            var helper = helper("v" + version);
            var fixture = new ProtocolMessagesTest.Fixture(ApiKeys.PRODUCE, version, ProtocolMessagesTest.produce());
            try (var book = helper.arm(fixture, "error-after-append", "request-timed-out", 0, System::nanoTime, Long.MAX_VALUE)) {
                var first = new KafkaProtocolFaultFilter(book);
                var second = new KafkaProtocolFaultFilter(book);
                for (int index = 1; index <= 3; index++) {
                    var filter = index == 1 ? first : second;
                    assertFalse(filter.onRequest(ApiKeys.PRODUCE, version, header(version, index),
                            ProtocolMessagesTest.wire(fixture), helper.context()).toCompletableFuture().join().drop());
                    assertNull(helper.synthetic.get(), "the broker must see the request first");
                    var original = wire(response(), version);
                    filter.onResponse(ApiKeys.PRODUCE, version, new ResponseHeaderData().setCorrelationId(index),
                            original, helper.context()).toCompletableFuture().join();
                    var forwarded = wire((ProduceResponseData) helper.responseForwarded.get(), version);
                    assertEquals(index <= 2 ? Errors.REQUEST_TIMED_OUT.code() : 0, partition(forwarded).errorCode());
                    assertEquals(index <= 2 ? -1 : 123L, partition(forwarded).baseOffset());
                    assertEquals(0, partition(original).errorCode(), "original broker answer is preserved");
                    assertEquals(123L, partition(original).baseOffset());
                }
                var affected = helper.events().stream().filter(e -> e.path("event").asText().equals("response-error-after-append")).toList();
                assertEquals(2, affected.size());
                assertEquals(3, helper.forwarded.get());
                for (var event : affected) {
                    assertEquals("eos-1", event.path("transactionalId").asText());
                    assertEquals(42, event.path("producerId").asLong());
                    assertEquals(2, event.path("producerEpoch").asInt());
                    assertEquals(0, event.at("/originalErrorCodes/output~10").asInt());
                    assertEquals(123L, event.at("/originalBaseOffsets/output~10").asLong());
                    assertEquals(7, event.path("substitutedErrorCode").asInt());
                    assertEquals("synthetic-after-append", event.path("errorOrigin").asText());
                    assertTrue(event.path("forwardedToBroker").asBoolean());
                    assertTrue(event.path("beforeDeadline").asBoolean());
                }
            }
        }
    }

    @Test void incompleteMixedForeignOrDuplicatePartitionRepliesDoNotConsumeAnOccurrence() throws Exception {
        List<ProduceResponseData> invalid = new ArrayList<>();
        invalid.add(response()); partition(invalid.getLast()).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code());
        invalid.add(response()); partition(invalid.getLast()).setBaseOffset(-1);
        invalid.add(response()); partition(invalid.getLast()).setIndex(1);
        invalid.add(new ProduceResponseData());
        invalid.add(response()); invalid.getLast().responses().iterator().next().setPartitionResponses(List.of(
                partition(response()), partition(response()).setIndex(1).setErrorCode(Errors.REQUEST_TIMED_OUT.code())));
        invalid.add(response()); invalid.getLast().responses().iterator().next().setPartitionResponses(List.of(partition(response()), partition(response())));
        var helper = helper("invalid");
        var fixture = new ProtocolMessagesTest.Fixture(ApiKeys.PRODUCE, (short) 12, ProtocolMessagesTest.produce());
        try (var book = helper.arm(fixture, "error-after-append", "request-timed-out", 0, System::nanoTime, Long.MAX_VALUE)) {
            var filter = new KafkaProtocolFaultFilter(book);
            int id = 0;
            for (var original : invalid) {
                filter.onRequest(ApiKeys.PRODUCE, (short) 12, header((short) 12, ++id), fixture.request(), helper.context());
                filter.onResponse(ApiKeys.PRODUCE, (short) 12, new ResponseHeaderData().setCorrelationId(id), original, helper.context());
                assertSame(original, helper.responseForwarded.get());
            }
            assertEquals(0, helper.events().stream().filter(e -> e.path("event").asText().equals("response-error-after-append")).count());
            filter.onRequest(ApiKeys.PRODUCE, (short) 12, header((short) 12, ++id), fixture.request(), helper.context());
            filter.onResponse(ApiKeys.PRODUCE, (short) 12, new ResponseHeaderData().setCorrelationId(id), response(), helper.context());
            assertEquals(7, partition((ProduceResponseData) helper.responseForwarded.get()).errorCode());
            assertEquals(1, helper.events().getLast().path("occurrence").asInt());
        }
    }

    @Test void allSuccessfulPartitionsAreBoundToTheOriginalRequest() {
        var request = ProtocolMessagesTest.produce();
        var first = request.topicData().iterator().next().partitionData().getFirst();
        request.topicData().iterator().next().partitionData().add(first.duplicate().setIndex(1));
        var broker = response();
        broker.responses().iterator().next().setPartitionResponses(List.of(
                partition(response()), partition(response()).setIndex(1).setBaseOffset(456L)));
        assertEquals(Map.of("output/0", 123L, "output/1", 456L),
                ProduceResponseFault.appendOffsets(broker, ProduceResponseFault.partitions(request)));
        var client = wire(ProduceResponseFault.timeout(broker), (short) 12);
        assertEquals(Set.of((short) 7), Set.copyOf(ProtocolMessages.errors((short) 12, client).values()));
        assertEquals(Set.of((short) 0), Set.copyOf(ProtocolMessages.errors((short) 12, broker).values()));
        assertTrue(ProduceResponseFault.appendOffsets(broker, Set.of("output/0")).isEmpty());
    }

    @Test void prefixAndAcksAreRequiredAndLateResponseIsNotProof() throws Exception {
        var helper = helper("deadline");
        var fixture = new ProtocolMessagesTest.Fixture(ApiKeys.PRODUCE, (short) 12, ProtocolMessagesTest.produce());
        AtomicLong clock = new AtomicLong();
        try (var book = helper.arm(fixture, "error-after-append", "request-timed-out", 0, clock::get, 100)) {
            var filter = new KafkaProtocolFaultFilter(book);
            List<ProduceRequestData> excluded = List.of(ProtocolMessagesTest.produce().setAcks((short) 1),
                    ProtocolMessagesTest.produce().setTransactionalId("foreign"));
            for (var request : excluded) {
                filter.onRequest(ApiKeys.PRODUCE, (short) 12, header((short) 12, 1), request, helper.context());
                var original = response();
                filter.onResponse(ApiKeys.PRODUCE, (short) 12, new ResponseHeaderData().setCorrelationId(1), original, helper.context());
                assertSame(original, helper.responseForwarded.get());
            }
            filter.onRequest(ApiKeys.PRODUCE, (short) 12, header((short) 12, 2), fixture.request(), helper.context());
            clock.set(101);
            filter.onResponse(ApiKeys.PRODUCE, (short) 12, new ResponseHeaderData().setCorrelationId(2), response(), helper.context());
            assertEquals(1, helper.events().getLast().path("occurrence").asInt());
            assertFalse(helper.events().getLast().path("beforeDeadline").asBoolean());
        }
    }

    @Test void onlyProduceTimeoutCanBeConfiguredAfterAppend() throws Exception {
        var helper = helper("policy");
        var rule = helper.json.createObjectNode().put("faultId", "f").put("api", "produce")
                .put("action", "error-after-append").put("error", "request-timed-out")
                .put("occurrences", 1).put("triggerDeadlineNanos", 1000);
        assertDoesNotThrow(() -> FaultRule.parse(rule));
        for (String error : List.of("producer-fenced", "not-enough-replicas", "not-enough-replicas-after-append")) {
            rule.put("error", error);
            assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(rule));
        }
        rule.put("error", "request-timed-out").put("api", "end-txn");
        assertThrows(IllegalArgumentException.class, () -> FaultRule.parse(rule));
    }

    private ProtocolActionsTest helper(String name) {
        var helper = new ProtocolActionsTest(); helper.control = control.resolve(name); return helper;
    }
    private static RequestHeaderData header(short version, int id) {
        return new RequestHeaderData().setRequestApiKey(ApiKeys.PRODUCE.id).setRequestApiVersion(version).setCorrelationId(id);
    }
    private static ProduceResponseData response() {
        var topic = new ProduceResponseData.TopicProduceResponse().setName("output").setPartitionResponses(List.of(
                new ProduceResponseData.PartitionProduceResponse().setIndex(0).setErrorCode((short) 0).setBaseOffset(123L)));
        return new ProduceResponseData().setResponses(new ProduceResponseData.TopicProduceResponseCollection(List.of(topic).iterator()));
    }
    private static ProduceResponseData.PartitionProduceResponse partition(ProduceResponseData response) {
        return response.responses().iterator().next().partitionResponses().getFirst();
    }
    private static ProduceResponseData wire(ProduceResponseData response, short version) {
        return new ProduceResponseData(new ByteBufferAccessor(ProtocolMessagesTest.bytes(response, version)), version);
    }
}
