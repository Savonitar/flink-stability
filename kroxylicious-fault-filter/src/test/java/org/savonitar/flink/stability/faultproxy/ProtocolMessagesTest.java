package org.savonitar.flink.stability.faultproxy;

import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.compress.Compression;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolMessagesTest {
    record Fixture(ApiKeys api, short version, ApiMessage request) {}

    static List<Fixture> fixtures() {
        var topic = new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic().setName("output").setPartitions(List.of(0));
        var topics = new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection(List.of(topic).iterator());
        return List.of(
            new Fixture(ApiKeys.END_TXN, (short) 3, new EndTxnRequestData().setTransactionalId("eos-1").setProducerId(42).setProducerEpoch((short) 2).setCommitted(true)),
            new Fixture(ApiKeys.END_TXN, (short) 5, new EndTxnRequestData().setTransactionalId("eos-1").setProducerId(42).setProducerEpoch((short) 2).setCommitted(true)),
            new Fixture(ApiKeys.INIT_PRODUCER_ID, (short) 2, new InitProducerIdRequestData().setTransactionalId("eos-1").setTransactionTimeoutMs(60000)),
            new Fixture(ApiKeys.INIT_PRODUCER_ID, (short) 5, new InitProducerIdRequestData().setTransactionalId("eos-1").setTransactionTimeoutMs(60000).setProducerId(42).setProducerEpoch((short) 2)),
            new Fixture(ApiKeys.PRODUCE, (short) 12, produce()),
            new Fixture(ApiKeys.ADD_PARTITIONS_TO_TXN, (short) 3, new AddPartitionsToTxnRequestData().setV3AndBelowTransactionalId("eos-1").setV3AndBelowProducerId(42).setV3AndBelowProducerEpoch((short) 2).setV3AndBelowTopics(topics)),
            new Fixture(ApiKeys.ADD_PARTITIONS_TO_TXN, (short) 5, new AddPartitionsToTxnRequestData().setTransactions(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection(List.of(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction().setTransactionalId("eos-1").setProducerId(42).setProducerEpoch((short) 2).setTopics(topics)).iterator()))),
            new Fixture(ApiKeys.ADD_OFFSETS_TO_TXN, (short) 3, new AddOffsetsToTxnRequestData().setTransactionalId("eos-1").setProducerId(42).setProducerEpoch((short) 2).setGroupId("group")),
            new Fixture(ApiKeys.TXN_OFFSET_COMMIT, (short) 1, offsets()),
            new Fixture(ApiKeys.TXN_OFFSET_COMMIT, (short) 5, offsets()),
            new Fixture(ApiKeys.FIND_COORDINATOR, (short) 3, new FindCoordinatorRequestData().setKey("eos-1").setKeyType((byte) 1)),
            new Fixture(ApiKeys.FIND_COORDINATOR, (short) 6, new FindCoordinatorRequestData().setKeyType((byte) 1).setCoordinatorKeys(List.of("eos-1"))));
    }

    static ProduceRequestData produce() {
        var records = MemoryRecords.withTransactionalRecords(Compression.NONE, 42, (short) 2, 0, new SimpleRecord(new byte[]{1, 2}));
        var partition = new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records);
        var topic = new ProduceRequestData.TopicProduceData().setName("output").setPartitionData(new java.util.ArrayList<>(List.of(partition)));
        return new ProduceRequestData().setTransactionalId("eos-1").setAcks((short) -1).setTimeoutMs(1000)
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(List.of(topic).iterator()));
    }

    static ProduceRequestData nonTransactionalProduce(boolean idempotent) {
        var request = produce().setTransactionalId(null);
        request.topicData().iterator().next().partitionData().getFirst().setRecords(idempotent
                ? MemoryRecords.withIdempotentRecords(Compression.NONE, 42, (short) 2, 0, new SimpleRecord(new byte[]{1, 2}))
                : MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[]{1, 2})));
        return request;
    }

    @Test void nonTransactionalProduceRequiresTopicScopeAndConsistentBatchFlags() throws Exception {
        for (boolean idempotent : List.of(false, true)) {
            var request = nonTransactionalProduce(idempotent);
            var identity = ProtocolMessages.identity((short)12, wire(new Fixture(ApiKeys.PRODUCE, (short)12, request))).orElseThrow();
            assertNull(identity.transactionalId());
            assertEquals(idempotent ? Long.valueOf(42) : null, identity.producerId());
            var json = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode().put("faultId","alo").put("api","produce")
                    .put("action","drop-response").put("occurrences",1).put("triggerDeadlineNanos",1000);
            assertFalse(FaultRule.parse(json).matches(identity));
            json.put("topic","output"); assertTrue(FaultRule.parse(json).matches(identity));
            json.put("topic","foreign"); assertFalse(FaultRule.parse(json).matches(identity));
            json.put("topic","output").put("transactionalIdPrefix","eos-"); assertFalse(FaultRule.parse(json).matches(identity));
            assertTrue(ProtocolMessages.identity((short)12, request.setTransactionalId("eos-1")).isEmpty());
        }
        assertTrue(ProtocolMessages.identity((short)12, produce().setTransactionalId(null)).isEmpty());
    }

    static TxnOffsetCommitRequestData offsets() {
        return new TxnOffsetCommitRequestData().setTransactionalId("eos-1").setProducerId(42).setProducerEpoch((short) 2).setGroupId("group")
            .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("input").setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(0).setCommittedOffset(5)))));
    }

    static java.nio.ByteBuffer bytes(ApiMessage message, short version) { return MessageUtil.toByteBufferAccessor(message, version).buffer(); }

    static ApiMessage wire(Fixture fixture) {
        var bytes = ProtocolMessagesTest.bytes(fixture.request(), fixture.version());
        ApiMessage decoded = ApiMessageType.fromApiKey(fixture.api().id).newRequest();
        decoded.read(new ByteBufferAccessor(bytes), fixture.version());
        assertFalse(bytes.hasRemaining());
        return decoded;
    }

    @Test void readsRealKafkaSerializedRequestsWithoutInventingAbsentFields() {
        for (var fixture : fixtures()) {
            var identity = ProtocolMessages.identity(fixture.version(), wire(fixture)).orElseThrow();
            assertEquals("eos-1", identity.transactionalId(), fixture.toString());
            if (fixture.api() == ApiKeys.FIND_COORDINATOR || fixture.api() == ApiKeys.INIT_PRODUCER_ID && fixture.version() < 3) {
                assertNull(identity.producerId()); assertNull(identity.producerEpoch());
            } else { assertEquals(42L, identity.producerId()); assertEquals((short) 2, identity.producerEpoch()); }
        }
    }

    @Test void skipsAmbiguousIdentitiesAndNonTransactionalMessages() {
        assertTrue(ProtocolMessages.identity((short) 6, new FindCoordinatorRequestData().setKeyType((byte) 1).setCoordinatorKeys(List.of("eos-1", "foreign-1"))).isEmpty());
        assertTrue(ProtocolMessages.identity((short) 3, new FindCoordinatorRequestData().setKey("eos-1").setKeyType((byte) 0)).isEmpty());
        assertTrue(ProtocolMessages.identity((short) 5, new InitProducerIdRequestData()).isEmpty());
        assertTrue(ProtocolMessages.identity((short) 12, produce().setAcks((short) 0)).isEmpty());
        assertTrue(ProtocolMessages.identity((short) 13, produce()).isEmpty());
        var mixed = produce();
        mixed.topicData().iterator().next().partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(1)
            .setRecords(MemoryRecords.withTransactionalRecords(Compression.NONE, 99, (short) 2, 0, new SimpleRecord(new byte[]{3}))));
        assertTrue(ProtocolMessages.identity((short) 12, mixed).isEmpty());
    }

    @Test void preservesMixedPartitionResponseErrors() {
        var topic = new ProduceResponseData.TopicProduceResponse().setName("output").setPartitionResponses(List.of(
            new ProduceResponseData.PartitionProduceResponse().setIndex(0).setErrorCode(Errors.NONE.code()),
            new ProduceResponseData.PartitionProduceResponse().setIndex(1).setErrorCode(Errors.REQUEST_TIMED_OUT.code())));
        var response = new ProduceResponseData().setResponses(new ProduceResponseData.TopicProduceResponseCollection(List.of(topic).iterator()));
        var bytes = ProtocolMessagesTest.bytes(response, (short) 12);
        var decoded = new ProduceResponseData(new ByteBufferAccessor(bytes), (short) 12);
        assertEquals(Set.of((short) 0, Errors.REQUEST_TIMED_OUT.code()), Set.copyOf(ProtocolMessages.errors((short) 12, decoded).values()));
    }
}
