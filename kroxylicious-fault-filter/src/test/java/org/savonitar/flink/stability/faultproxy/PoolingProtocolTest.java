package org.savonitar.flink.stability.faultproxy;

import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class PoolingProtocolTest {
    static List<ProtocolMessagesTest.Fixture> fixtures() {
        var result = new ArrayList<ProtocolMessagesTest.Fixture>();
        for (short version = 0; version <= 1; version++) {
            if (version == 0) result.add(new ProtocolMessagesTest.Fixture(ApiKeys.DESCRIBE_PRODUCERS, version,
                    new DescribeProducersRequestData().setTopics(List.of(new DescribeProducersRequestData.TopicRequest()
                            .setName("output").setPartitionIndexes(List.of(0, 1))))));
            result.add(new ProtocolMessagesTest.Fixture(ApiKeys.LIST_TRANSACTIONS, version,
                    new ListTransactionsRequestData().setProducerIdFilters(List.of(42L, 43L)).setStateFilters(List.of("Ongoing"))));
        }
        return result;
    }
    @Test void decodesPinnedRecoveryRequestsWithoutInventingTransactionalIds() {
        for (var fixture : fixtures()) {
            var identity = ProtocolMessages.identity(fixture.version(), ProtocolMessagesTest.wire(fixture)).orElseThrow();
            assertNull(identity.transactionalId()); assertNull(identity.producerId());
            assertEquals(fixture.api() == ApiKeys.DESCRIBE_PRODUCERS ? Set.of("output") : Set.of(), identity.topics());
        }
    }
    @Test void unfilteredListingAndEmptyOrInvalidPartitionsCannotBeFaulted() {
        var list = new ListTransactionsRequestData().setStateFilters(List.of("Ongoing"));
        assertTrue(ProtocolMessages.identity((short) 1, list).isEmpty());
        list.setProducerIdFilters(List.of(-1L));
        assertTrue(ProtocolMessages.identity((short) 1, list).isEmpty());
        list.setProducerIdFilters(List.of(42L)).setStateFilters(List.of("CompleteCommit"));
        assertTrue(ProtocolMessages.identity((short) 1, list).isEmpty());
        assertTrue(ProtocolMessages.identity((short) 1, new DescribeProducersRequestData()).isEmpty());
        var describe = new DescribeProducersRequestData().setTopics(List.of(new DescribeProducersRequestData.TopicRequest()
                .setName("output").setPartitionIndexes(List.of(-1))));
        assertTrue(ProtocolMessages.identity((short) 1, describe).isEmpty());
    }
    @Test void mixedPartitionErrorsAndUnknownListingStatesAreNotSuccessfulReplies() {
        var response = new DescribeProducersResponseData().setTopics(List.of(new DescribeProducersResponseData.TopicResponse()
                .setName("output").setPartitions(List.of(
                        new DescribeProducersResponseData.PartitionResponse().setPartitionIndex(0),
                        new DescribeProducersResponseData.PartitionResponse().setPartitionIndex(1).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())))));
        assertEquals(Map.of("output/0", (short) 0, "output/1", Errors.NOT_LEADER_OR_FOLLOWER.code()), ProtocolMessages.errors((short) 0, response));
        assertTrue(ProtocolMessages.errors((short) 0, new ListTransactionsResponseData().setUnknownStateFilters(List.of("foreign")))
                .values().stream().anyMatch(code -> code != 0));
    }
}
