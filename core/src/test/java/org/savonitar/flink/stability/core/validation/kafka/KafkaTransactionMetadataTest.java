package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class KafkaTransactionMetadataTest {
    @Test void metadataMustDescribeEveryPartitionWithoutGaps() {
        assertEquals(3, KafkaAdminTransactionLister.partitionCount(List.of(partition(2), partition(0), partition(1))));
        assertThrows(IllegalArgumentException.class, () -> KafkaAdminTransactionLister.partitionCount(List.of()));
        assertThrows(IllegalArgumentException.class, () -> KafkaAdminTransactionLister.partitionCount(List.of(partition(0), partition(2))));
        assertThrows(IllegalArgumentException.class, () -> KafkaAdminTransactionLister.partitionCount(List.of(partition(0), partition(0))));
    }
    private static TopicPartitionInfo partition(int id) {
        return new TopicPartitionInfo(id, Node.noNode(), List.of(), List.of());
    }
}
