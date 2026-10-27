package org.savonitar.flink.stability.core.validation.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class KafkaClientStateSnapshotReaderTest {
    private static final String TOPIC = "output";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC,0);

    @Test void usesRawBoundaryAndRawBytesIncludingDelayedCommittedRecords() throws Exception {
        var boundary = configured(0,3);
        var committed = new MockConsumer<byte[],byte[]>("earliest") {
            @Override public synchronized Map<TopicPartition,Long> endOffsets(Collection<TopicPartition> partitions,Duration timeout) {
                throw new AssertionError("Committed LSO cannot define the raw high watermark");
            }
        };
        configure(committed,0,3);
        byte[] longJson = (KafkaStatePrefixValidatorTest.json(0,0,1,1)+" ".repeat(80)).getBytes(StandardCharsets.UTF_8);
        committed.schedulePollTask(() -> committed.addRecord(new ConsumerRecord<>(TOPIC,0,0,null,longJson)));
        committed.schedulePollTask(() -> {
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,1,null,new byte[]{(byte)0xc3,0x28}));
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,2,null,null));
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,3,null,new byte[]{9}));
        });
        var isolations = new ArrayList<String>();
        var reader = new KafkaClientStateSnapshotReader(properties -> {
            assertEquals(ByteArrayDeserializer.class,properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
            String isolation = properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
            isolations.add(isolation);
            return isolation.equals("read_uncommitted") ? boundary : committed;
        });
        var snapshot = reader.read("kafka:9092",TOPIC,Duration.ofSeconds(5),100,128*1024*1024L);
        assertEquals(List.of("read_uncommitted","read_committed"),isolations);
        assertEquals(Map.of(0,3L),snapshot.endOffsets());
        assertEquals(3,snapshot.records().size());
        assertArrayEquals(longJson,snapshot.records().get(0).bytes());
        assertArrayEquals(new byte[]{(byte)0xc3,0x28},snapshot.records().get(1).bytes());
        assertEquals(-1,snapshot.records().get(2).originalSize());
        assertTrue(boundary.closed());
        assertTrue(committed.closed());
    }

    @Test void oversizedValuesKeepOriginalLengthAndCannotBeParsedAsTheirValidPrefix() throws Exception {
        var boundary = configured(0,1);
        var committed = configured(0,1);
        String oversized = KafkaStatePrefixValidatorTest.json(0,0,1,1)+" ".repeat(2000);
        committed.schedulePollTask(() -> committed.addRecord(new ConsumerRecord<>(TOPIC,0,0,null,
                oversized.getBytes(StandardCharsets.UTF_8))));
        var snapshot = reader(boundary,committed).read("k",TOPIC,Duration.ofSeconds(5),10,1024);
        assertEquals(1024,snapshot.records().getFirst().retainedBytes());
        assertEquals(oversized.length(),snapshot.records().getFirst().originalSize());
        var result = new KafkaStatePrefixValidator((b,t,d,n,bytes)->snapshot).validate(
                "k",TOPIC,KafkaStatePrefixValidatorTest.manifest(1),1,Duration.ofSeconds(5));
        assertEquals("validator.kafka.state-prefix.malformed-records",result.reason());
    }

    @Test void byteBudgetFailsWithPartialBoundsAndObservedCount() {
        var boundary = configured(0,2);
        var committed = configured(0,2);
        committed.schedulePollTask(() -> {
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,0,null,new byte[600]));
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,1,null,new byte[600]));
        });
        var failure = assertThrows(KafkaSnapshotException.class,()->reader(boundary,committed).read(
                "k",TOPIC,Duration.ofSeconds(5),10,1024));
        assertEquals("verification.kafka.output-byte-limit-exceeded",failure.reason());
        assertEquals(2,failure.partialEvidence().observedCount());
        assertEquals(Map.of(0,2L),failure.partialEvidence().endOffsets());
        assertTrue(committed.closed());
    }

    @Test void recordBudgetIsIndependentOfByteBudget() {
        var boundary = configured(0,2);
        var committed = configured(0,2);
        committed.schedulePollTask(() -> {
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,0,null,null));
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,1,null,null));
        });
        var failure = assertThrows(KafkaSnapshotException.class,()->reader(boundary,committed).read(
                "k",TOPIC,Duration.ofSeconds(5),1,1024));
        assertEquals("verification.kafka.output-limit-exceeded",failure.reason());
        assertEquals(2,failure.partialEvidence().observedCount());
    }

    @Test void timeoutAfterPollDoesNotExamineLateRecords() {
        AtomicLong clock = new AtomicLong();
        var boundary = configured(0,1);
        var committed = configured(0,1);
        committed.schedulePollTask(()-> {
            committed.addRecord(new ConsumerRecord<>(TOPIC,0,0,null,new byte[]{1}));
            clock.set(2);
        });
        var reader = new KafkaClientStateSnapshotReader(properties -> properties.getProperty(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG).equals("read_uncommitted") ? boundary : committed, clock::get);
        var failure = assertThrows(KafkaSnapshotException.class,()->reader.read("k",TOPIC,Duration.ofNanos(1),10,1024));
        assertEquals("verification.kafka.incomplete-after-timeout",failure.reason());
        assertEquals(0,failure.partialEvidence().observedCount());
    }

    @Test void rejectsTruncatedSnapshotAndRetainsBounds() {
        var failure = assertThrows(KafkaSnapshotException.class,()->reader(configured(1,2),configured(1,2))
                .read("k",TOPIC,Duration.ofSeconds(5),10,1024));
        assertEquals("verification.kafka.snapshot-truncated",failure.reason());
        assertEquals(Map.of(0,1L),failure.partialEvidence().beginningOffsets());
        assertEquals(Map.of(0,2L),failure.partialEvidence().endOffsets());
    }

    private static KafkaClientStateSnapshotReader reader(MockConsumer<byte[],byte[]> boundary,MockConsumer<byte[],byte[]> committed) {
        return new KafkaClientStateSnapshotReader(properties -> properties.getProperty(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG).equals("read_uncommitted") ? boundary : committed);
    }

    private static MockConsumer<byte[],byte[]> configured(long beginning,long end) {
        var consumer = new MockConsumer<byte[],byte[]>("earliest");
        configure(consumer,beginning,end);
        return consumer;
    }

    private static void configure(MockConsumer<byte[],byte[]> consumer,long beginning,long end) {
        consumer.updatePartitions(TOPIC,List.of(new PartitionInfo(TOPIC,0,Node.noNode(),new Node[0],new Node[0])));
        consumer.updateBeginningOffsets(Map.of(PARTITION,beginning));
        consumer.updateEndOffsets(Map.of(PARTITION,end));
    }
}
