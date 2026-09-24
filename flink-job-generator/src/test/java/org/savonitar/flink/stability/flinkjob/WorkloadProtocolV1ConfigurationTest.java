package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadProtocolV1ConfigurationTest {

    @Test
    void parsesExactlyOnceWorkloadWithIndependentEndpointsAndBoundedOffsets() {
        WorkloadProtocolV1Configuration workload = parse(baseExactlyOnceValues());

        assertEquals("eos-job", workload.jobAlias());
        assertEquals("source-kafka:19092", workload.sourceBootstrapServers());
        assertEquals("source-topic", workload.sourceTopic());
        assertEquals("source-group", workload.sourceGroupId());
        assertEquals(Map.of(0, 1000L, 1, 750L), workload.sourceStoppingOffsets());
        assertEquals("sink-kafka:29092", workload.sinkBootstrapServers());
        assertEquals("sink-topic", workload.sinkTopic());
        assertEquals(DeliveryGuarantee.EXACTLY_ONCE, workload.deliveryGuarantee());
        assertEquals("scenario-attempt-job", workload.transactionalIdPrefix());
        assertEquals(TransactionNamingStrategy.INCREMENTING, workload.transactionNamingStrategy());
        assertEquals(7_200_000, workload.transactionTimeoutMs());
        assertFalse(workload.stateTtl().enabled());
        assertEquals("no-watermarks", workload.watermarks().strategy());

        assertEquals(Boundedness.BOUNDED, FlinkKafkaEosJob.createSource(workload).getBoundedness());
        assertDoesNotThrow(() -> FlinkKafkaEosJob.createSink(workload));
    }

    @Test
    void leavesStandardFlinkConfigurationOwnedByTheExecutionEnvironment() {
        Map<String, String> values = baseExactlyOnceValues();
        values.put("parallelism.default", "4");
        values.put("execution.checkpointing.interval", "5 s");
        values.put("state.backend.type", "hashmap");
        values.put("restart-strategy.type", "none");
        Configuration configuration = Configuration.fromMap(values);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        WorkloadProtocolV1Configuration workload =
                WorkloadProtocolV1Configuration.from(env.getConfiguration());

        assertEquals("4", env.getConfiguration().toMap().get("parallelism.default"));
        assertEquals(
                "hashmap", env.getConfiguration().toMap().get("state.backend.type"));
        assertEquals("none", env.getConfiguration().toMap().get("restart-strategy.type"));
        assertTrue(env.getCheckpointConfig().isCheckpointingEnabled());
        assertEquals(5_000L, env.getCheckpointConfig().getCheckpointInterval());
        assertEquals("eos-job", workload.jobAlias());
    }

    @Test
    void buildsPipelineWithStableOperatorUids() {
        Configuration configuration = Configuration.fromMap(baseExactlyOnceValues());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        WorkloadProtocolV1Configuration workload =
                WorkloadProtocolV1Configuration.from(env.getConfiguration());

        FlinkKafkaEosJob.buildPipeline(
                env, workload, FlinkKafkaEosJobArguments.from(new String[0]));

        StreamGraph streamGraph = env.getStreamGraph();
        Set<String> uids = streamGraph.getStreamNodes().stream()
                .map(StreamNode::getTransformationUID)
                .filter(uid -> uid != null)
                .collect(Collectors.toSet());
        assertTrue(uids.contains(FlinkKafkaEosJob.SOURCE_UID));
        assertTrue(uids.contains(FlinkKafkaEosJob.THROTTLE_UID));
        assertTrue(uids.contains(FlinkKafkaEosJob.STATEFUL_OPERATOR_UID));
        assertTrue(uids.contains(FlinkKafkaEosJob.SINK_UID));
        assertDoesNotThrow(() -> {
            streamGraph.getJobGraph();
        });
    }

    @Test
    void chainsTheProcessingDelayToTheSourceBeforeTheKeyedShuffle() {
        Configuration configuration = Configuration.fromMap(baseExactlyOnceValues());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        WorkloadProtocolV1Configuration workload =
                WorkloadProtocolV1Configuration.from(env.getConfiguration());

        FlinkKafkaEosJob.buildPipeline(
                env,
                workload,
                FlinkKafkaEosJobArguments.from(new String[]{"--processingDelayMs", "5"}));

        List<JobVertex> vertices =
                env.getStreamGraph().getJobGraph().getVerticesSortedTopologicallyFromSources();
        // The module compiles for Java 11, so no List.getFirst()/getLast().
        assertEquals(2, vertices.size());
        assertTrue(vertices.get(0).getName().contains("Kafka Source"));
        assertTrue(vertices.get(0).getName().contains("Source Throttle"));
        assertTrue(vertices.get(1).getName().contains("Managed State Pass-Through"));
    }

    @Test
    void supportsNonTransactionalSchemaGuaranteesWithoutTransactionOptions() {
        for (String guarantee : new String[]{"NONE", "AT_LEAST_ONCE"}) {
            Map<String, String> values = baseExactlyOnceValues();
            values.put(
                    WorkloadProtocolV1Configuration.SINK_DELIVERY_GUARANTEE,
                    guarantee);
            values.remove(WorkloadProtocolV1Configuration.SINK_TRANSACTIONAL_ID_PREFIX);
            values.remove(WorkloadProtocolV1Configuration.SINK_TRANSACTION_ID_NAMING_STRATEGY);
            values.remove(WorkloadProtocolV1Configuration.SINK_TRANSACTION_TIMEOUT_MS);

            WorkloadProtocolV1Configuration workload = parse(values);

            assertEquals(DeliveryGuarantee.valueOf(guarantee), workload.deliveryGuarantee());
            assertNull(workload.transactionalIdPrefix());
            assertNull(workload.transactionNamingStrategy());
            assertNull(workload.transactionTimeoutMs());
            assertDoesNotThrow(() -> FlinkKafkaEosJob.createSink(workload));
        }
    }

    @Test
    void parsesEveryStateTtlCleanupShape() {
        for (String cleanup : new String[]{"none", "incremental", "rocksdb-compaction-filter"}) {
            Map<String, String> values = baseExactlyOnceValues();
            values.put(WorkloadProtocolV1Configuration.STATE_TTL_ENABLED, "true");
            values.put(WorkloadProtocolV1Configuration.STATE_TTL_MS, "30000");
            values.put(WorkloadProtocolV1Configuration.STATE_TTL_CLEANUP, cleanup);

            WorkloadProtocolV1Configuration workload = parse(values);

            assertTrue(workload.stateTtl().enabled());
            assertEquals(30_000L, workload.stateTtl().ttlMs());
            assertEquals(cleanup, workload.stateTtl().cleanup());
            assertTrue(workload.stateTtl().toFlinkConfig().isEnabled());
        }
    }

    @Test
    void parsesEveryWatermarkShape() {
        Map<String, String> monotonicValues = baseExactlyOnceValues();
        monotonicValues.put(
                WorkloadProtocolV1Configuration.WATERMARKS_STRATEGY,
                "monotonic-timestamps");
        monotonicValues.put(WorkloadProtocolV1Configuration.WATERMARKS_IDLENESS_MS, "1000");
        WorkloadProtocolV1Configuration monotonic = parse(monotonicValues);
        assertEquals(1_000L, monotonic.watermarks().idlenessMs());
        assertDoesNotThrow(() -> monotonic.watermarks().toFlinkStrategy());

        Map<String, String> boundedValues = baseExactlyOnceValues();
        boundedValues.put(
                WorkloadProtocolV1Configuration.WATERMARKS_STRATEGY,
                "bounded-out-of-orderness");
        boundedValues.put(
                WorkloadProtocolV1Configuration.WATERMARKS_MAX_OUT_OF_ORDERNESS_MS,
                "250");
        WorkloadProtocolV1Configuration bounded = parse(boundedValues);
        assertEquals(250L, bounded.watermarks().maxOutOfOrdernessMs());
        assertDoesNotThrow(() -> bounded.watermarks().toFlinkStrategy());
    }

    @Test
    void rejectsUnknownReservedConfigurationKey() {
        Map<String, String> values = baseExactlyOnceValues();
        values.put("flink-stability.workload.v1.sink.typo", "value");

        assertThrows(IllegalArgumentException.class, () -> parse(values));
    }

    @Test
    void rejectsMissingOrWrongProtocolVersion() {
        Map<String, String> missing = baseExactlyOnceValues();
        missing.remove(WorkloadProtocolV1Configuration.PROTOCOL);
        assertThrows(IllegalArgumentException.class, () -> parse(missing));

        Map<String, String> wrong = baseExactlyOnceValues();
        wrong.put(WorkloadProtocolV1Configuration.PROTOCOL, "v2");
        assertThrows(IllegalArgumentException.class, () -> parse(wrong));
    }

    @Test
    void rejectsNonCanonicalOrUnorderedStoppingOffsets() {
        for (String offsets : new String[]{
                "01:1000",
                "1:750,0:1000",
                "0:1000,0:1001",
                "0:-1",
                "0:1000,",
                "2147483648:1"
        }) {
            Map<String, String> values = baseExactlyOnceValues();
            values.put(WorkloadProtocolV1Configuration.SOURCE_STOPPING_OFFSETS, offsets);
            assertThrows(IllegalArgumentException.class, () -> parse(values), offsets);
        }
    }

    @Test
    void rejectsIncompleteExactlyOnceTransactionConfiguration() {
        for (String key : new String[]{
                WorkloadProtocolV1Configuration.SINK_TRANSACTIONAL_ID_PREFIX,
                WorkloadProtocolV1Configuration.SINK_TRANSACTION_ID_NAMING_STRATEGY,
                WorkloadProtocolV1Configuration.SINK_TRANSACTION_TIMEOUT_MS
        }) {
            Map<String, String> values = baseExactlyOnceValues();
            values.remove(key);
            assertThrows(IllegalArgumentException.class, () -> parse(values), key);
        }
    }

    @Test
    void rejectsTransactionOptionsForNonTransactionalGuarantee() {
        Map<String, String> values = baseExactlyOnceValues();
        values.put(WorkloadProtocolV1Configuration.SINK_DELIVERY_GUARANTEE, "NONE");

        assertThrows(IllegalArgumentException.class, () -> parse(values));
    }

    @Test
    void rejectsTtlFieldsWhenTtlIsDisabled() {
        Map<String, String> values = baseExactlyOnceValues();
        values.put(WorkloadProtocolV1Configuration.STATE_TTL_MS, "1000");

        assertThrows(IllegalArgumentException.class, () -> parse(values));
    }

    @Test
    void rejectsWatermarkFieldsWhichDoNotBelongToTheStrategy() {
        Map<String, String> noWatermarks = baseExactlyOnceValues();
        noWatermarks.put(WorkloadProtocolV1Configuration.WATERMARKS_IDLENESS_MS, "1000");
        assertThrows(IllegalArgumentException.class, () -> parse(noWatermarks));

        Map<String, String> bounded = baseExactlyOnceValues();
        bounded.put(
                WorkloadProtocolV1Configuration.WATERMARKS_STRATEGY,
                "bounded-out-of-orderness");
        assertThrows(IllegalArgumentException.class, () -> parse(bounded));
    }

    private static WorkloadProtocolV1Configuration parse(Map<String, String> values) {
        return WorkloadProtocolV1Configuration.from(Configuration.fromMap(values));
    }

    private static Map<String, String> baseExactlyOnceValues() {
        Map<String, String> values = new HashMap<>();
        values.put(WorkloadProtocolV1Configuration.PROTOCOL, "v1");
        values.put(WorkloadProtocolV1Configuration.JOB_ALIAS, "eos-job");
        values.put(
                WorkloadProtocolV1Configuration.SOURCE_BOOTSTRAP_SERVERS,
                "source-kafka:19092");
        values.put(WorkloadProtocolV1Configuration.SOURCE_TOPIC, "source-topic");
        values.put(WorkloadProtocolV1Configuration.SOURCE_GROUP_ID, "source-group");
        values.put(
                WorkloadProtocolV1Configuration.SOURCE_STARTING_OFFSETS,
                "committed-or-earliest");
        values.put(
                WorkloadProtocolV1Configuration.SOURCE_STOPPING_OFFSETS,
                "0:1000,1:750");
        values.put(
                WorkloadProtocolV1Configuration.SOURCE_ISOLATION_LEVEL,
                "read_uncommitted");
        values.put(
                WorkloadProtocolV1Configuration.SINK_BOOTSTRAP_SERVERS,
                "sink-kafka:29092");
        values.put(WorkloadProtocolV1Configuration.SINK_TOPIC, "sink-topic");
        values.put(
                WorkloadProtocolV1Configuration.SINK_DELIVERY_GUARANTEE,
                "EXACTLY_ONCE");
        values.put(
                WorkloadProtocolV1Configuration.SINK_TRANSACTIONAL_ID_PREFIX,
                "scenario-attempt-job");
        values.put(
                WorkloadProtocolV1Configuration.SINK_TRANSACTION_ID_NAMING_STRATEGY,
                "INCREMENTING");
        values.put(
                WorkloadProtocolV1Configuration.SINK_TRANSACTION_TIMEOUT_MS,
                "7200000");
        values.put(WorkloadProtocolV1Configuration.STATE_TTL_ENABLED, "false");
        values.put(
                WorkloadProtocolV1Configuration.WATERMARKS_STRATEGY,
                "no-watermarks");
        return values;
    }
}
