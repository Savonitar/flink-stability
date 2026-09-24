package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class FlinkKafkaEosJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaEosJob.class);

    static final String SOURCE_UID = "flink-stability-kafka-source-v1";
    static final String THROTTLE_UID = "flink-stability-source-throttle-v1";
    static final String STATEFUL_OPERATOR_UID = "flink-stability-managed-state-pass-through-v1";
    static final String SINK_UID = "flink-stability-kafka-sink-v1";

    public static void main(String[] args) throws Exception {
        LOG.info("FlinkKafkaEosJob job starting with args={}", Arrays.toString(args));
        FlinkKafkaEosJobArguments arguments = FlinkKafkaEosJobArguments.from(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WorkloadProtocolV1Configuration workload =
                WorkloadProtocolV1Configuration.from(env.getConfiguration());

        LOG.info(
                "Starting workload alias={} with source={} and sink={}",
                workload.jobAlias(),
                workload.sourceBootstrapServers(),
                workload.sinkBootstrapServers());
        buildPipeline(env, workload, arguments);
        env.execute(workload.jobAlias());
    }

    static void buildPipeline(
            StreamExecutionEnvironment env,
            WorkloadProtocolV1Configuration workload,
            FlinkKafkaEosJobArguments arguments) {
        KafkaSource<String> source = createSource(workload);
        KafkaSink<String> sink = createSink(workload);

        // The delay runs in an operator chained to the source, so it slows the source itself.
        // A delay after the keyBy shuffle would let the source fill the network buffers
        // first; every checkpoint barrier would then wait behind that backlog, and a short
        // bounded input would finish before its first checkpoint completed.
        env.fromSource(source, workload.watermarks().toFlinkStrategy(), "Kafka Source")
                .uid(SOURCE_UID)
                .map(new SourceThrottle(arguments.processingDelayMs()))
                .name("Source Throttle")
                .uid(THROTTLE_UID)
                .keyBy(value -> value)
                .map(new ManagedStatePassThrough(workload.stateTtl()))
                .name("Managed State Pass-Through")
                .uid(STATEFUL_OPERATOR_UID)
                .sinkTo(sink)
                .name("Kafka Sink")
                .uid(SINK_UID);
    }

    @SuppressWarnings("deprecation") // Flink connector 5.0 still exposes Kafka's deprecated enum.
    static KafkaSource<String> createSource(WorkloadProtocolV1Configuration workload) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(workload.sourceBootstrapServers())
                .setTopics(workload.sourceTopic())
                .setGroupId(workload.sourceGroupId())
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setBounded(OffsetsInitializer.offsets(workload.sourceStoppingOffsetsByTopic()))
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    static KafkaSink<String> createSink(WorkloadProtocolV1Configuration workload) {
        Properties producerProperties = new Properties();
        if (workload.transactionTimeoutMs() != null) {
            producerProperties.setProperty(
                    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                    workload.transactionTimeoutMs().toString());
        }

        KafkaSinkBuilder<String> builder = KafkaSink.<String>builder()
                .setBootstrapServers(workload.sinkBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(workload.sinkTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(workload.deliveryGuarantee())
                .setKafkaProducerConfig(producerProperties);

        if (workload.deliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            builder.setTransactionalIdPrefix(workload.transactionalIdPrefix());
            builder.setTransactionNamingStrategy(workload.transactionNamingStrategy());
        }
        return builder.build();
    }

    private static final class SourceThrottle implements MapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private final int processingDelayMs;

        private SourceThrottle(int processingDelayMs) {
            this.processingDelayMs = processingDelayMs;
        }

        @Override
        public String map(String value) throws Exception {
            if (processingDelayMs > 0) {
                Thread.sleep(processingDelayMs);
            }
            return value;
        }
    }

    private static final class ManagedStatePassThrough extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;
        private static final String STATE_NAME = "flink-stability-last-record-v1";

        private final WorkloadProtocolV1Configuration.StateTtlSettings stateTtl;

        private transient ValueState<String> lastRecord;

        private ManagedStatePassThrough(
                WorkloadProtocolV1Configuration.StateTtlSettings stateTtl) {
            this.stateTtl = stateTtl;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>(STATE_NAME, String.class);
            if (stateTtl.enabled()) {
                descriptor.enableTimeToLive(stateTtl.toFlinkConfig());
            }
            lastRecord = getRuntimeContext().getState(descriptor);
        }

        @Override
        public String map(String value) throws Exception {
            lastRecord.update(value);
            LOG.debug("FlinkKafkaEosJob processed msg={}", value);
            return value;
        }
    }
}
