package org.savonitar.flink.stability.flinkjob;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class FlinkKafkaEosJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaEosJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("FlinkKafkaEosJob job starting with args={}", Arrays.toString(args));
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String bootstrapServers = parameters.getRequired("bootstrapServers");
        AtomicInteger processingDelayMs = new AtomicInteger(parameters.getInt("processingDelayMs", 0));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.getCheckpointConfig().setCheckpointStorage("file:/flink/checkpoints");


        Properties props = new Properties();
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                (int) Duration.ofHours(2).toMillis());
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("client.id", "flink-producer");
        props.setProperty("metadata.max.age.ms", "5000");
        LOG.info("Connecting to Kafka at: {}", bootstrapServers);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("input-topic")
                .setGroupId("flink-job-test-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink-output")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setKafkaProducerConfig(props)
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(x -> {
                    if (processingDelayMs.get() != 0) {
                        Thread.sleep(processingDelayMs.get());
                    } else {
                        Thread.sleep(100);
                    }
                    LOG.info("FlinkKafkaEosJob Processed msg={}", x);
                    return x;
                })
                .sinkTo(sink).name("Kafka Sink");

        env.execute("Flink Kafka Source-Sink Job");
    }
}
