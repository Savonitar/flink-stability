package org.apache.flink.connector.kafka.sink.internal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

/** Experimental wrong-decision payload buffer; only live byte[] producers are supported. */
final class CalibrationReplay {
    static final int MAX_RECORDS = 10_000;
    static final long MAX_BYTES = 16 * 1024 * 1024;
    static final int MAX_HEADERS = 128;
    private final Properties properties = new Properties();
    private final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    private String transactionalId;
    private long bytes;
    private boolean failed;

    CalibrationReplay(Properties properties) {
        this.properties.putAll(properties);
    }

    void begin(String id) {
        clear();
        transactionalId = id;
    }

    void clear() {
        records.clear();
        bytes = 0;
        failed = false;
        transactionalId = null;
    }

    void capture(ProducerRecord<?, ?> record) {
        if (failed || transactionalId == null) {
            throw unsupported("missing live transaction or invalid buffer");
        }
        try {
            long size = stringBound(record.topic()) + length(record.key()) + length(record.value());
            int headerCount = 0;
            for (Header header : record.headers()) {
                if (++headerCount > MAX_HEADERS) throw unsupported("header count limit exceeded");
                size += stringBound(header.key()) + length(header.value());
            }
            if (records.size() >= MAX_RECORDS || size > MAX_BYTES - bytes) {
                throw unsupported("payload buffer limit exceeded");
            }
            RecordHeaders headers = new RecordHeaders();
            for (Header header : record.headers()) headers.add(header.key(), copy(header.value()));
            records.add(new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                    copy(record.key()), copy(record.value()), headers));
            bytes += size;
        } catch (RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    int replay(Runnable stopOriginal) {
        return replay(stopOriginal, KafkaProducer::new);
    }

    int replay(Runnable stopOriginal,
            Function<Properties, Producer<byte[], byte[]>> factory) {
        if (failed || transactionalId == null || records.isEmpty()) {
            throw unsupported("recovered, empty, or invalid live payload buffer");
        }
        Properties replayProperties = new Properties();
        replayProperties.putAll(properties);
        replayProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                transactionalId + "-calibration-rewrite-" + UUID.randomUUID());
        failed = true; // An unsuccessful replay is terminal; never silently replay it again.
        // A completed close stops the old sender before any replacement producer starts.
        stopOriginal.run();
        Producer<byte[], byte[]> producer = factory.apply(replayProperties);
        int count = records.size();
        try {
            producer.initTransactions();
            producer.beginTransaction();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            for (ProducerRecord<byte[], byte[]> record : records) {
                if (System.nanoTime() >= deadline) throw unsupported("replay send deadline exceeded");
                producer.send(record);
            }
            producer.commitTransaction();
            clear();
            return count;
        } finally {
            producer.close(Duration.ZERO);
        }
    }

    private static byte[] copy(Object value) {
        if (value == null) return null;
        if (value instanceof byte[]) return ((byte[]) value).clone();
        throw unsupported("only byte[] keys and values are supported");
    }

    private static long length(Object value) {
        if (value == null) return 0;
        if (value instanceof byte[]) return ((byte[]) value).length;
        throw unsupported("only byte[] keys and values are supported");
    }
    private static long stringBound(String value) { return 3L * value.length(); }
    private static IllegalStateException unsupported(String reason) {
        return new IllegalStateException("CALIBRATION_UNSUPPORTED " + reason);
    }
}
