package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.objenesis.ObjenesisStd;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExperimentCheck {
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        timing();
        unchangedOutcomes(mode);
        decision(mode, false);
        decision(mode, true);
        if (mode.equals("rewrite")) replay();
        System.out.println("EXPERIMENT_CHECK_PASS mode=" + mode);
    }

    private static void timing() throws Exception {
        Method method = FlinkKafkaInternalProducer.class
                .getDeclaredMethod("withTransactionalId", Properties.class, String.class);
        method.setAccessible(true);
        Properties input = new Properties();
        Properties result = (Properties) method.invoke(null, input, "transaction");
        require(result.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG).equals("5000"), "commit wait");
        require(result.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG).equals("30000"), "request wait");
        require(input.isEmpty(), "caller properties unchanged");
        TransactionalRequestResult pending = new TransactionalRequestResult("EndTxn");
        expect(TimeoutException.class, () -> pending.await(10, TimeUnit.MILLISECONDS));
        require(!pending.isCompleted(), "deadline does not cancel the request");
        pending.done();
        pending.await(10, TimeUnit.MILLISECONDS);
        require(pending.isSuccessful(), "request can complete after the caller timed out");
    }

    private static void decision(String mode, boolean recovered) throws Exception {
        TestProducer producer = new ObjenesisStd().newInstance(TestProducer.class);
        producer.mode = mode;
        producer.failure = new TimeoutException("controlled timeout");
        String id = mode + recovered;
        Request request = new Request(new KafkaCommittable(1L, (short) 0, id,
                recovered ? null : producer));
        try (ReadableBackchannel<TransactionFinished> reader = BackchannelFactory.getInstance()
                        .getReadableBackchannel(0, 0, id);
                KafkaCommitter committer = new KafkaCommitter(new Properties(), id, 0, 0,
                        false, (config, txn) -> producer)) {
            if (recovered && !mode.equals("control")) {
                expect(IllegalStateException.class, () -> {
                    try { committer.commit(List.of(request)); }
                    catch (java.io.IOException | InterruptedException error) { throw new AssertionError(error); }
                });
                require(reader.poll() == null, "unsupported recovery cannot notify success");
                require(!producer.closed, "unsupported recovery did not mutate producer");
            } else {
                committer.commit(List.of(request));
                require(request.retry == mode.equals("control"), "decision retry signal");
                TransactionFinished notice = reader.poll();
                require(mode.equals("control") ? notice == null : notice != null && !notice.isSuccess(),
                        "wrong-decision completion discards original producer");
                require(producer.closed == !mode.equals("control"), "old producer stopped");
                require(producer.replayed == mode.equals("rewrite"), "replay selected only in rewrite");
                require(reader.poll() == null, "single notice");
            }
        }
    }

    private static void unchangedOutcomes(String mode) throws Exception {
        RuntimeException[] failures = {null, new ProducerFencedException("controlled fencing"),
                new IllegalStateException("controlled unknown error"),
                new RetriableException("controlled non-timeout retry") {}};
        for (int index = 0; index < failures.length; index++) {
            String id = mode + "-unchanged-" + index;
            TestProducer producer = new ObjenesisStd().newInstance(TestProducer.class);
            producer.failure = failures[index];
            producer.transactionId = id;
            Request request = new Request(new KafkaCommittable(1L, (short) 0, id, producer));
            try (ReadableBackchannel<TransactionFinished> reader = BackchannelFactory.getInstance()
                            .getReadableBackchannel(0, 0, id);
                    KafkaCommitter committer = new KafkaCommitter(new Properties(), id, 0, 0,
                            false, (config, txn) -> { throw new AssertionError("unexpected factory"); })) {
                committer.commit(List.of(request));
                require(request.retry == (index == 3), "unchanged retry behavior");
                require(request.known == (index == 1 ? failures[index] : null), "unchanged known failure");
                require(request.unknown == (index == 2 ? failures[index] : null), "unchanged unknown failure");
                TransactionFinished notice = reader.poll();
                require(index <= 1 ? notice != null && notice.isSuccess() == (index == 0)
                        : notice == null, "unchanged backchannel outcome");
                require(!producer.replayed && !producer.closed, "non-timeout path does not mutate producer");
            }
        }
    }

    private static void replay() {
        CalibrationReplay buffer = new CalibrationReplay(new Properties());
        expect(IllegalStateException.class, () -> buffer.replay(() -> {}));
        buffer.begin("original");
        byte[] key = {1}, value = {2}, header = {3};
        buffer.capture(new ProducerRecord<>("output", 0, 123L, key, value,
                new RecordHeaders().add("test", header)));
        key[0] = 9; value[0] = 9; header[0] = 9;
        AtomicBoolean stopped = new AtomicBoolean();
        MockProducer<byte[], byte[]> mock = mock();
        int count = buffer.replay(() -> stopped.set(true), props -> {
            require(stopped.get(), "old sender stopped before replacement creation");
            require(props.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
                    .startsWith("original-calibration-rewrite-"), "fresh transaction ID");
            return mock;
        });
        require(count == 1 && mock.commitCount() == 1 && mock.closed(), "replay committed and closed");
        ProducerRecord<byte[], byte[]> replayed = mock.history().get(0);
        require(replayed.key()[0] == 1 && replayed.value()[0] == 2
                && replayed.headers().lastHeader("test").value()[0] == 3, "deep payload copy");
        require(replayed.partition() == 0 && replayed.timestamp() == 123L, "record routing metadata");
        expect(IllegalStateException.class, () -> buffer.replay(() -> {}));
        buffer.begin("failed");
        buffer.capture(new ProducerRecord<>("output", new byte[] {1}));
        MockProducer<byte[], byte[]> failing = mock();
        failing.commitTransactionException = new TimeoutException("replay timeout");
        expect(TimeoutException.class, () -> buffer.replay(() -> {}, props -> failing));
        require(failing.closed(), "failed replay closes replacement");
        expect(IllegalStateException.class, () -> buffer.replay(() -> {}));
        buffer.begin("limits");
        expect(IllegalStateException.class, () -> buffer.capture(new ProducerRecord<>("output", "text")));
        expect(IllegalStateException.class, () -> buffer.replay(() -> {}));
        buffer.begin("bytes");
        expect(IllegalStateException.class, () -> buffer.capture(
                new ProducerRecord<>("output", new byte[(int) CalibrationReplay.MAX_BYTES])));
        buffer.begin("records");
        for (int i = 0; i < CalibrationReplay.MAX_RECORDS; i++)
            buffer.capture(new ProducerRecord<>("output", new byte[0]));
        expect(IllegalStateException.class, () -> buffer.capture(new ProducerRecord<>("output", new byte[0])));
        buffer.begin("headers");
        RecordHeaders tooMany = new RecordHeaders();
        for (int i = 0; i <= CalibrationReplay.MAX_HEADERS; i++) tooMany.add("", null);
        expect(IllegalStateException.class, () -> buffer.capture(
                new ProducerRecord<>("output", 0, null, null, new byte[0], tooMany)));
    }

    private static MockProducer<byte[], byte[]> mock() {
        return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    }
    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static void expect(Class<? extends RuntimeException> type, Runnable operation) {
        try { operation.run(); } catch (RuntimeException failure) {
            if (type.isInstance(failure)) return;
            throw failure;
        }
        throw new AssertionError("Expected " + type.getSimpleName());
    }
    public static final class TestProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        String mode;
        boolean closed;
        boolean replayed;
        RuntimeException failure;
        String transactionId;
        private TestProducer() { super(new Properties()); }
        public void commitTransaction() { if (failure != null) throw failure; }
        public String getTransactionalId() { return transactionId; }
        public void resumeTransaction(long id, short epoch) {}
        public void close() { closed = true; }
        public int calibrationReplayUnknownCommit() { replayed = true; close(); return 1; }
        public String toString() { return "controlled-producer"; }
    }
    private static final class Request implements CommitRequest<KafkaCommittable> {
        final KafkaCommittable committable;
        boolean retry;
        Throwable known;
        Throwable unknown;
        Request(KafkaCommittable committable) { this.committable = committable; }
        public KafkaCommittable getCommittable() { return committable; }
        public int getNumberOfRetries() { return 0; }
        public void retryLater() { retry = true; }
        public void signalFailedWithKnownReason(Throwable cause) { known = cause; }
        public void signalFailedWithUnknownReason(Throwable cause) { unknown = cause; }
        public void updateAndRetryLater(KafkaCommittable replacement) { throw new AssertionError(); }
        public void signalAlreadyCommitted() { throw new AssertionError(); }
    }
}
