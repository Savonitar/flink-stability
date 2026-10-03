package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.kafka.common.errors.ConcurrentTransactionsException;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.objenesis.ObjenesisStd;

import java.util.List;
import java.util.Properties;

/** Direct decision check, no producer constructor, threads, broker or network. */
public final class DecisionCheck {
    public static void main(String[] args) throws Exception {
        boolean mutant = args[0].equals("mutant");
        RuntimeException[] failures = {new TimeoutException("uncertain commit"),
                new ConcurrentTransactionsException("concurrent"),
                new CoordinatorLoadInProgressException("loading"),
                new NotCoordinatorException("moved"),
                new RetriableException("unrelated retry") {}, null,
                new ProducerFencedException("fenced"), new IllegalStateException("unknown")};
        for (boolean recovered : new boolean[]{false, true}) {
            for (int index = 0; index < failures.length; index++) {
                String id = args[0] + "-" + recovered + "-" + index;
                TestProducer producer = new ObjenesisStd().newInstance(TestProducer.class);
                producer.failure = failures[index];
                producer.transactionId = id;
                Request request = new Request(new KafkaCommittable(1L, (short) 0, id,
                        recovered ? null : producer));
                try (ReadableBackchannel<TransactionFinished> reader = BackchannelFactory.getInstance()
                                .getReadableBackchannel(0, 0, id);
                        KafkaCommitter committer = new KafkaCommitter(new Properties(), id, 0, 0,
                                false, (config, txn) -> producer)) {
                    committer.commit(List.of(request));
                    boolean dropped = mutant && index < 4;
                    require(request.retry == (index < 5 && !dropped), "retry decision " + id);
                    require(request.known == (index == 6 ? failures[index] : null), "known failure " + id);
                    require(request.unknown == (index == 7 ? failures[index] : null), "unknown failure " + id);
                    TransactionFinished notice = reader.poll();
                    require((notice != null) == (dropped || index == 5 || index == 6), "notice " + id);
                    if (notice != null) require(notice.isSuccess() == (index == 5), "notice outcome " + id);
                    require(reader.poll() == null, "single notice " + id);
                    require(producer.closed == (dropped || (recovered && index >= 6)), "disposed " + id);
                    if (dropped && recovered) require(committer.getCommittingProducer() == null, "clear closed producer");
                }
            }
        }
        System.out.println("DECISION_CHECK_PASS mode=" + args[0] + " cases=16");
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    public static final class TestProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        boolean closed;
        RuntimeException failure;
        String transactionId;
        private TestProducer() { super(new Properties()); }
        @Override public void commitTransaction() { if (failure != null) throw failure; }
        @Override public String getTransactionalId() { return transactionId; }
        @Override public void resumeTransaction(long id, short epoch) {}
        @Override public void close() { closed = true; }
        @Override public String toString() { return "controlled-producer"; }
    }
    private static final class Request implements CommitRequest<KafkaCommittable> {
        final KafkaCommittable commmittable;
        boolean retry;
        Throwable known;
        Throwable unknown;
        Request(KafkaCommittable value) { commmittable = value; }
        public KafkaCommittable getCommittable() { return commmittable; }
        public int getNumberOfRetries() { return 0; }
        public void retryLater() { retry = true; }
        public void signalFailedWithKnownReason(Throwable cause) { known = cause; }
        public void signalFailedWithUnknownReason(Throwable cause) { unknown = cause; }
        public void updateAndRetryLater(KafkaCommittable replacement) { throw new AssertionError(); }
        public void signalAlreadyCommitted() { throw new AssertionError(); }
    }
}
