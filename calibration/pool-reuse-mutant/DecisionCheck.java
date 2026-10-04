package org.apache.flink.connector.kafka.sink.internal;

import org.objenesis.ObjenesisStd;

import java.util.List;
import java.util.Properties;

/** Exercises the real pool without constructing a Kafka client or opening a connection. */
public final class DecisionCheck {
    public static void main(String[] args) throws Exception {
        boolean mutant = args[0].equals("mutant");
        for (String requested : new String[]{"same", "different", null}) {
            try (ProducerPoolImpl pool = new ProducerPoolImpl(new Properties(),
                    ignored -> { throw new AssertionError("Unexpected producer allocation"); }, List.of())) {
                RecordingProducer producer = new ObjenesisStd().newInstance(RecordingProducer.class);
                producer.id = "same";
                pool.getProducers().add(producer);
                require(pool.getTransactionalProducer(requested, 2) == producer, "reuses instance");
                boolean initialized = requested != null && !(mutant && requested.equals("same"));
                require(producer.resets == (initialized ? 1 : 0), "reset decision");
                require(producer.initializations == (initialized ? 1 : 0), "init decision");
                producer.beginTransaction();
                require(producer.inTransaction, "caller starts next transaction");
                if (requested != null) {
                    require(pool.getOngoingTransactions().size() == 1, "tracks reused ID");
                    CheckpointTransaction txn = pool.getOngoingTransactions().iterator().next();
                    require(txn.getCheckpointId() == 2 && txn.getTransactionalId().equals(requested), "checkpoint identity");
                    pool.recycleByTransactionId(requested, true);
                    require(producer.commits == 1 && !producer.inTransaction, "recycle double commit");
                    require(pool.getOngoingTransactions().isEmpty(), "removes old checkpoint");
                    require(pool.getProducers().size() == 1, "returns producer");
                    pool.getTransactionalProducer(requested, 3);
                    require(producer.initializations == (mutant ? (initialized ? 1 : 0) : 2), "second reuse");
                    pool.recycleByTransactionId(requested, false);
                    require(producer.closed && pool.getProducers().isEmpty(), "failed producer discarded");
                }
            }
        }
        System.out.println("DECISION_CHECK_PASS mode=" + args[0]);
    }

    private static void require(boolean result, String message) {
        if (!result) throw new AssertionError(message);
    }

    public static final class RecordingProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        String id;
        int resets, initializations, commits;
        boolean inTransaction, closed;
        private RecordingProducer() { super(new Properties()); }
        @Override public String getTransactionalId() { return id; }
        @Override public void setTransactionId(String id) { this.id = id; resets++; }
        @Override public void initTransactions() { initializations++; }
        @Override public void beginTransaction() { inTransaction = true; }
        @Override public boolean isInTransaction() { return inTransaction; }
        @Override public void commitTransaction() { commits++; inTransaction = false; }
        @Override public void close() { closed = true; }
        @Override public String toString() { return "recording-producer"; }
    }
}
