package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Runs the real reader snapshot and split-restore APIs without constructing a Kafka client. */
public final class RecoveryCheck {
    private static final TopicPartition PARTITION = new TopicPartition("input", 0);

    public static void main(String[] args) throws Exception {
        require(args.length == 1 && (args[0].equals("release") || args[0].equals("mutant")),
                "Select release or mutant explicitly");
        boolean mutant = args[0].equals("mutant");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream original = System.err;
        try (PrintStream captured = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            System.setErr(captured);
            snapshotAndRestore(mutant, true);
            snapshotAndRestore(mutant, false);
            boundaries(mutant);
        } finally {
            System.setErr(original);
        }
        String markers = bytes.toString(StandardCharsets.UTF_8);
        if (mutant) {
            require(markers.contains("CALIBRATION_RECOVERY_SNAPSHOT checkpoint=20 topic=input"
                    + " partition=0 from=40 to=41 stop=100"), "snapshot marker identifies exact corruption");
            require(markers.contains("CALIBRATION_RECOVERY_ADD_SPLIT topic=input partition=0"
                    + " start=41 stop=100"), "restored addSplits marker identifies concrete offset");
            require(markers.contains("stop=100 process=" + System.getenv("HOSTNAME")),
                    "markers retain the available container hostname");
            require(!markers.contains("CALIBRATION_RECOVERY_SNAPSHOT checkpoint=99 "),
                    "ineligible offsets never claim a mutation");
        } else {
            require(!markers.contains("CALIBRATION_RECOVERY_"), "release artifact is unmodified");
        }
        System.out.println("RECOVERY_CHECK_PASS mode=" + args[0]);
    }

    private static void snapshotAndRestore(boolean mutant, boolean commits) throws Exception {
        KafkaPartitionSplit original = new KafkaPartitionSplit(PARTITION, 10, 100);
        try (Fixture live = new Fixture(commits)) {
            live.reader.addSplits(List.of(original));
            require(live.fetcher.assigned.get(0) == original, "addSplits forwards the original split");
            live.reader.states.get(PARTITION).setCurrentOffset(40);
            List<KafkaPartitionSplit> saved = live.reader.snapshotState(20);
            long expected = mutant ? 41 : 40;
            require(saved.size() == 1 && saved.get(0).getStartingOffset() == expected,
                    "real snapshot API returns the expected checkpoint copy");
            require(saved.get(0).getTopicPartition().equals(PARTITION)
                    && saved.get(0).getStoppingOffset().orElseThrow() == 100,
                    "checkpoint preserves partition and exclusive stopping offset");
            require(original.getStartingOffset() == 10, "caller split was not changed");
            require(live.reader.states.get(PARTITION).getCurrentOffset() == 40,
                    "snapshot never advances live reader state");
            require(live.reader.snapshotState(21).get(0).getStartingOffset() == expected,
                    "repeated snapshots do not accumulate an offset change in live state");
            if (commits) {
                require(live.reader.getOffsetsToCommit().get(20L).get(PARTITION).offset() == 40,
                        "Kafka commit bookkeeping retains the actual emitted offset");
            } else {
                require(live.reader.getOffsetsToCommit().isEmpty(), "disabled commits remain disabled");
            }
            live.reader.notifyCheckpointComplete(20);
            require(commits ? live.fetcher.committed.get(PARTITION).offset() == 40
                            : live.fetcher.committed == null,
                    "checkpoint notification never commits a corrupted offset to Kafka");

            KafkaPartitionSplitSerializer serializer = new KafkaPartitionSplitSerializer();
            KafkaPartitionSplit decoded = serializer.deserialize(
                    serializer.getVersion(), serializer.serialize(saved.get(0)));
            try (Fixture restored = new Fixture(commits)) {
                restored.reader.addSplits(List.of(decoded));
                require(restored.fetcher.assigned.size() == 1
                        && restored.fetcher.assigned.get(0) == decoded,
                        "restore hands the serialized checkpoint split to the fetcher unchanged");
                require(restored.reader.states.get(PARTITION).getCurrentOffset() == expected,
                        "restored state begins at the checkpoint offset");
            }

            live.reader.states.get(PARTITION).setCurrentOffset(44);
            require(live.reader.snapshotState(22).get(0).getStartingOffset() == (mutant ? 45 : 44),
                    "normal progress, not a previous mutated snapshot, determines the next snapshot");
        }
    }

    private static void boundaries(boolean mutant) throws Exception {
        // Concrete offsets strictly below a concrete bound are the only eligible splits.
        long[][] cases = {
            {0, 1, 1},
            {Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MAX_VALUE},
            {0, 0, 0},
            {100, 100, 100},
            {101, 100, 101},
            {Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE},
            {KafkaPartitionSplit.EARLIEST_OFFSET, 100, KafkaPartitionSplit.EARLIEST_OFFSET},
            {KafkaPartitionSplit.COMMITTED_OFFSET, 100, KafkaPartitionSplit.COMMITTED_OFFSET},
            {KafkaPartitionSplit.LATEST_OFFSET, 100, KafkaPartitionSplit.LATEST_OFFSET},
            {KafkaPartitionSplit.MIGRATED, 100, KafkaPartitionSplit.MIGRATED},
            {0, KafkaPartitionSplit.NO_STOPPING_OFFSET, 0},
            {0, KafkaPartitionSplit.LATEST_OFFSET, 0},
            {0, KafkaPartitionSplit.COMMITTED_OFFSET, 0}
        };
        for (long[] test : cases) {
            try (Fixture fixture = new Fixture(false)) {
                KafkaPartitionSplit split = new KafkaPartitionSplit(PARTITION, test[0], test[1]);
                fixture.reader.addSplits(List.of(split));
                boolean eligible = test[0] >= 0 && test[1] >= 0 && test[0] < test[1];
                KafkaPartitionSplit saved = fixture.reader.snapshotState(eligible ? 98 : 99).get(0);
                require(saved.getStartingOffset() == (mutant ? test[2] : test[0]),
                        "sentinel, finished, and overflow boundaries are preserved");
                require(saved.getStoppingOffset().equals(split.getStoppingOffset()),
                        "stopping offset is never changed");
                require(fixture.reader.states.get(PARTITION).getCurrentOffset() == test[0],
                        "boundary snapshot keeps live state unchanged");
            }
        }
        try (Fixture empty = new Fixture(true)) {
            require(empty.reader.snapshotState(99).isEmpty(), "finished/empty reader stays empty");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final RecordingFetcher fetcher;
        private final Reader reader;

        private Fixture(boolean commits) {
            Configuration configuration = new Configuration();
            configuration.set(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT, commits);
            SourceReaderMetricGroup metrics = UnregisteredMetricsGroup.createSourceReaderMetricGroup();
            SourceReaderContext context = (SourceReaderContext) Proxy.newProxyInstance(
                    SourceReaderContext.class.getClassLoader(), new Class<?>[] {SourceReaderContext.class},
                    (proxy, method, arguments) -> {
                        switch (method.getName()) {
                            case "metricGroup": return metrics;
                            case "getConfiguration": return configuration;
                            default: throw new AssertionError("Unexpected context operation: " + method);
                        }
                    });
            FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> queue =
                    new FutureCompletingBlockingQueue<>();
            fetcher = new RecordingFetcher(queue);
            reader = new Reader(queue, fetcher, configuration, context,
                    new KafkaSourceReaderMetrics(metrics));
        }

        @Override
        public void close() throws Exception {
            reader.close();
        }
    }

    private static final class Reader extends KafkaSourceReader<String> {
        private final Map<TopicPartition, KafkaPartitionSplitState> states = new HashMap<>();

        private Reader(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> queue,
                RecordingFetcher fetcher, Configuration configuration,
                SourceReaderContext context, KafkaSourceReaderMetrics metrics) {
            super(queue, fetcher, (record, output, state) -> {
                throw new AssertionError("No records should be fetched by this check");
            }, configuration, context, metrics);
        }

        @Override
        protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
            KafkaPartitionSplitState state = super.initializedState(split);
            states.put(split.getTopicPartition(), state);
            return state;
        }
    }

    private static final class RecordingFetcher extends KafkaSourceFetcherManager {
        private List<KafkaPartitionSplit> assigned = List.of();
        private Map<TopicPartition, OffsetAndMetadata> committed;

        private RecordingFetcher(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> queue) {
            super(queue, () -> { throw new AssertionError("No Kafka client may be created"); }, ignored -> {});
        }

        @Override
        public void addSplits(List<KafkaPartitionSplit> splits) {
            // Exercise reader state/restore while keeping the I/O side a recording boundary.
            assigned = List.copyOf(splits);
        }

        @Override
        public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            committed = Map.copyOf(offsets);
        }
    }
}
