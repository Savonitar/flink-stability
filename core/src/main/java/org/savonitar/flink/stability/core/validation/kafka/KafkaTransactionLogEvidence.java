package org.savonitar.flink.stability.core.validation.kafka;

import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Physical observations joined by producer identity; neither visibility nor responsibility is inferred. */
public final class KafkaTransactionLogEvidence {
    public static final String TOPIC = "__transaction_state";
    public record Source(String topic, int partition, String file) {}
    public record Coordinator(Source source, long offset, KafkaTransactionStateDecoder.Result record) {}
    public record Marker(Source source, long offset, String type, int coordinatorEpoch) {}
    public record Chain(String transactionalId, long producerId, int producerEpoch,
                        List<Marker> markers, Coordinator latestCoordinator) {}
    public record Summary(String scope, boolean decoded, List<Coordinator> coordinatorRecords,
                          List<Chain> chains, List<String> diagnostics) {}
    private record Identity(long producerId, int producerEpoch) {}
    private final List<Coordinator> records = new ArrayList<>();
    private final Map<Identity, List<Marker>> markers = new LinkedHashMap<>();
    private final Map<String, Map<Identity, Boolean>> identities = new LinkedHashMap<>();
    private final Map<String, Coordinator> latest = new LinkedHashMap<>();
    private final List<String> diagnostics = new ArrayList<>();
    private int remainingRecords = KafkaLogSegmentDecoder.MAX_RECORDS;

    public void add(Source source, KafkaLogSegmentDecoder.Segment segment, MonotonicDeadline deadline) {
        if (segment.recordCount() > remainingRecords) throw new IllegalArgumentException("Shared transaction evidence record budget exhausted");
        remainingRecords -= Math.toIntExact(segment.recordCount());
        var decoder = new KafkaTransactionStateDecoder();
        for (var batch : segment.batches()) {
            check(deadline);
            var identity = new Identity(batch.producerId(), batch.producerEpoch());
            for (var entry : batch.records()) {
                check(deadline);
                if (TOPIC.equals(source.topic())) {
                    if (batch.control()) throw new IllegalArgumentException("Control batch in coordinator log");
                    var result = decoder.decode(bytes(entry.keyBase64()), bytes(entry.valueBase64()),
                            KafkaTransactionStateDecoder.Limits.bounded(), deadline);
                    var observation = new Coordinator(source, entry.offset(), result);
                    records.add(observation);
                    if (result.status() != KafkaTransactionStateDecoder.Status.DECODED
                            && result.status() != KafkaTransactionStateDecoder.Status.TOMBSTONE) {
                        diagnostics.add(source.file() + "@" + entry.offset() + ": " + result.status());
                    }
                    if (result.transactionalId() == null) continue;
                    String id = result.transactionalId();
                    var previous = latest.get(id);
                    if (previous != null && previous.source().partition() != source.partition()) {
                        diagnostics.add("Transactional ID observed in different coordinator partitions: " + id);
                    } else if (previous == null || entry.offset() > previous.offset()) {
                        latest.put(id, observation);
                    } else if (entry.offset() == previous.offset() && !previous.record().equals(result)) {
                        diagnostics.add("Conflicting coordinator record at the same offset: " + id);
                    }
                    // Unknown interpretations cannot supply identity edges.
                    if (result.knownSemantics()) {
                        var value = result.decoded();
                        identities.computeIfAbsent(id, ignored -> new LinkedHashMap<>())
                                .put(new Identity(value.producerId(), value.producerEpoch()), true);
                    }
                } else if (batch.transactional() || batch.control()) {
                    var observed = markers.computeIfAbsent(identity, ignored -> new ArrayList<>());
                    if (entry.marker() != null) observed.add(new Marker(source, entry.offset(),
                            entry.marker().type(), entry.marker().coordinatorEpoch()));
                }
            }
        }
    }

    public Summary summary() {
        var chains = new ArrayList<Chain>();
        var associated = new java.util.HashSet<Identity>();
        identities.forEach((id, values) -> values.keySet().forEach(identity -> {
            associated.add(identity);
            chains.add(new Chain(id, identity.producerId(), identity.producerEpoch(),
                    List.copyOf(markers.getOrDefault(identity, List.of())), latest.get(id)));
        }));
        markers.forEach((identity, values) -> {
            if (!associated.contains(identity)) chains.add(new Chain(null, identity.producerId(),
                    identity.producerEpoch(), List.copyOf(values), null));
        });
        return new Summary("Retained physical records only. Latest means greatest observed coordinator offset per ID; "
                + "compaction, capture gaps and elapsed time may hide history. No visibility or fault attribution.",
                diagnostics.isEmpty(), List.copyOf(records), List.copyOf(chains), List.copyOf(diagnostics));
    }

    private static byte[] bytes(String encoded) {
        return encoded == null ? null : Base64.getDecoder().decode(encoded);
    }
    private static void check(MonotonicDeadline deadline) {
        if (Thread.currentThread().isInterrupted() || deadline.remaining().isZero())
            throw new IllegalArgumentException("Shared transaction evidence deadline expired or interrupted");
    }
}
