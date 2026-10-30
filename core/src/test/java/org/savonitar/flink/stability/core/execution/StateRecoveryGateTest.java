package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.core.validation.kafka.KafkaSnapshotException;
import org.savonitar.flink.stability.core.validation.kafka.KafkaStateSnapshotReader;
import org.savonitar.flink.stability.core.validation.kafka.KafkaStateTopicSnapshot;
import org.savonitar.flink.stability.runtime.api.Digests;
import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.TaskManagerControl;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class StateRecoveryGateTest {
    private static final String JOB = "a".repeat(32);
    private static final String PROCESS = "taskmanager-1#0";
    private static final FlinkJobHandle HANDLE = new FlinkJobHandle(JOB);
    @TempDir Path temporary;

    @Test
    void completeTraversalOfValidPrefixMayHaveMissingTerminalIdsAndStillArmReadiness() throws Exception {
        var calls = new ArrayList<String>();
        var gate = gate(snapshot(List.of(row(2), row(0), row(3), row(1))), new AtomicLong(), calls);
        var readiness = gate.awaitReady(HANDLE, "taskmanager-1").orElseThrow();
        assertTrue(readiness.ready(), readiness.reason());
        assertTrue(readiness.committedPrefix().validStatePrefix());
        assertEquals(4, readiness.committedPrefix().observedRecords());
        assertEquals(3, readiness.committedPrefix().maximumId());
        assertEquals(Map.of(0, 2L, 1, 2L), readiness.committedPrefix().perKeyCounts());
        assertEquals(List.of("taskManagerIdentity", "captureRawEvidence", "checkpointOverview", "latestCallEvidence", "observe",
                "flinkProvisioningEvidence", "flinkComponentLogs"), calls);
        assertEquals(List.of(readiness), gate.attempts());
    }

    @Test
    void retainsFixedCommittedBoundaryWhileWaitingForInFlightCheckpointToFinish() throws Exception {
        var reads = new AtomicInteger();
        var calls = new ArrayList<String>();
        var gate = gate((bootstrap, topic, timeout, records, bytes) -> {
            if (reads.incrementAndGet() > 1) throw new AssertionError("A second consumer read would miss the window");
            return snapshot(List.of(row(0), row(1), row(2), row(3)));
        }, new AtomicLong(), calls, 1);
        var readiness = gate.awaitReady(HANDLE, "taskmanager-1").orElseThrow();
        assertTrue(readiness.ready(), readiness.reason());
        assertEquals(1, reads.get());
        assertEquals(2, calls.stream().filter("checkpointOverview"::equals).count());
        assertEquals(Map.of(0, 0L), readiness.committedPrefix().beginningOffsets());
        assertEquals(Map.of(0, 4L), readiness.committedPrefix().endOffsets());
    }

    @Test
    void rejectsHolesDuplicatesWrongStateMalformedAndUnexpectedRecords() throws Exception {
        for (List<String> rows : List.of(
                List.of(row(0), row(1), row(3), row(4)),
                List.of(row(0), row(1), row(2), row(2)),
                List.of(row(0), row(1), row(2).replace("\"prefixSum\":4", "\"prefixSum\":3"), row(3)),
                List.of(row(0), row(1), "{}", row(3)),
                List.of(row(0), row(1), row(2), row(3).replace("\"id\":3", "\"id\":30")))) {
            var gate = gate(snapshot(rows), new AtomicLong(), new ArrayList<>());
            var readiness = gate.awaitReady(HANDLE, "taskmanager-1").orElseThrow();
            assertFalse(readiness.ready(), rows.toString());
            assertFalse(readiness.committedPrefix().validStatePrefix(), rows.toString());
            assertEquals("state-recovery.committed-prefix-unconfirmed", readiness.reason());
        }
    }

    @Test
    void requiresEveryKeyAndEnoughUnconsumedSuffixEvenForValidState() throws Exception {
        for (List<String> rows : List.of(List.of(row(0)), IntStream.range(0, 9).mapToObj(StateRecoveryGateTest::row).toList(),
                IntStream.range(0, 10).mapToObj(StateRecoveryGateTest::row).toList())) {
            var readiness = gate(snapshot(rows), new AtomicLong(), new ArrayList<>())
                    .awaitReady(HANDLE, "taskmanager-1").orElseThrow();
            assertFalse(readiness.ready());
        }
    }

    @Test
    void incompleteSnapshotCannotBecomeAReadinessAttempt() throws Exception {
        var calls = new ArrayList<String>();
        var gate = gate((bootstrap, topic, timeout, records, bytes) -> {
            throw new KafkaSnapshotException("verification.kafka.incomplete-after-timeout", "partial traversal");
        }, new AtomicLong(), calls);
        var failure = assertThrows(KafkaSnapshotException.class, () -> gate.awaitReady(HANDLE, "taskmanager-1"));
        assertEquals("verification.kafka.incomplete-after-timeout", failure.reason());
        assertTrue(gate.attempts().isEmpty());
        assertTrue(calls.isEmpty());
    }

    @Test
    void oneDeadlineIncludesPrefixComparisonAndDoesNotGrantFreshControlBudget() throws Exception {
        var clock = new AtomicLong();
        var calls = new ArrayList<String>();
        var reads = new AtomicInteger();
        var gate = gate((bootstrap, topic, timeout, records, bytes) -> {
            reads.incrementAndGet();
            assertEquals(StateRecoveryGate.TIMEOUT, timeout);
            clock.addAndGet(StateRecoveryGate.TIMEOUT.toNanos() + 1);
            return snapshot(List.of(row(0), row(1), row(2), row(3)));
        }, clock, calls);
        assertThrows(IOException.class, () -> gate.awaitReady(HANDLE, "taskmanager-1"));
        assertEquals(1, reads.get());
        assertTrue(calls.isEmpty());
        assertTrue(gate.attempts().isEmpty());
    }

    @Test
    void missingProgressIsRetainedAsUnreadyAndNoRuntimeMutationIsPossible() throws Exception {
        var clock = new AtomicLong();
        var calls = new ArrayList<String>();
        var gate = gate(snapshot(List.of(row(0), row(1), row(2), row(3))), clock, calls);
        Files.writeString(temporary.resolve("process.log"), "unrelated process line\n");
        var readiness = gate.awaitReady(HANDLE, "taskmanager-1").orElseThrow();
        assertFalse(readiness.ready());
        assertEquals("state-recovery.execution-unconfirmed", readiness.reason());
        assertEquals(1, gate.attempts().size());
        assertTrue(clock.get() >= StateRecoveryGate.TIMEOUT.toNanos());
        assertTrue(calls.stream().noneMatch(call -> call.startsWith("kill") || call.startsWith("restart")));
    }

    private StateRecoveryGate gate(KafkaStateTopicSnapshot snapshot, AtomicLong clock, List<String> calls) throws Exception {
        return gate((bootstrap, topic, timeout, records, bytes) -> snapshot, clock, calls);
    }

    private StateRecoveryGate gate(KafkaStateSnapshotReader reader, AtomicLong clock, List<String> calls) throws Exception {
        return gate(reader, clock, calls, 0);
    }

    private StateRecoveryGate gate(KafkaStateSnapshotReader reader, AtomicLong clock, List<String> calls,
                                   int inFlightPolls) throws Exception {
        var remainingInFlight = new AtomicInteger(inFlightPolls);
        Path log = temporary.resolve("process.log");
        Files.writeString(log, marker("START", -1, "buildMode=correct")
                + marker("SNAPSHOT", 3, "keyCount=2 observedKeys=2 minimumObservedCount=2 processedThisAttempt=4 maximumInputId=3")
                + marker("COMPLETED", 3, ""));
        var component = FlinkComponentProvisioningEvidence.verified("taskmanager-1", FlinkComponentRole.TASK_MANAGER,
                "runtime-1", "flink:2.2.0", "sha256:" + "1".repeat(64), "2".repeat(64), "3".repeat(64), List.of())
                .withProcessConfiguration(Map.of(), PROCESS);
        var runtime = (V1AttemptRuntime) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{V1AttemptRuntime.class}, (proxy, method, args) -> {
                    calls.add(method.getName());
                    return switch (method.getName()) {
                        case "taskManagerIdentity" -> Optional.of(new TaskManagerControl.Identity("taskmanager-1", "runtime-1", "resource-1"));
                        case "flinkProvisioningEvidence" -> List.of(component);
                        case "flinkComponentLogs" -> List.of(new FlinkComponentLog(PROCESS, log, false, Optional.empty()));
                        default -> throw new AssertionError("Gate must never call runtime mutation: " + method.getName());
                    };
                });
        var checkpoints = new ObjectMapper().readTree("""
                {"latest":{"completed":{"id":3,"status":"COMPLETED","latest_ack_timestamp":900}}}
                """);
        var observation = new FlinkJobObservation(1000, FlinkJobState.RUNNING, 1, 0, Optional.empty(), List.of(),
                List.of(new FlinkJobObservation.Subtask("Keyed Prefix v1 -> Kafka Sink: Writer", 0, 0,
                        "RUNNING", Optional.of("resource-1"))));
        var flink = (FlinkScenarioControl) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{FlinkScenarioControl.class}, (proxy, method, args) -> {
                    calls.add(method.getName());
                    return switch (method.getName()) {
                        case "captureRawEvidence" -> FlinkScenarioControl.RawEvidenceCapture.NONE;
                        case "latestCallEvidence" -> List.of();
                        case "checkpointOverview" -> {
                            var current = checkpoints.deepCopy();
                            ((com.fasterxml.jackson.databind.node.ObjectNode) current).putObject("counts")
                                    .put("in_progress", remainingInFlight.getAndDecrement() > 0 ? 1 : 0);
                            yield current;
                        }
                        case "observe" -> observation;
                        default -> throw new AssertionError("Unexpected Flink operation: " + method.getName());
                    };
                });
        return new StateRecoveryGate(flink, runtime, manifest(), "kafka:9092", "output", 2,
                reader, clock::get, ignored -> clock.addAndGet(inFlightPolls > 0
                        ? java.time.Duration.ofMillis(100).toNanos() : StateRecoveryGate.TIMEOUT.toNanos()));
    }

    private static KafkaInputManifest manifest() {
        var records = IntStream.range(0, 10).mapToObj(id -> new KafkaInputManifest.RecordAcknowledgement(id,
                Digests.sha256(Integer.toString(id)), List.of(new KafkaInputManifest.ProducerAttempt(1, 0, 0, id,
                KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED)), 0, id,
                KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED, KafkaInputManifest.TerminalDisposition.PRESENT)).toList();
        return new KafkaInputManifest("main", "input", 10, records, Map.of(0, 0L), Map.of(0, 10L),
                KafkaInputManifest.EvidenceStatus.COMPLETE, new KafkaInputManifest.ReconciliationEvidence(
                Map.of("isolation.level", "read_uncommitted"), IntStream.range(0, 10).mapToObj(id -> (long) id).toList(), true));
    }

    private static KafkaStateTopicSnapshot snapshot(List<String> rows) {
        return new KafkaStateTopicSnapshot("output", Map.of(0, 0L), Map.of(0, (long) rows.size()),
                IntStream.range(0, rows.size()).mapToObj(offset -> new KafkaStateTopicSnapshot.ObservedRecord(
                        0, offset, rows.get(offset).getBytes(StandardCharsets.UTF_8))).toList());
    }

    private static String row(int id) {
        long[] sums = {1, 2, 4, 6, 9, 12, 16, 20, 25, 30};
        return "{\"version\":1,\"id\":" + id + ",\"key\":" + id % 2
                + ",\"count\":" + (id / 2 + 1) + ",\"prefixSum\":" + sums[id] + "}";
    }

    private static String marker(String event, long checkpoint, String fields) {
        return "STATE_PREFIX_" + event + " version=1 jobId=" + JOB + " operatorUid=" + StateRecoveryEvidence.OPERATOR_UID
                + " attempt=0 subtask=0 checkpoint=" + checkpoint + " restoredCheckpoint=-1"
                + (fields.isEmpty() ? "" : " " + fields) + "\n";
    }
}
