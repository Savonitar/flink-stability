package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.core.validation.kafka.KafkaClientStateSnapshotReader;
import org.savonitar.flink.stability.core.validation.kafka.KafkaStatePrefixValidator;
import org.savonitar.flink.stability.core.validation.kafka.KafkaStateSnapshotReader;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/** Observes a committed prefix and checkpoint state before allowing a keyed-workload kill. */
final class StateRecoveryGate {
    static final Duration TIMEOUT = Duration.ofSeconds(45);
    private final FlinkScenarioControl flink;
    private final V1AttemptRuntime runtime;
    private final KafkaInputManifest manifest;
    private final String bootstrap;
    private final String topic;
    private final int keys;
    private final KafkaStateSnapshotReader reader;
    private final LongSupplier nanoTime;
    private final PhaseSleeper sleeper;
    private final List<StateRecoveryEvidence.Readiness> attempts = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();

    StateRecoveryGate(FlinkScenarioControl flink, V1AttemptRuntime runtime, KafkaInputManifest manifest,
            String bootstrap, String topic, int keys) {
        this(flink, runtime, manifest, bootstrap, topic, keys, new KafkaClientStateSnapshotReader(),
                System::nanoTime, ExecutablePhaseExecutor::sleep);
    }

    StateRecoveryGate(FlinkScenarioControl flink, V1AttemptRuntime runtime, KafkaInputManifest manifest,
            String bootstrap, String topic, int keys, KafkaStateSnapshotReader reader,
            LongSupplier nanoTime, PhaseSleeper sleeper) {
        this.flink = flink; this.runtime = runtime; this.manifest = manifest;
        this.bootstrap = bootstrap; this.topic = topic; this.keys = keys;
        this.reader = reader; this.nanoTime = nanoTime; this.sleeper = sleeper;
    }

    Optional<StateRecoveryEvidence.Readiness> awaitReady(FlinkJobHandle job, String target) throws Exception {
        return awaitReady(job, target, TIMEOUT);
    }

    Optional<StateRecoveryEvidence.Readiness> awaitReady(FlinkJobHandle job, String target, Duration budget) throws Exception {
        var deadline = MonotonicDeadline.start(budget.compareTo(TIMEOUT) < 0 ? budget : TIMEOUT, nanoTime);
        StateRecoveryEvidence.Readiness latest = null;
        StateRecoveryEvidence.CommittedPrefix prefix = null;
        try {
        while (!deadline.remaining().isZero()) {
            // A completely traversed committed prefix is durable evidence. Reuse that fixed
            // boundary while polling fresh checkpoints; consumer startup can span an interval.
            if (prefix == null || !prefix.validStatePrefix() || prefix.observedRecords() < keys)
                prefix = prefix(deadline);
            var identity = runtime.taskManagerIdentity(target, remaining(deadline))
                    .orElseThrow(() -> new IOException("TaskManager identity unavailable before stateful kill"));
            var checkpoints = flink.checkpointOverview(job, remaining(deadline));
            var observation = flink.observe(job, remaining(deadline));
            var trace = StateRecoveryEvidence.collect(runtime.flinkProvisioningEvidence(), runtime.flinkComponentLogs(), job.jobId());
            latest = StateRecoveryEvidence.ready(job.jobId(), keys, manifest.totalRecords(), identity,
                    observation, checkpoints, trace, prefix);
            remaining(deadline);
            if (latest.ready() || observation.state().terminal()) break;
            sleeper.sleep(deadline.remaining().compareTo(Duration.ofMillis(100)) < 0
                    ? deadline.remaining() : Duration.ofMillis(100));
        }
        return Optional.ofNullable(latest);
        } finally {
            if (latest != null) attempts.add(latest);
        }
    }

    private StateRecoveryEvidence.CommittedPrefix prefix(MonotonicDeadline deadline) throws Exception {
        var snapshot = reader.read(bootstrap, topic, remaining(deadline),
                ExecutableScenarioPlan.FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS,
                KafkaStatePrefixValidator.MAX_RETAINED_BYTES);
        var verified = new KafkaStatePrefixValidator((b, t, timeout, records, bytes) -> snapshot)
                .validate(bootstrap, topic, manifest, keys, remaining(deadline));
        var evidence = verified.evidence();
        long count = evidence.observedCount();
        boolean valid = evidence.snapshotComplete() && evidence.defectTotals().filter(totals ->
                totals.malformedCount() == 0 && totals.unexpectedCount() == 0 && totals.duplicateCount() == 0
                        && totals.distinctExpectedCount() == count && totals.missingCount() == manifest.totalRecords() - count)
                .isPresent() && verified.stateEvidence().mismatchCount().orElse(-1) == 0
                && count < manifest.totalRecords() && !evidence.missingSamples().isEmpty()
                && evidence.missingSamples().getFirst() == count;
        // With all IDs unique/in-range, smallest missing ID == count proves the exact prefix [0,count).
        Map<Integer, Long> perKey = new TreeMap<>();
        if (valid) for (int key = 0; key < keys; key++) perKey.put(key, count <= key ? 0 : (count - 1 - key) / keys + 1);
        return new StateRecoveryEvidence.CommittedPrefix(evidence.snapshotComplete(), valid, count,
                valid ? count - 1 : -1, perKey, evidence.beginningOffsets(), evidence.endOffsets());
    }

    private static Duration remaining(MonotonicDeadline deadline) throws IOException {
        return deadline.remainingOrThrow(() -> new IOException("State recovery readiness deadline expired"));
    }

    void recordFailure(Exception failure) {
        if (failures.size() < 100) failures.add(failure.getClass().getSimpleName() + ": " + failure.getMessage());
    }
    List<StateRecoveryEvidence.Readiness> attempts() { return List.copyOf(attempts); }
    List<String> failures() { return List.copyOf(failures); }
}
