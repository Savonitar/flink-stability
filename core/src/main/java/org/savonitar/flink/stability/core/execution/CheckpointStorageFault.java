package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkCheckpointEvidence;
import org.savonitar.flink.stability.core.flink.FlinkCheckpointTrigger;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import org.savonitar.flink.stability.runtime.api.RawEvidenceRef;
import org.savonitar.flink.stability.runtime.api.StorageFaultControl;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.LongSupplier;

/** One bounded write-EIO / physical replacement / state-recovery experiment. */
public final class CheckpointStorageFault {
    public static final String UNCONFIRMED = "checkpoint-storage.effect-unconfirmed";
    private static final String TARGET = "taskmanager-1";
    public enum Stage { BEFORE_ARM, ARMED, FAILED_CHECKPOINT, STOPPED, HEALED, RECOVERED }

    static String verifiedCheckpointUri(StorageFaultControl.Snapshot storage) throws IOException {
        try {
            require(storage.complete() && storage.namespace().matches("job_[a-zA-Z0-9_]+")
                    && java.net.URI.create("file:/flink/checkpoints/" + storage.namespace())
                        .equals(java.net.URI.create(storage.checkpointUri())), "Generated FUSE checkpoint namespace is unconfirmed");
            return storage.checkpointUri();
        } catch (IllegalArgumentException malformed) {
            throw new IOException("Generated FUSE checkpoint URI is malformed", malformed);
        }
    }

    public record Sample(Stage stage, long sequence, long elapsedNanos,
                         StorageFaultControl.Snapshot storage, FlinkCheckpointEvidence.History checkpoints,
                         FlinkJobObservation job) {}

    public record Evidence(String path, String jobId, String triggerId, long timeoutMillis,
                           long checkpointId, long failedCheckpointId,
                           FlinkCheckpointEvidence.Configuration configuration,
                           StateRecoveryEvidence.Readiness readiness,
                           StorageFaultControl.Materialization materialization,
                           FlinkCheckpointTrigger.Observation triggerResult,
                           FlinkCheckpointEvidence.Checkpoint failedCheckpoint,
                           StateRecoveryEvidence.Trace failureTrace,
                           StateRecoveryEvidence.Trace recoveryTrace,
                           List<Sample> samples, List<StorageFaultControl.Transition> transitions,
                           PhaseExecutionEvidence.TaskManagerKill kill,
                           PhaseExecutionEvidence.TaskManagerRestart restart,
                           StateRecoveryEvidence.Recovery recovery, List<String> errors,
                           ExecutableScenarioPlan.CheckpointControls declaredControls,
                           List<RawEvidenceRef> triggerSubmitEvidence, List<RawEvidenceRef> beforeKillEvidence,
                           List<RawEvidenceRef> afterKillEvidence) {
        public Evidence {
            java.util.Objects.requireNonNull(declaredControls, "declaredControls");
            triggerSubmitEvidence = List.copyOf(triggerSubmitEvidence);
            beforeKillEvidence = List.copyOf(beforeKillEvidence); afterKillEvidence = List.copyOf(afterKillEvidence);
            samples = List.copyOf(samples); transitions = List.copyOf(transitions); errors = List.copyOf(errors);
            if (samples.size() > 6 || transitions.size() > 3 || errors.size() > 20)
                throw new IllegalArgumentException("Storage experiment evidence limit exceeded");
        }
        public Evidence(String path, String jobId, String triggerId, long timeoutMillis,
                        long checkpointId, long failedCheckpointId, FlinkCheckpointEvidence.Configuration configuration,
                        StateRecoveryEvidence.Readiness readiness, StorageFaultControl.Materialization materialization,
                        FlinkCheckpointTrigger.Observation triggerResult, FlinkCheckpointEvidence.Checkpoint failedCheckpoint,
                        StateRecoveryEvidence.Trace failureTrace, StateRecoveryEvidence.Trace recoveryTrace,
                        List<Sample> samples, List<StorageFaultControl.Transition> transitions,
                        PhaseExecutionEvidence.TaskManagerKill kill, PhaseExecutionEvidence.TaskManagerRestart restart,
                        StateRecoveryEvidence.Recovery recovery, List<String> errors,
                        ExecutableScenarioPlan.CheckpointControls declaredControls) {
            this(path, jobId, triggerId, timeoutMillis, checkpointId, failedCheckpointId, configuration,
                    readiness, materialization, triggerResult, failedCheckpoint, failureTrace, recoveryTrace,
                    samples, transitions, kill, restart, recovery, errors, declaredControls, List.of(), List.of(), List.of());
        }
        public Evidence(String path, String jobId, String triggerId, long timeoutMillis,
                        long checkpointId, long failedCheckpointId, FlinkCheckpointEvidence.Configuration configuration,
                        StateRecoveryEvidence.Readiness readiness, StorageFaultControl.Materialization materialization,
                        FlinkCheckpointTrigger.Observation triggerResult, FlinkCheckpointEvidence.Checkpoint failedCheckpoint,
                        StateRecoveryEvidence.Trace failureTrace, StateRecoveryEvidence.Trace recoveryTrace,
                        List<Sample> samples, List<StorageFaultControl.Transition> transitions,
                        PhaseExecutionEvidence.TaskManagerKill kill, PhaseExecutionEvidence.TaskManagerRestart restart,
                        StateRecoveryEvidence.Recovery recovery, List<String> errors) {
            this(path, jobId, triggerId, timeoutMillis, checkpointId, failedCheckpointId, configuration,
                    readiness, materialization, triggerResult, failedCheckpoint, failureTrace, recoveryTrace,
                    samples, transitions, kill, restart, recovery, errors,
                    new ExecutableScenarioPlan.CheckpointControls(0, 1, 10));
        }
        public boolean confirmed() {
            try { validate(this); return true; }
            catch (IOException | RuntimeException incomplete) { return false; }
        }
    }

    @FunctionalInterface
    interface ReadinessGate {
        Optional<StateRecoveryEvidence.Readiness> await(FlinkJobHandle job, String target, Duration timeout) throws Exception;
    }

    private final FlinkScenarioControl flink;
    private final V1AttemptRuntime runtime;
    private final ReadinessGate gate;
    private final LongSupplier clock;
    private final PhaseSleeper sleeper;

    CheckpointStorageFault(FlinkScenarioControl flink, V1AttemptRuntime runtime, ReadinessGate gate,
                           LongSupplier clock, PhaseSleeper sleeper) {
        this.flink = flink; this.runtime = runtime; this.gate = gate; this.clock = clock; this.sleeper = sleeper;
    }

    Evidence execute(String path, List<PhaseExecutionEvidence.LoopIteration> iterations, FlinkJobHandle job,
                     ExecutableScenarioPlan.CheckpointStorageFault request) {
        return execute(path, iterations, job, request, new ExecutableScenarioPlan.CheckpointControls(0, 1, 10));
    }

    Evidence execute(String path, List<PhaseExecutionEvidence.LoopIteration> iterations, FlinkJobHandle job,
                     ExecutableScenarioPlan.CheckpointStorageFault request,
                     ExecutableScenarioPlan.CheckpointControls declaredControls) {
        Recorder r = new Recorder(path, job.jobId(), request.timeout().toMillis(), declaredControls);
        MonotonicDeadline deadline = MonotonicDeadline.start(request.timeout(), clock);
        long started = clock.getAsLong();
        boolean armAttempted = false, healed = false;
        try {
            require(gate != null, "State readiness gate is unavailable");
            r.readiness = gate.await(job, TARGET, left(deadline)).orElseThrow(() -> new IOException("No readiness observation"));
            require(r.readiness.ready(), "State readiness unconfirmed: " + r.readiness.reason());
            r.configuration = flink.checkpointConfiguration(job, left(deadline));
            validateConfiguration(r.configuration, job.jobId(), declaredControls);
            r.checkpoint = r.readiness.completedSnapshots().keySet().stream().mapToLong(Long::longValue).max().orElseThrow();
            r.materialization = runtime.inspectCheckpoint(job.jobId(), r.checkpoint, left(deadline));
            Sample before = sample(r, Stage.BEFORE_ARM, job, deadline, started);
            validateBaseline(r.readiness, r.checkpoint, before);
            validateMaterialization(r.materialization, before.storage(), r.readiness, r.checkpoint);

            armAttempted = true; // An acknowledgement can be lost after the command took effect.
            r.transitions.add(runtime.armStorage(r.triggerId, 1, request.lease(), left(deadline)));
            Sample armed = sample(r, Stage.ARMED, job, deadline, started);
            require(sameCheckpointBoundary(before.checkpoints(), armed.checkpoints()), "Checkpoint changed while arming storage");
            requireArmed(armed.storage(), r.triggerId, r.readiness.target().runtimeId(), false);
            validateTransition(r.transitions.getFirst(), r.triggerId, 1, before.storage(), armed.storage(), false);
            require(sameExecution(r.readiness, armed.job()), "Execution changed before checkpoint trigger");

            // Exactly one submission, even if its response is lost. A failed trigger has no checkpoint ID.
            require(flink.triggerCheckpoint(job, r.triggerId, left(deadline)).equals(r.triggerId), "Checkpoint trigger acknowledgement mismatch");
            r.triggerSubmitEvidence = flink.latestCallEvidence();
            do {
                r.triggerResult = flink.checkpointStatus(job, r.triggerId, left(deadline));
                if (r.triggerResult.state() == FlinkCheckpointTrigger.State.IN_PROGRESS) pause(deadline);
            } while (r.triggerResult.state() == FlinkCheckpointTrigger.State.IN_PROGRESS);
            // The asynchronous trigger and checkpoint statistics have separate REST
            // publication/cache boundaries. A completed trigger can precede its failed
            // history entry. Wait only for the unchanged or sole pending candidate;
            // all competing/completed/restored changes still go to strict correlation.
            while (pendingFailureHistory(armed.checkpoints(), flink.checkpointHistory(job, left(deadline)))) pause(deadline);
            Sample failed = sample(r, Stage.FAILED_CHECKPOINT, job, deadline, started);
            r.failedCheckpoint = correlate(armed.checkpoints(), failed.checkpoints(), r.triggerResult);
            r.failedId = r.failedCheckpoint.id();
            FlinkCheckpointEvidence.Checkpoint details = flink.checkpointDetails(job, r.failedId, left(deadline));
            require(details.id() == r.failedId && details.status() == FlinkCheckpointEvidence.Status.FAILED
                    && details.triggerTimestamp() == r.failedCheckpoint.triggerTimestamp(), "Failed checkpoint details contradict history");
            r.failedCheckpoint = details;
            // Docker log delivery can trail REST completion. Wait for the receipt under
            // the same budget, then recheck the whole failure window before killing.
            do {
                r.failureTrace = StateRecoveryEvidence.collect(runtime.flinkProvisioningEvidence(), runtime.flinkComponentLogs(), job.jobId());
                if (r.failureTrace.complete() && r.failureTrace.markers().stream().anyMatch(marker ->
                        marker.event() == StateRecoveryEvidence.Event.SNAPSHOT && marker.checkpointId() == r.failedId)) break;
                pause(deadline);
            } while (true);
            var failedJob = flink.observe(job, left(deadline));
            var failedHistory = flink.checkpointHistory(job, left(deadline));
            var failedStorage = runtime.storageSnapshot(left(deadline));
            left(deadline);
            require(sameCheckpointBoundary(failed.checkpoints(), failedHistory), "Checkpoint changed while awaiting failed snapshot receipt");
            failed = new Sample(Stage.FAILED_CHECKPOINT, failed.sequence(), clock.getAsLong() - started,
                    failedStorage, failedHistory, failedJob);
            r.samples.set(2, failed);
            validateFailure(r.snapshot(), failed);
            require(sameExecution(r.readiness, failed.job()), "Unexpected failure/restart before the deliberate TaskManager kill");

            long beforeKill = flink.jobManagerTimeMillis(job, left(deadline));
            r.beforeKillEvidence = flink.latestCallEvidence();
            runtime.killTaskManager(TARGET, left(deadline));
            long afterKill = flink.jobManagerTimeMillis(job, left(deadline));
            r.afterKillEvidence = flink.latestCallEvidence();
            r.kill = new PhaseExecutionEvidence.TaskManagerKill(path, iterations, TARGET,
                    new FlinkJobObservation.Attempt(Optional.of(failed.job()), Optional.empty()),
                    OptionalLong.of(beforeKill), OptionalLong.of(afterKill), Optional.of(r.readiness.target()),
                    Optional.empty(), Optional.empty(), Optional.of(r.readiness));
            FlinkJobObservation stopped;
            do {
                stopped = flink.observe(job, left(deadline));
                if (stopped.activeSubtasks().isEmpty()
                        && flink.checkpointHistory(job, left(deadline)).counts().inProgress() == 0) break;
                pause(deadline);
            } while (true);
            Sample dead = sample(r, Stage.STOPPED, job, deadline, started, stopped);
            require(dead.job().activeSubtasks().isEmpty(), "Tasks remain active before healing");
            require(afterKillBoundary(failed.checkpoints(), dead.checkpoints(), r.checkpoint, beforeKill), "Checkpoint boundary changed before healing");
            requireArmed(dead.storage(), r.triggerId, r.readiness.target().runtimeId(), true);
            r.transitions.add(runtime.healStorage(r.triggerId, 2, left(deadline)));
            Sample healthy = sample(r, Stage.HEALED, job, deadline, started);
            validateTransition(r.transitions.get(1), r.triggerId, 2, dead.storage(), healthy.storage(), true);
            require(afterKillBoundary(failed.checkpoints(), healthy.checkpoints(), r.checkpoint, beforeKill)
                    && monotonicRestore(dead.checkpoints(), healthy.checkpoints()), "Checkpoint changed across healing");
            requireHealthySurvivor(healthy.storage(), r.readiness.target().runtimeId());
            healed = true;

            runtime.restartTaskManager(TARGET, left(deadline));
            var replacement = runtime.taskManagerIdentity(TARGET, left(deadline));
            r.restart = new PhaseExecutionEvidence.TaskManagerRestart(path, iterations, TARGET,
                    Optional.of(r.readiness.target()), replacement, Optional.empty(), Optional.empty());
            require(replacement.isPresent() && !replacement.orElseThrow().runtimeId().equals(r.readiness.target().runtimeId()),
                    "TaskManager replacement identity is unconfirmed");
            while (true) {
                FlinkJobObservation observed = flink.observe(job, left(deadline));
                var history = flink.checkpointHistory(job, left(deadline));
                var trace = StateRecoveryEvidence.collect(runtime.flinkProvisioningEvidence(), runtime.flinkComponentLogs(), job.jobId());
                r.recoveryTrace = trace;
                var effect = TaskManagerKillEffect.evaluate(List.of(r.kill),
                        Optional.of(new FlinkJobObservation.Attempt(Optional.of(observed), Optional.empty()))).getFirst();
                r.recovery = StateRecoveryEvidence.evaluate(r.readiness, effect, trace);
                if (r.recovery.confirmed() && r.recovery.restoredCheckpointId() == r.checkpoint
                        && subsequentCheckpoint(history, trace, r.recovery, r.failedId)) {
                    r.samples.add(new Sample(Stage.RECOVERED, r.samples.size() + 1, clock.getAsLong() - started,
                            runtime.storageSnapshot(left(deadline)), history, observed));
                    left(deadline);
                    validate(r.snapshot());
                    break;
                }
                require(!observed.state().terminal(), "Job terminated before checkpoint/state recovery proof completed");
                pause(deadline);
            }
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            r.errors.add(failure.getClass().getSimpleName() + ": " + String.valueOf(failure.getMessage()));
            if (armAttempted && !healed) {
                try {
                    // Separate cleanup budget; never heal an aborted/unknown operation into a success.
                    r.transitions.add(runtime.abortStorage(r.triggerId, 2, Duration.ofSeconds(5)));
                } catch (RuntimeException cleanup) { r.errors.add("Abort failed: " + cleanup); }
            }
        }
        return r.snapshot();
    }

    private Sample sample(Recorder r, Stage stage, FlinkJobHandle job, MonotonicDeadline deadline, long started) throws IOException {
        return sample(r, stage, job, deadline, started, flink.observe(job, left(deadline)));
    }

    private Sample sample(Recorder r, Stage stage, FlinkJobHandle job, MonotonicDeadline deadline, long started,
                          FlinkJobObservation observed) throws IOException {
        var history = flink.checkpointHistory(job, left(deadline));
        var storage = runtime.storageSnapshot(left(deadline));
        left(deadline);
        Sample value = new Sample(stage, r.samples.size() + 1, clock.getAsLong() - started, storage, history, observed);
        r.samples.add(value);
        require(history.jobId().equals(job.jobId()), "Checkpoint history belongs to another job");
        require(storage.complete() && storage.diagnostics().isEmpty(), "Storage capture incomplete: " + storage.diagnostics());
        return value;
    }

    /** Counts plus identities expose competing automatic checkpoints even when history is truncated. */
    static boolean pendingFailureHistory(FlinkCheckpointEvidence.History before, FlinkCheckpointEvidence.History after) {
        if (sameCheckpointBoundary(before, after)) return true;
        var candidates = after.retained().stream().filter(value -> value.id() > before.highestObservedId()).toList();
        return before.jobId().equals(after.jobId()) && before.counts().inProgress() == 0 && before.inProgress().isEmpty()
                && after.counts().total() == before.counts().total() + 1 && after.counts().inProgress() == 1
                && after.inProgress().size() == 1 && candidates.size() == 1
                && candidates.getFirst().status() == FlinkCheckpointEvidence.Status.IN_PROGRESS
                && !candidates.getFirst().savepoint() && after.highestObservedId() == candidates.getFirst().id()
                && after.counts().completed() == before.counts().completed() && after.counts().failed() == before.counts().failed()
                && after.counts().restored() == before.counts().restored()
                && before.latestCompleted().equals(after.latestCompleted()) && before.latestFailed().equals(after.latestFailed())
                && before.latestRestored().equals(after.latestRestored());
    }

    /** Correlate only a terminal published history, never an unchanged cached response. */
    static FlinkCheckpointEvidence.Checkpoint correlate(FlinkCheckpointEvidence.History before,
            FlinkCheckpointEvidence.History after, FlinkCheckpointTrigger.Observation trigger) throws IOException {
        require(trigger != null && trigger.state() == FlinkCheckpointTrigger.State.COMPLETED
                && trigger.checkpointId().isEmpty() && trigger.failure().filter(value -> !value.isBlank()).isPresent(),
                "The acknowledged checkpoint trigger did not fail");
        require(before.jobId().equals(after.jobId()) && before.counts().inProgress() == 0 && before.inProgress().isEmpty()
                && after.counts().inProgress() == 0 && after.inProgress().isEmpty()
                && after.counts().total() == Math.addExact(before.counts().total(), 1)
                && after.counts().failed() == Math.addExact(before.counts().failed(), 1)
                && after.counts().completed() == before.counts().completed()
                && after.counts().restored() == before.counts().restored()
                && before.latestCompleted().equals(after.latestCompleted()), "Ambiguous failed checkpoint window");
        var candidates = after.retained().stream().filter(value -> value.id() > before.highestObservedId()).toList();
        require(candidates.size() == 1 && candidates.getFirst().status() == FlinkCheckpointEvidence.Status.FAILED
                && !candidates.getFirst().savepoint() && after.highestObservedId() == candidates.getFirst().id(),
                "Exactly one new failed checkpoint is required");
        return candidates.getFirst();
    }

    private static void validateConfiguration(FlinkCheckpointEvidence.Configuration config, String jobId,
                                              ExecutableScenarioPlan.CheckpointControls declared) throws IOException {
        require(config != null && config.jobId().equals(jobId) && config.mode().equals("exactly_once")
                && config.maxConcurrent() == declared.maxConcurrent()
                && config.tolerableFailedCheckpoints() == declared.tolerableFailures() && !config.unalignedCheckpoints()
                && !config.stateChangelogEnabled(), "Unsupported observed checkpoint configuration");
    }

    private static void validateBaseline(StateRecoveryEvidence.Readiness ready, long checkpoint, Sample before) throws IOException {
        require(ready.ready() && sameExecution(ready, before.job())
                && before.checkpoints().counts().failed() == 0 && before.checkpoints().counts().restored() == 0
                && before.checkpoints().highestObservedId() == checkpoint
                && before.checkpoints().counts().inProgress() == 0 && before.checkpoints().inProgress().isEmpty()
                && before.checkpoints().latestCompleted().filter(value -> value.id() == checkpoint).isPresent(),
                "Latest completed checkpoint changed after readiness");
        var instances = before.storage().instances();
        require(instances.size() == 2 && instances.stream().noneMatch(StorageFaultControl.InstanceEvidence::processDead)
                && instances.stream().filter(value -> value.instance().role() == FlinkComponentRole.JOB_MANAGER).count() == 1
                && instances.stream().filter(value -> value.instance().runtimeId().equals(ready.target().runtimeId())
                        && value.instance().role() == FlinkComponentRole.TASK_MANAGER).count() == 1,
                "Exactly the ready JobManager/TaskManager storage instances are required");
        for (var instance : instances) {
            requireVerified(instance);
            require(instance.status().state() == StorageFaultControl.State.HEALTHY && instance.status().revision() == 0,
                    "Initial storage is not healthy revision zero");
        }
    }

    private static void validateMaterialization(StorageFaultControl.Materialization proof, StorageFaultControl.Snapshot storage,
            StateRecoveryEvidence.Readiness ready, long checkpoint) throws IOException {
        require(proof != null && proof.complete() && proof.diagnostics().isEmpty() && proof.jobId().equals(ready.jobId())
                && proof.checkpointId() == checkpoint && proof.operatorUid().equals(StateRecoveryEvidence.OPERATOR_UID)
                && storage.instances().stream().anyMatch(value -> value.instance().equals(proof.instance())),
                "Keyed-state checkpoint metadata proof is incomplete");
        String prefix = storage.namespace() + "/" + ready.jobId() + "/chk-" + checkpoint + "/";
        require(file(proof.metadata(), prefix + "_metadata") && !proof.files().isEmpty(), "No external keyed-state files in metadata");
        for (var actual : proof.files()) {
            require(relative(actual.path()).startsWith(prefix) && !relative(actual.path()).equals(prefix + "_metadata")
                    && file(actual, actual.path()), "Keyed-state file escapes the completed checkpoint");
            require(storage.instances().stream().filter(value -> value.instance().runtimeId().equals(ready.target().runtimeId()))
                    .flatMap(value -> value.operations().stream()).anyMatch(operation -> operation.operation().equals("write")
                            && relative(operation.path()).equals(relative(actual.path())) && operation.errno() == 0
                            && operation.returnedBytes() > 0 && operation.revision() == 0),
                    "No matching successful TaskManager write for external keyed state");
            require(storage.instances().stream().filter(value -> value.instance().equals(proof.instance()))
                    .flatMap(value -> value.operations().stream()).anyMatch(operation -> operation.operation().equals("read")
                            && relative(operation.path()).equals(relative(actual.path())) && operation.errno() == 0 && operation.returnedBytes() > 0),
                    "Keyed-state inspector did not read the actual FUSE state file");
        }
    }

    private static boolean file(StorageFaultControl.FileEvidence file, String path) {
        return file != null && file.regularFile() && relative(file.path()).equals(relative(path)) && file.size() > 0
                && file.sha256() != null && file.sha256().matches("[0-9a-f]{64}");
    }

    private static void validateFailure(Evidence evidence, Sample failed) throws IOException {
        requireArmed(failed.storage(), evidence.triggerId(), evidence.readiness().target().runtimeId(), false);
        var checkpoint = evidence.failedCheckpoint();
        require(checkpoint != null && checkpoint.id() == evidence.failedCheckpointId()
                && checkpoint.status() == FlinkCheckpointEvidence.Status.FAILED && checkpoint.failureTimestamp().isPresent()
                && hasIoFailure(evidence.triggerResult(), checkpoint), "Checkpoint failure does not retain an I/O cause");
        require(evidence.failureTrace() != null && evidence.failureTrace().complete(), "Failed-checkpoint workload receipts incomplete");
        var baseline = evidence.readiness().completedSnapshots().get(evidence.checkpointId()).snapshot();
        var snapshots = evidence.failureTrace().markers().stream().filter(marker -> marker.event() == StateRecoveryEvidence.Event.SNAPSHOT
                && marker.checkpointId() == checkpoint.id() && marker.jobId().equals(evidence.jobId())
                && marker.executionAttempt() == evidence.readiness().executionAttempt() && sameMarkerExecution(marker, baseline)
                && marker.lineNumber() > baseline.lineNumber() && marker.progress().filter(value ->
                    value.keyCount() == evidence.readiness().keyCount() && value.observedKeys() == evidence.readiness().keyCount()
                        && value.minimumObservedCount() > 0).isPresent()).toList();
        require(snapshots.size() == 1, "Failed checkpoint lacks one bound keyed-state snapshot receipt");
        String prefix = failed.storage().namespace() + "/" + evidence.jobId() + "/chk-" + checkpoint.id() + "/";
        long armedSequence = evidence.transitions().getFirst().acknowledgements().stream()
                .filter(command -> command.instance().runtimeId().equals(evidence.readiness().target().runtimeId()))
                .findFirst().orElseThrow().status().sequence();
        require(failed.storage().instances().stream().filter(value -> value.instance().runtimeId()
                        .equals(evidence.readiness().target().runtimeId())).flatMap(value -> value.operations().stream())
                .anyMatch(operation -> operation.sequence() > armedSequence && operation.errno() == 5 && operation.revision() == 1
                        && operation.state() == StorageFaultControl.State.ARMED && evidence.triggerId().equals(operation.faultId())
                        && List.of("create", "write").contains(operation.operation())
                        && relative(operation.path()).startsWith(prefix)
                        && !relative(operation.path()).equals(prefix + "_metadata")),
                "No real TaskManager checkpoint create/write EIO receipt for the failed checkpoint");
    }

    private static boolean hasIoFailure(FlinkCheckpointTrigger.Observation trigger, FlinkCheckpointEvidence.Checkpoint checkpoint) {
        String text = trigger.failure().orElse("") + "\n" + checkpoint.failureMessage().orElse("");
        return text.contains("Input/output error") || text.contains("Input/output Error");
    }

    private static boolean sameExecution(StateRecoveryEvidence.Readiness ready, FlinkJobObservation observation) {
        return observation.state() == FlinkJobState.RUNNING
                && observation.restoredCheckpoints() == ready.jobObservation().restoredCheckpoints()
                && observation.failures().equals(ready.jobObservation().failures())
                && observation.subtasks().stream().anyMatch(task -> task.vertexName().contains("Keyed Prefix v1")
                    && task.index() == 0 && task.attempt() == ready.executionAttempt() && task.status().equals("RUNNING")
                    && task.taskManagerId().filter(ready.target().resourceId()::equals).isPresent());
    }

    private static boolean sameCheckpointBoundary(FlinkCheckpointEvidence.History before, FlinkCheckpointEvidence.History after) {
        return before.jobId().equals(after.jobId()) && before.counts().equals(after.counts())
                && before.highestObservedId() == after.highestObservedId()
                && before.latestCompleted().equals(after.latestCompleted())
                && before.latestFailed().equals(after.latestFailed()) && before.latestRestored().equals(after.latestRestored())
                && before.counts().inProgress() == 0 && before.inProgress().isEmpty() && after.inProgress().isEmpty();
    }

    // The coordinator can trigger checkpoints before heartbeat loss detects the dead TM.
    // Account for every such failure; no new completed checkpoint may replace restore point N.
    static boolean afterKillBoundary(FlinkCheckpointEvidence.History failed, FlinkCheckpointEvidence.History after,
            long checkpoint, long beforeKill) {
        if (after.counts().total() < failed.counts().total() || after.counts().failed() < failed.counts().failed()) return false;
        long additionalFailures = after.counts().failed() - failed.counts().failed();
        var candidates = after.retained().stream().filter(value -> value.id() > failed.highestObservedId()).toList();
        var originalFailure = failed.latestFailed();
        var latestFailure = candidates.stream().max(java.util.Comparator.comparingLong(FlinkCheckpointEvidence.Checkpoint::id))
                .or(() -> originalFailure);
        long restores = after.counts().restored() - failed.counts().restored();
        return failed.jobId().equals(after.jobId())
                && after.counts().total() - failed.counts().total() == additionalFailures
                && failed.counts().completed() == after.counts().completed()
                && after.counts().inProgress() == 0 && after.inProgress().isEmpty()
                && candidates.size() == additionalFailures
                && candidates.stream().map(FlinkCheckpointEvidence.Checkpoint::id).distinct().count() == additionalFailures
                && candidates.stream().allMatch(value -> value.status() == FlinkCheckpointEvidence.Status.FAILED
                    && !value.savepoint() && value.triggerTimestamp() >= beforeKill
                    && value.failureTimestamp().isPresent() && value.failureTimestamp().orElseThrow() >= value.triggerTimestamp())
                && originalFailure.filter(value -> value.status() == FlinkCheckpointEvidence.Status.FAILED
                    && after.retained().contains(value)).isPresent()
                && after.latestFailed().equals(latestFailure)
                && after.highestObservedId() == latestFailure.orElseThrow().id()
                && failed.latestCompleted().equals(after.latestCompleted()) && failed.latestSavepoint().equals(after.latestSavepoint())
                && (restores == 0 && failed.latestRestored().equals(after.latestRestored())
                    || restores == 1 && after.latestRestored().filter(value -> !value.savepoint() && value.id() == checkpoint
                        && value.restoreTimestamp() >= beforeKill).isPresent());
    }

    static boolean monotonicRestore(FlinkCheckpointEvidence.History before, FlinkCheckpointEvidence.History after) {
        return after.counts().total() >= before.counts().total() && after.counts().failed() >= before.counts().failed()
                && before.retained().stream().filter(value -> value.status() == FlinkCheckpointEvidence.Status.FAILED)
                    .allMatch(value -> after.find(value.id()).filter(value::equals).isPresent())
                && after.counts().restored() >= before.counts().restored()
                && (before.counts().restored() == 0 || before.latestRestored().equals(after.latestRestored()));
    }

    private static void requireArmed(StorageFaultControl.Snapshot snapshot, String faultId, String target, boolean dead) throws IOException {
        require(snapshot.complete() && snapshot.instances().size() == 2, "Armed storage inventory is incomplete");
        for (var value : snapshot.instances()) {
            requireVerified(value);
            require(value.processDead() == (dead && value.instance().runtimeId().equals(target))
                    && value.status().state() == StorageFaultControl.State.ARMED && value.status().revision() == 1
                    && faultId.equals(value.status().faultId()), "Write fault expired, aborted, or changed before replacement");
        }
    }

    private static void requireHealthySurvivor(StorageFaultControl.Snapshot snapshot, String target) throws IOException {
        require(snapshot.complete() && snapshot.instances().size() == 2, "Healed storage inventory incomplete");
        for (var value : snapshot.instances()) {
            requireVerified(value);
            if (value.instance().runtimeId().equals(target)) require(value.processDead(), "Old TaskManager is still alive");
            else require(!value.processDead() && value.status().state() == StorageFaultControl.State.HEALTHY
                    && value.status().revision() == 2, "Surviving storage did not heal");
        }
    }

    private static void requireVerified(StorageFaultControl.InstanceEvidence value) throws IOException {
        var v = value.verification();
        require(v != null && v.complete() && v.diagnostics().isEmpty() && v.rawReadDenied() && v.rawWriteDenied()
                && v.uid().size() == 4 && v.uid().stream().allMatch(uid -> uid == 9999)
                && v.gid().size() == 4 && v.gid().stream().allMatch(gid -> gid == 9999)
                && v.capabilities().matches("0+") && v.jvmPid() > 0
                && value.journalComplete() && value.diagnostics().isEmpty() && value.status() != null
                && value.status().journalComplete() && value.journalBytes() > 0
                && value.journalSha256() != null && value.journalSha256().matches("[0-9a-f]{64}")
                && value.operations().size() == value.lastSequence(), "FUSE mount, JVM identity or native journal verification incomplete");
        for (int i = 0; i < value.operations().size(); i++) require(value.operations().get(i).sequence() == i + 1
                && value.operations().get(i).instance().equals(value.instance()), "Native journal has a gap or foreign instance");
    }

    private static void validateTransition(StorageFaultControl.Transition transition, String fault, long revision,
            StorageFaultControl.Snapshot before, StorageFaultControl.Snapshot after, boolean deadTarget) throws IOException {
        require(transition.confirmed() && transition.diagnostics().isEmpty() && transition.faultId().equals(fault)
                && transition.revision() == revision && transition.completedNanos() - transition.startedNanos() >= 0,
                "Storage transition not confirmed");
        var live = before.instances().stream().filter(value -> !value.processDead()).map(StorageFaultControl.InstanceEvidence::instance).toList();
        var dead = before.instances().stream().filter(StorageFaultControl.InstanceEvidence::processDead)
                .map(StorageFaultControl.InstanceEvidence::instance).toList();
        require(transition.acknowledgements().size() == live.size() && transition.deadInstances().size() == dead.size()
                && transition.deadInstances().containsAll(dead) && (!deadTarget || dead.size() == 1), "Transition missed an incarnation");
        String operation = revision == 1 ? "ARM" : "HEAL";
        for (var instance : live) require(transition.acknowledgements().stream().filter(command -> command.instance().equals(instance)
                && command.confirmed() && command.exitCode() == 0 && command.status() != null
                && operation.equals(command.command()) && command.status().journalComplete()
                && command.status().sequence() > before.instances().stream().filter(value -> value.instance().equals(instance))
                    .findFirst().orElseThrow().lastSequence()
                && command.status().revision() == revision && fault.equals(command.status().faultId())
                && command.status().state() == (revision == 1 ? StorageFaultControl.State.ARMED : StorageFaultControl.State.HEALTHY)
                && after.instances().stream().filter(value -> value.instance().equals(instance))
                    .anyMatch(value -> value.commands().contains(command) && value.operations().stream().anyMatch(receipt ->
                        receipt.sequence() == command.status().sequence() && receipt.instance().equals(instance)
                            && receipt.operation().equals(operation) && receipt.revision() == revision
                            && fault.equals(receipt.faultId()) && receipt.state() == command.status().state()
                            && receipt.errno() == 0 && receipt.result() == 0))).count() == 1,
                "Missing exact-instance storage acknowledgement");
    }

    private static boolean subsequentCheckpoint(FlinkCheckpointEvidence.History history, StateRecoveryEvidence.Trace trace,
            StateRecoveryEvidence.Recovery recovery, long failed) {
        if (trace == null || !trace.complete() || recovery.restoredStart().isEmpty() || recovery.postRestoreProgress().isEmpty()
                || history.latestCompleted().isEmpty()) return false;
        var completed = history.latestCompleted().orElseThrow();
        var start = recovery.restoredStart().orElseThrow();
        long progressLine = recovery.postRestoreProgress().orElseThrow().lineNumber();
        return completed.id() > failed && history.latestRestored().filter(value -> value.id() == recovery.restoredCheckpointId()
                && completed.triggerTimestamp() >= value.restoreTimestamp()).isPresent()
                && trace.markers().stream().anyMatch(marker -> sameMarkerExecution(marker, start)
                    && marker.event() == StateRecoveryEvidence.Event.SNAPSHOT && marker.checkpointId() == completed.id()
                    && marker.lineNumber() > progressLine
                    && marker.progress().filter(value -> value.keyCount() == recovery.readiness().keyCount()
                        && value.observedKeys() == recovery.readiness().keyCount() && value.minimumObservedCount() > 0).isPresent()
                    && trace.markers().stream().anyMatch(ack -> sameMarkerExecution(ack, marker)
                        && ack.event() == StateRecoveryEvidence.Event.COMPLETED && ack.checkpointId() == completed.id()
                        && ack.lineNumber() > marker.lineNumber()));
    }

    private static boolean sameMarkerExecution(StateRecoveryEvidence.Marker left, StateRecoveryEvidence.Marker right) {
        return left.process().equals(right.process()) && left.jobId().equals(right.jobId())
                && left.operatorUid().equals(right.operatorUid()) && left.subtask() == right.subtask()
                && left.executionAttempt() == right.executionAttempt() && left.restoredCheckpointId() == right.restoredCheckpointId();
    }

    private static void validate(Evidence e) throws IOException {
        require(e.errors().isEmpty() && e.samples().size() == 6 && e.transitions().size() == 2
                && e.kill() != null && e.restart() != null && e.recovery() != null && e.recovery().confirmed()
                && e.recovery().restoredCheckpointId() == e.checkpointId(), "Storage experiment did not complete");
        validateConfiguration(e.configuration(), e.jobId(), e.declaredControls());
        for (int i = 0; i < 6; i++) {
            Sample sample = e.samples().get(i);
            require(sample.stage() == Stage.values()[i] && sample.sequence() == i + 1 && sample.elapsedNanos() >= 0
                    && sample.elapsedNanos() <= Duration.ofMillis(e.timeoutMillis()).toNanos()
                    && (i == 0 || sample.elapsedNanos() >= e.samples().get(i - 1).elapsedNanos())
                    && sample.storage().complete() && sample.storage().diagnostics().isEmpty()
                    && sample.checkpoints().jobId().equals(e.jobId()), "Incomplete/out-of-order storage stage");
            var first = e.samples().getFirst().storage();
            require(sample.storage().attemptId().equals(first.attemptId()) && sample.storage().storageId().equals(first.storageId())
                    && sample.storage().namespace().equals(first.namespace()) && sample.storage().checkpointUri().equals(first.checkpointUri())
                    && sample.storage().helperManifestSha256().equals(first.helperManifestSha256()), "Storage identity changed");
        }
        var before = e.samples().get(0); var armed = e.samples().get(1); var failed = e.samples().get(2);
        var stopped = e.samples().get(3); var healed = e.samples().get(4); var recovered = e.samples().get(5);
        validateBaseline(e.readiness(), e.checkpointId(), before);
        validateMaterialization(e.materialization(), before.storage(), e.readiness(), e.checkpointId());
        require(sameCheckpointBoundary(before.checkpoints(), armed.checkpoints()), "Arming checkpoint race");
        requireArmed(armed.storage(), e.triggerId(), e.readiness().target().runtimeId(), false);
        validateTransition(e.transitions().get(0), e.triggerId(), 1, before.storage(), armed.storage(), false);
        require(correlate(armed.checkpoints(), failed.checkpoints(), e.triggerResult()).id() == e.failedCheckpointId(), "Failed checkpoint ID differs");
        validateFailure(e, failed);
        require(sameExecution(e.readiness(), failed.job()) && stopped.job().activeSubtasks().isEmpty()
                && e.kill().jobManagerTimeBeforeKill().isPresent()
                && afterKillBoundary(failed.checkpoints(), stopped.checkpoints(), e.checkpointId(), e.kill().jobManagerTimeBeforeKill().orElseThrow())
                && afterKillBoundary(failed.checkpoints(), healed.checkpoints(), e.checkpointId(), e.kill().jobManagerTimeBeforeKill().orElseThrow())
                && monotonicRestore(stopped.checkpoints(), healed.checkpoints()), "Kill/heal checkpoint window missed");
        requireArmed(stopped.storage(), e.triggerId(), e.readiness().target().runtimeId(), true);
        requireHealthySurvivor(healed.storage(), e.readiness().target().runtimeId());
        validateTransition(e.transitions().get(1), e.triggerId(), 2, stopped.storage(), healed.storage(), true);
        require(e.kill().stateReadiness().filter(e.readiness()::equals).isPresent()
                && e.kill().jobBeforeKill().observation().filter(failed.job()::equals).isPresent()
                && e.kill().identity().filter(e.readiness().target()::equals).isPresent()
                && e.restart().previousIdentity().filter(e.readiness().target()::equals).isPresent()
                && e.restart().replacementIdentity().isPresent(), "Replacement lacks readiness binding");
        String replacement = e.restart().replacementIdentity().orElseThrow().runtimeId();
        require(e.recoveryTrace() != null && e.recoveryTrace().complete(), "Post-restore trace is missing");
        var effect = TaskManagerKillEffect.evaluate(List.of(e.kill()), Optional.of(new FlinkJobObservation.Attempt(
                Optional.of(recovered.job()), Optional.empty()))).getFirst();
        require(StateRecoveryEvidence.evaluate(e.readiness(), effect, e.recoveryTrace()).equals(e.recovery())
                && subsequentCheckpoint(recovered.checkpoints(), e.recoveryTrace(), e.recovery(), e.failedCheckpointId()),
                "Restored state or subsequent checkpoint is not bound to its actual receipts");
        var newProcess = e.recovery().restoredStart().orElseThrow().process();
        require(recovered.storage().instances().size() == 3 && !replacement.equals(e.readiness().target().runtimeId())
                && e.recovery().restoredStart().orElseThrow().operatorUid().equals(StateRecoveryEvidence.OPERATOR_UID)
                && newProcess.runtimeId().equals(replacement) && newProcess.logicalName().equals(TARGET)
                && before.storage().instances().stream().allMatch(old -> recovered.storage().instances().stream()
                    .anyMatch(value -> value.instance().equals(old.instance())))
                && recovered.storage().instances().stream().filter(value -> value.instance().runtimeId().equals(replacement)
                    && value.instance().role() == FlinkComponentRole.TASK_MANAGER
                    && value.instance().logicalName().equals(newProcess.logicalName()) && value.instance().process().equals(newProcess.process())
                    && !value.processDead() && value.status().state() == StorageFaultControl.State.HEALTHY
                    && value.status().revision() == 0).count() == 1,
                "Replacement lacks a fresh healthy FUSE instance");
        for (var instance : recovered.storage().instances()) requireVerified(instance);
        require(recovered.checkpoints().latestRestored().filter(value -> value.id() == e.checkpointId()).isPresent()
                && recovered.checkpoints().counts().restored() == before.checkpoints().counts().restored() + 1
                && recovered.checkpoints().latestCompleted().filter(value -> value.id() > e.failedCheckpointId()).isPresent(),
                "Restoration or subsequent successful checkpoint is absent");
    }

    public static boolean finalConfirmed(Evidence evidence, StorageFaultControl.Snapshot finalSnapshot) {
        if (!evidence.confirmed() || !healthyFinalCapture(finalSnapshot)) return false;
        var recovered = evidence.samples().getLast().storage();
        if (!recovered.attemptId().equals(finalSnapshot.attemptId()) || !recovered.storageId().equals(finalSnapshot.storageId())
                || !recovered.namespace().equals(finalSnapshot.namespace()) || !recovered.checkpointUri().equals(finalSnapshot.checkpointUri())
                || !recovered.helperManifestSha256().equals(finalSnapshot.helperManifestSha256())
                || recovered.instances().size() != finalSnapshot.instances().size()) return false;
        return evidence.samples().stream().flatMap(sample -> sample.storage().instances().stream()).allMatch(prior ->
                finalSnapshot.instances().stream().anyMatch(last -> last.instance().equals(prior.instance())
                        && last.operations().containsAll(prior.operations()) && last.commands().containsAll(prior.commands())));
    }

    public static boolean healthyFinalCapture(StorageFaultControl.Snapshot snapshot) {
        if (snapshot == null || !snapshot.complete() || !snapshot.quiescent() || !snapshot.diagnostics().isEmpty()
                || snapshot.instances().isEmpty() || snapshot.instances().stream().anyMatch(value -> !value.processDead())) return false;
        try {
            for (var instance : snapshot.instances()) {
                requireVerified(instance);
                require(!terminalFailure(instance.status().state())
                        && instance.operations().stream().noneMatch(operation -> terminalFailure(operation.state())),
                        "Storage aborted, expired or lost its journal before the final fence");
            }
            return true;
        }
        catch (IOException | RuntimeException failure) { return false; }
    }

    private static boolean terminalFailure(StorageFaultControl.State state) {
        return state == StorageFaultControl.State.EXPIRED || state == StorageFaultControl.State.ABORTED || state == StorageFaultControl.State.FATAL;
    }

    static StorageFaultControl.Snapshot finalCapture(V1AttemptRuntime runtime) {
        try { return runtime.storageSnapshot(Duration.ofSeconds(10)); }
        catch (RuntimeException failure) {
            return new StorageFaultControl.Snapshot("unavailable", "unavailable", "unavailable", "unavailable", "",
                    List.of(), List.of(), false, false, List.of("Final storage capture failed: " + failure));
        }
    }

    private void pause(MonotonicDeadline deadline) throws IOException, InterruptedException {
        Duration left = left(deadline); sleeper.sleep(left.compareTo(Duration.ofMillis(100)) < 0 ? left : Duration.ofMillis(100));
    }
    private static Duration left(MonotonicDeadline deadline) throws IOException {
        return deadline.remainingOrThrow(() -> new IOException("Checkpoint storage experiment deadline expired"));
    }
    private static String relative(String path) {
        if (path.startsWith("/flink/checkpoints/")) return path.substring("/flink/checkpoints/".length());
        return path.startsWith("/") ? path.substring(1) : path;
    }
    private static void require(boolean condition, String message) throws IOException { if (!condition) throw new IOException(message); }

    private static final class Recorder {
        final String path, jobId, triggerId = UUID.randomUUID().toString().replace("-", "");
        final long timeout;
        final ExecutableScenarioPlan.CheckpointControls declaredControls;
        long checkpoint = -1, failedId = -1;
        FlinkCheckpointEvidence.Configuration configuration;
        StateRecoveryEvidence.Readiness readiness;
        StorageFaultControl.Materialization materialization;
        FlinkCheckpointTrigger.Observation triggerResult;
        FlinkCheckpointEvidence.Checkpoint failedCheckpoint;
        StateRecoveryEvidence.Trace failureTrace;
        StateRecoveryEvidence.Trace recoveryTrace;
        final List<Sample> samples = new ArrayList<>();
        final List<StorageFaultControl.Transition> transitions = new ArrayList<>();
        PhaseExecutionEvidence.TaskManagerKill kill;
        PhaseExecutionEvidence.TaskManagerRestart restart;
        StateRecoveryEvidence.Recovery recovery;
        final List<String> errors = new ArrayList<>();
        List<RawEvidenceRef> triggerSubmitEvidence = List.of(), beforeKillEvidence = List.of(), afterKillEvidence = List.of();
        Recorder(String path, String jobId, long timeout, ExecutableScenarioPlan.CheckpointControls declaredControls) {
            this.path = path; this.jobId = jobId; this.timeout = timeout;
            this.declaredControls = java.util.Objects.requireNonNull(declaredControls, "declaredControls");
        }
        Evidence snapshot() { return new Evidence(path, jobId, triggerId, timeout, checkpoint, failedId, configuration, readiness,
                materialization, triggerResult, failedCheckpoint, failureTrace, recoveryTrace, samples, transitions, kill, restart,
                recovery, errors, declaredControls, triggerSubmitEvidence, beforeKillEvidence, afterKillEvidence); }
    }
}
