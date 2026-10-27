package org.savonitar.flink.stability.runtime.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Attempt-owned checkpoint filesystem control. Every transition retains per-incarnation receipts. */
public interface StorageFaultControl {
    enum State { STARTING, HEALTHY, ARMED, EXPIRED, ABORTED, FATAL, STOPPED }

    record Instance(String logicalName, String process, String runtimeId, String instanceId,
                    FlinkComponentRole role, long daemonPid) {
        public Instance {
            Checks.requireNonBlank(logicalName, "logicalName"); Checks.requireNonBlank(process, "process");
            Checks.requireNonBlank(runtimeId, "runtimeId"); Checks.requireNonBlank(instanceId, "instanceId");
            Objects.requireNonNull(role, "role");
            if (daemonPid < 1) throw new IllegalArgumentException("Verified daemon PID is required");
        }
    }

    record Verification(String imageId, String helperSha256, String probeSha256, String mountInfo,
                        List<Long> uid, List<Long> gid, String capabilities, long jvmPid, String jvmCommand, boolean rawReadDenied,
                        boolean rawWriteDenied, boolean complete, List<String> diagnostics) {
        public Verification { uid = List.copyOf(uid); gid = List.copyOf(gid); diagnostics = List.copyOf(diagnostics); }
    }

    record Status(State state, String faultId, long revision, long sequence, long leaseMs,
                  long leaseDeadlineMonotonicNs, boolean journalComplete, boolean stopping) {
        public Status { Objects.requireNonNull(state, "state"); }
    }

    record Command(Instance instance, String command, long startedNanos, long completedNanos,
                   int exitCode, Status status, String stdout, String stderr, boolean confirmed) {}

    /** Raw errno and byte counts from every journal record, including control/lifecycle records.
     * IO paths are daemon-relative: namespace/job-id/chk-N/file. */
    record Operation(Instance instance, long sequence, long timestampMillis, long monotonicNanos,
                     long revision, State state, String faultId, String operation, String path,
                     String faultOperation, long result, int errno, long requestedBytes,
                     long returnedBytes, Boolean datasync, String reason) {}

    /** Exact absolute FUSE path observed by the inspector, never the raw backing path. */
    record FileEvidence(String path, long size, boolean regularFile, String sha256, long observedNanos) {}

    record Materialization(Instance instance, String jobId, long checkpointId, String operatorUid,
                           FileEvidence metadata, List<FileEvidence> files, boolean complete,
                           List<String> diagnostics) {
        public Materialization { files = List.copyOf(files); diagnostics = List.copyOf(diagnostics); }
    }

    default Materialization inspectCheckpoint(String jobId, long checkpointId, Duration timeout) {
        throw new UnsupportedOperationException("Checkpoint state inspection is unavailable");
    }

    /** journalPath refers to an immutable local capture; its bytes include every complete raw record. */
    record InstanceEvidence(Instance instance, Verification verification, Status status,
                            boolean processDead, boolean journalComplete, String journalPath, String journalSha256,
                            long journalBytes, long lastSequence, List<Operation> operations,
                            List<Command> commands, List<String> diagnostics) {
        public InstanceEvidence {
            operations = List.copyOf(operations); commands = List.copyOf(commands); diagnostics = List.copyOf(diagnostics);
        }
    }

    record Snapshot(String attemptId, String storageId, String namespace, String checkpointUri,
                    String helperManifestSha256, List<InstanceEvidence> instances,
                    List<FileEvidence> materializedFiles, boolean complete, boolean quiescent,
                    List<String> diagnostics) {
        public Snapshot {
            instances = List.copyOf(instances); materializedFiles = List.copyOf(materializedFiles);
            diagnostics = List.copyOf(diagnostics);
        }
    }

    record Transition(String faultId, long revision, long startedNanos, long completedNanos,
                      List<Command> acknowledgements, List<Instance> deadInstances,
                      boolean confirmed, List<String> diagnostics) {
        public Transition {
            acknowledgements = List.copyOf(acknowledgements); deadInstances = List.copyOf(deadInstances);
            diagnostics = List.copyOf(diagnostics);
        }
    }

    default Snapshot storageSnapshot(Duration timeout) {
        throw new UnsupportedOperationException("Checkpoint storage fault control is unavailable");
    }

    default Transition armStorage(String faultId, long revision, Duration lease, Duration timeout) {
        throw new UnsupportedOperationException("Checkpoint storage fault control is unavailable");
    }

    default Transition healStorage(String faultId, long revision, Duration timeout) {
        throw new UnsupportedOperationException("Checkpoint storage fault control is unavailable");
    }

    default Transition abortStorage(String faultId, long revision, Duration timeout) {
        throw new UnsupportedOperationException("Checkpoint storage fault control is unavailable");
    }
}
