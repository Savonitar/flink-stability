package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.*;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/** One stop/restore transition, retaining partial receipts after every uncertain outcome. */
public final class SavepointLifecycle {
    private SavepointLifecycle() {}
    public record Evidence(String path, String oldJobId, String triggerId, String requestedDirectory,
            String acknowledgedTrigger, FlinkJobState beforeState, long beforeCompletedCheckpoints,
            FlinkSavepoint.Status savepoint, FlinkJobState stoppedState,
            String restoredJobId, int requestedParallelism, String requestedStrategy,
            FlinkSavepoint.RestoreProof restoreProof, long elapsedNanos, String error) {
        public boolean confirmed() {
            return error == null && beforeState == FlinkJobState.RUNNING && beforeCompletedCheckpoints >= 1
                    && triggerId.equals(acknowledgedTrigger) && savepoint != null && savepoint.completed()
                    && savepoint.failure() == null && ownedLocation(requestedDirectory, savepoint.location())
                    && stoppedState == FlinkJobState.FINISHED && restoredJobId != null && !restoredJobId.equals(oldJobId)
                    && restoreProof != null && restoreProof.state() == FlinkJobState.RUNNING && restoreProof.savepoint()
                    && Objects.equals(savepoint.location(), restoreProof.location()) && restoreProof.parallelism() == requestedParallelism;
        }
    }
    public static boolean ownedLocation(String directory, String location) {
        try {
            URI base = URI.create(directory), target = URI.create(location);
            var basePath = java.nio.file.Path.of(base).normalize();
            var targetPath = java.nio.file.Path.of(target).normalize();
            return base.getPath().startsWith("/flink/checkpoints/") && targetPath.startsWith(basePath) && !targetPath.equals(basePath)
                    && "file".equals(base.getScheme()) && "file".equals(target.getScheme())
                    && base.getAuthority() == null && target.getAuthority() == null
                    && target.getQuery() == null && target.getFragment() == null
                    && target.equals(target.normalize()) && target.getPath().startsWith(base.getPath() + "/")
                    && !target.getPath().contains("\\") && !target.getPath().contains("/../");
        } catch (RuntimeException invalid) { return false; }
    }
    static Evidence execute(String path, FlinkScenarioControl flink, FlinkJobHandle oldJob, FlinkJobSubmission original,
            String directory, ExecutableScenarioPlan.SavepointRestore step,
            PhaseSleeper sleeper, LongSupplier clock) {
        String trigger = UUID.randomUUID().toString().replace("-", ""), acknowledged = null, restoredJob = null, error = null;
        FlinkSavepoint.Status status = null; FlinkSavepoint.RestoreProof proof = null; FlinkJobState stopped = null, beforeState = null; long beforeCheckpoints = 0;
        long started = clock.getAsLong(); var deadline = MonotonicDeadline.start(step.timeout(), clock);
        try {
            if (original == null || directory == null || !directory.startsWith("file:/flink/checkpoints/"))
                throw new IOException("Prepared restore submission or owned savepoint directory unavailable");
            var before = flink.observe(oldJob, remaining(deadline));
            beforeState = before.state(); beforeCheckpoints = before.completedCheckpoints();
            if (before.state() != FlinkJobState.RUNNING || before.completedCheckpoints() < 1)
                throw new IOException("Savepoint requires a running job with a completed checkpoint");
            acknowledged = flink.stopWithSavepoint(oldJob, trigger, directory, remaining(deadline));
            if (!trigger.equals(acknowledged)) throw new IOException("Stop acknowledgement mismatch");
            while (true) {
                status = flink.savepointStatus(oldJob, trigger, remaining(deadline));
                if (status.completed()) break;
                pause(sleeper, deadline);
            }
            if (status.failure() != null) throw new IOException("Stop with savepoint failed: " + status.failure());
            if (!ownedLocation(directory, status.location())) throw new IOException("Savepoint location escaped its attempt directory");
            stopped = flink.awaitState(oldJob, FlinkJobState.FINISHED, remaining(deadline));
            if (stopped != FlinkJobState.FINISHED) throw new IOException("Old job did not finish");
            var configuration = new LinkedHashMap<>(original.flinkConfiguration());
            configuration.put(ExecutableScenarioPlan.WorkloadConfiguration.PREFIX + "sink.transaction-id-naming-strategy", step.strategy().name());
            var submission = new FlinkJobSubmission(original.uploadedJarId(), step.parallelism(), configuration,
                    original.programArguments(), Optional.of(status.location()));
            restoredJob = flink.submit(submission, remaining(deadline)).jobId();
            if (restoredJob.equals(oldJob.jobId())) throw new IOException("Restore reused the old JobID");
            var job = new FlinkJobHandle(restoredJob);
            flink.awaitState(job, FlinkJobState.RUNNING, remaining(deadline));
            while (true) {
                proof = flink.restoredSavepoint(job, remaining(deadline));
                if (proof.state() != FlinkJobState.RUNNING) throw new IOException("Restored job is no longer running");
                if (proof.location() != null) break;
                pause(sleeper, deadline);
            }
            if (!proof.savepoint() || !status.location().equals(proof.location()) || proof.parallelism() != step.parallelism())
                throw new IOException("Savepoint restoration path or parallelism is unconfirmed");
            remaining(deadline);
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            error = failure.toString();
        }
        return new Evidence(path, oldJob.jobId(), trigger, directory, acknowledged, beforeState, beforeCheckpoints, status, stopped, restoredJob,
                step.parallelism(), step.strategy().name(), proof, Math.max(0, clock.getAsLong() - started), error);
    }
    private static Duration remaining(MonotonicDeadline deadline) throws IOException {
        var remaining = deadline.remaining();
        if (remaining.isZero()) throw new IOException("Savepoint lifecycle deadline expired");
        return remaining;
    }
    private static void pause(PhaseSleeper sleeper, MonotonicDeadline deadline) throws Exception {
        var remaining = remaining(deadline);
        sleeper.sleep(remaining.compareTo(Duration.ofMillis(250)) < 0 ? remaining : Duration.ofMillis(250));
    }
}
