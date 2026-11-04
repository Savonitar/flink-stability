package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Explicit, bounded first checkpoint-storage experiment; no arbitrary filesystem paths. */
final class CheckpointStoragePlanCompiler {
    private static final Set<String> OWNED_OPTIONS = Set.of(
            "execution.checkpointing.data-inline-threshold", "state.storage.fs.memory-threshold",
            "state.backend.fs.memory-threshold", "execution.checkpointing.max-concurrent-checkpoints",
            "execution.checkpointing.tolerable-failed-checkpoints");

    private CheckpointStoragePlanCompiler() {}

    static void validate(Path source, JsonNode document, List<Diagnostic> issues) {
        JsonNode job = document.at("/workload/jobs/0");
        JsonNode checkpointing = job.path("checkpointing");
        boolean fuse = checkpointing.at("/storage/fuse").isObject();
        List<JsonNode> steps = new ArrayList<>();
        for (JsonNode phase : document.path("phases")) for (JsonNode step : phase.path("steps")) steps.add(step);
        long atomic = steps.stream().filter(step -> step.has("checkpoint_storage_fault")).count();
        boolean anyAtomic = contains(document.path("phases"), "checkpoint_storage_fault");
        if (!fuse) {
            if (anyAtomic || checkpointing.has("small_file_threshold") || checkpointing.has("max_concurrent")
                    || checkpointing.has("tolerable_failures"))
                reject(source, issues, "fuse-required", "Storage faults and typed checkpoint controls require explicit FUSE storage");
            return;
        }
        try { specification(checkpointing); }
        catch (IllegalArgumentException invalid) { reject(source, issues, "configuration-invalid", invalid.getMessage()); }
        JsonNode flink = document.at("/setup/flink");
        if (!"keyed-prefix-v1".equals(job.at("/behavior/type").asText())
                || !"hashmap".equals(job.path("state_backend").asText())
                || !flink.path("image_id").equals(checkpointing.at("/storage/fuse/image_id"))
                || flink.has("runtime_jar") || flink.has("token_provider") || flink.has("high_availability")) {
            reject(source, issues, "topology-unsupported",
                    "FUSE requires keyed-prefix-v1, HashMap state, an identical pinned runtime image and no HA/token provider");
        }
        if (!checkpointing.path("small_file_threshold").isIntegralNumber()
                || checkpointing.path("small_file_threshold").asInt(-1) != 0
                || checkpointing.path("max_concurrent").asInt(-1) != 1
                || checkpointing.path("tolerable_failures").asInt() < 1
                || checkpointing.path("tolerable_failures").asInt() > 10) {
            reject(source, issues, "controls-required",
                    "FUSE requires explicit small_file_threshold=0, max_concurrent=1 and tolerable_failures in 1..10");
        }
        for (String key : OWNED_OPTIONS) {
            if (flink.path("config").has(key) || job.path("program_args").toString().contains(key))
                reject(source, issues, "configuration-conflict", "FUSE checkpoint controls cannot be overridden: " + key);
        }
        int kills = 0;
        for (JsonNode step : steps) {
            if (step.has("checkpoint_storage_fault")) {
                JsonNode fault = step.path("checkpoint_storage_fault");
                if (!fault.path("job").equals(job.path("alias")))
                    reject(source, issues, "job-mismatch", "Storage fault must target the compiled workload job");
                try { fault(fault); }
                catch (IllegalArgumentException invalid) { reject(source, issues, "budget-invalid", invalid.getMessage()); }
            } else if (step.has("kill") || step.has("restart")) {
                if (atomic > 0) reject(source, issues, "fault-combination", "Atomic storage fault cannot be combined with another process fault");
                if (step.has("kill")) kills++;
            } else if (!step.has("await") && !step.has("wait")) {
                reject(source, issues, "fault-combination", "FUSE permits only awaits/waits and one atomic fault or one TaskManager kill/restart control");
            }
        }
        if (atomic > 1 || kills > 1 || anyAtomic && atomic == 0)
            reject(source, issues, "fault-count", "Only one non-looping checkpoint-storage fault or TaskManager kill is supported");
        if (atomic > 0) {
            for (JsonNode cluster : document.at("/setup/kafka/clusters")) for (JsonNode topic : cluster.path("topics")) {
                if (topic.has("input_source") && topic.at("/input_source/total").asLong() < 2L * job.at("/behavior/key_count").asInt())
                    reject(source, issues, "input-too-small", "Storage recovery needs a nonzero prefix and a suffix for every key");
            }
        }
    }

    static Optional<ExecutableScenarioPlan.FuseStorageSpec> specification(JsonNode checkpointing) {
        JsonNode fuse = checkpointing.at("/storage/fuse");
        return fuse.isObject() ? Optional.of(new ExecutableScenarioPlan.FuseStorageSpec(
                fuse.path("bundle").asText(), fuse.path("manifest_sha256").asText(),
                fuse.path("image_id").asText(), fuse.path("platform").asText())) : Optional.empty();
    }

    static Optional<ExecutableScenarioPlan.CheckpointControls> controls(JsonNode checkpointing) {
        return checkpointing.at("/storage/fuse").isObject()
                ? Optional.of(new ExecutableScenarioPlan.CheckpointControls(checkpointing.path("small_file_threshold").asInt(),
                        checkpointing.path("max_concurrent").asInt(), checkpointing.path("tolerable_failures").asInt()))
                : Optional.empty();
    }

    static ExecutableScenarioPlan.CheckpointStorageFault fault(JsonNode node) {
        return new ExecutableScenarioPlan.CheckpointStorageFault(node.path("job").asText(),
                ExecutableScenarioPlanCompiler.parseDuration(node.path("timeout").asText()),
                ExecutableScenarioPlanCompiler.parseDuration(node.path("lease").asText()));
    }

    private static boolean contains(JsonNode node, String field) {
        if (node.isObject() && node.has(field)) return true;
        if (node.isContainerNode()) for (JsonNode child : node) if (contains(child, field)) return true;
        return false;
    }

    private static void reject(Path source, List<Diagnostic> issues, String suffix, String detail) {
        issues.add(ExecutableScenarioPlanCompiler.issue(source, "runner.checkpoint-storage." + suffix,
                "$/workload/jobs/0/checkpointing", detail));
    }
}
