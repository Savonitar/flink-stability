package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.runtime.api.FuseCheckpointStorage;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointStoragePlanCompilerTest {
    private final SpecificationLoader loader = new SpecificationLoader();

    @Test void compilesIndependentFixtureToPinnedStorageAndOneAtomicStep() {
        var plan = compile(document -> {});
        var storage = plan.job().checkpointing().fuse().orElseThrow();
        assertEquals("./helper-build", storage.bundle());
        assertEquals("a".repeat(64), storage.manifestSha256());
        assertEquals(FuseCheckpointStorage.SUPPORTED_IMAGE_ID, storage.imageId());
        assertEquals("linux/arm64", storage.platform());
        assertEquals("0 bytes", plan.job().standardFlinkConfiguration().get("execution.checkpointing.data-inline-threshold"));
        assertEquals("1", plan.job().standardFlinkConfiguration().get("execution.checkpointing.max-concurrent-checkpoints"));
        assertEquals("10", plan.job().standardFlinkConfiguration().get("execution.checkpointing.tolerable-failed-checkpoints"));
        var step = (ExecutableScenarioPlan.CheckpointStorageFault) plan.phases().getFirst().steps().getLast();
        assertEquals("state-job", step.jobAlias());
        assertEquals(Duration.ofSeconds(90), step.timeout());
        assertEquals(Duration.ofSeconds(120), step.lease());
    }

    @Test void legacyFilesystemDoesNotAcquireNewConfiguration() {
        var plan = compile(document -> {
            checkpointing(document).remove(List.of("small_file_threshold", "max_concurrent", "tolerable_failures"));
            ((ObjectNode) checkpointing(document).path("storage")).remove("fuse");
            steps(document).remove(1);
        });
        assertTrue(plan.job().checkpointing().fuse().isEmpty());
        assertTrue(plan.job().checkpointing().controls().isEmpty());
        assertFalse(plan.job().standardFlinkConfiguration().containsKey("execution.checkpointing.data-inline-threshold"));
    }

    @Test void resolvesWholeScalarBundleAndManifestParametersBeforeCompilation() {
        var plan = compile(document -> {
            var parameters = document.putObject("parameters");
            parameters.putObject("helper_bundle").put("type", "string").put("default", "./helper-build");
            parameters.putObject("helper_manifest_sha256").put("type", "string").put("default", "a".repeat(64));
            fuse(document).put("bundle", "${helper_bundle}").put("manifest_sha256", "${helper_manifest_sha256}");
        });
        assertEquals("./helper-build", plan.job().checkpointing().fuse().orElseThrow().bundle());
        assertEquals("a".repeat(64), plan.job().checkpointing().fuse().orElseThrow().manifestSha256());
    }

    @Test void supportsHealthyUninterruptedAndSingleOrdinaryRecoveryControls() {
        assertDoesNotThrow(() -> compile(document -> steps(document).remove(1)));
        assertDoesNotThrow(() -> compile(document -> { steps(document).remove(1); addKillRestart(document); }));
    }

    @Test void rejectsUnknownJobDuringSemanticResolution() {
        failure(SpecificationException.Stage.PREFLIGHT, document -> fault(document).put("job", "missing"));
    }

    @Test void rejectsArbitraryFilesystemPathsAndUnsupportedShapeAtSchemaBoundary() {
        for (String change : List.of("mount", "raw", "mode", "hash", "platform", "threshold", "concurrent", "tolerance")) {
            failure(SpecificationException.Stage.DOCUMENT, document -> {
                switch (change) {
                    case "mount" -> fuse(document).put("mount", "/view");
                    case "raw" -> fuse(document).put("backing", "/backing");
                    case "mode" -> fault(document).put("mode", "enospc");
                    case "hash" -> fuse(document).remove("manifest_sha256");
                    case "platform" -> fuse(document).put("platform", "linux/amd64");
                    case "threshold" -> checkpointing(document).put("small_file_threshold", 1);
                    case "concurrent" -> checkpointing(document).put("max_concurrent", 2);
                    case "tolerance" -> checkpointing(document).put("tolerable_failures", 11);
                }
            });
        }
    }

    @Test void rejectsMissingControlsUnpinnedRuntimeInvalidBudgetAndInsufficientInputBeforeArtifacts() {
        for (String change : List.of("storage", "threshold", "concurrent", "tolerance", "runtime", "identity", "path", "absolute", "timeout", "lease", "short-input")) {
            failure(SpecificationException.Stage.RUNNER_CAPABILITY, document -> {
                switch (change) {
                    case "storage" -> ((ObjectNode) checkpointing(document).path("storage")).remove("fuse");
                    case "threshold" -> checkpointing(document).remove("small_file_threshold");
                    case "concurrent" -> checkpointing(document).remove("max_concurrent");
                    case "tolerance" -> checkpointing(document).remove("tolerable_failures");
                    case "runtime" -> ((ObjectNode) document.at("/setup/flink")).remove("image_id");
                    case "identity" -> {
                        fuse(document).put("image_id", "sha256:" + "b".repeat(64));
                        ((ObjectNode) document.at("/setup/flink")).put("image_id", "sha256:" + "b".repeat(64));
                    }
                    case "path" -> fuse(document).put("bundle", "../elsewhere");
                    case "absolute" -> fuse(document).put("bundle", "/tmp/helper");
                    case "timeout" -> fault(document).put("timeout", "121s");
                    case "lease" -> fault(document).put("lease", "89s");
                    case "short-input" -> ((ObjectNode) document.at("/setup/kafka/clusters/main/topics/0/input_source")).put("total", 3);
                }
            });
        }
    }

    @Test void rejectsLoopsMultipleFaultsAndCompetingKills() {
        failure(SpecificationException.Stage.RUNNER_CAPABILITY, document -> steps(document).add(steps(document).get(1).deepCopy()));
        failure(SpecificationException.Stage.RUNNER_CAPABILITY, CheckpointStoragePlanCompilerTest::addKillRestart);
        failure(SpecificationException.Stage.RUNNER_CAPABILITY, document -> {
            var atomic = steps(document).remove(1);
            steps(document).addObject().putObject("loop").put("times", 1).putArray("steps").add(atomic);
        });
    }

    @Test void rejectsCanonicalAndDeprecatedConfigurationOverrides() {
        for (String key : List.of("execution.checkpointing.data-inline-threshold", "state.storage.fs.memory-threshold",
                "state.backend.fs.memory-threshold", "execution.checkpointing.max-concurrent-checkpoints",
                "execution.checkpointing.tolerable-failed-checkpoints")) {
            failure(SpecificationException.Stage.RUNNER_CAPABILITY, document ->
                    ((ObjectNode) document.at("/setup/flink")).putObject("config").put(key, "1"));
            failure(SpecificationException.Stage.RUNNER_CAPABILITY, document ->
                    ((ObjectNode) document.at("/workload/jobs/0")).putArray("program_args").add("--" + key + "=1"));
        }
    }

    @Test void typedBudgetAndControlRecordsCannotBypassTheirBounds() {
        assertThrows(IllegalArgumentException.class, () -> new ExecutableScenarioPlan.CheckpointStorageFault(
                "state-job", Duration.ofSeconds(121), Duration.ofSeconds(120)));
        assertThrows(IllegalArgumentException.class, () -> new ExecutableScenarioPlan.CheckpointControls(1, 1, 10));
        assertThrows(IllegalArgumentException.class, () -> new ExecutableScenarioPlan.CheckpointControls(0, 2, 10));
        assertThrows(IllegalArgumentException.class, () -> new ExecutableScenarioPlan.CheckpointControls(0, 1, 0));
    }

    private void failure(SpecificationException.Stage stage, Consumer<ObjectNode> changes) {
        var failure = assertThrows(SpecificationException.class, () -> compile(changes));
        assertEquals(stage, failure.stage(), () -> failure.diagnostics().toString());
    }

    private ExecutableScenarioPlan compile(Consumer<ObjectNode> changes) {
        try {
            Path source = Path.of(getClass().getResource("/spec/valid/checkpoint-storage-fault.yaml").toURI());
            var document = loader.loadScenario(source).document();
            changes.accept(document);
            Path expected = Path.of(getClass().getResource("/spec/valid/checkpoint-storage-fault.expected.yaml").toURI());
            var resolved = new ScenarioPlanResolver().resolve(new ScenarioBundle(loader.validateScenarioDocument(source, document),
                    loader.loadExpectedResult(expected)), ResolutionRequest.none());
            return new ExecutableScenarioPlanCompiler().compile(resolved);
        } catch (java.net.URISyntaxException exception) { throw new AssertionError(exception); }
    }

    private static void addKillRestart(ObjectNode document) {
        steps(document).addObject().putObject("kill").putObject("target")
                .put("kind", "named").put("role", "taskmanager").put("name", "taskmanager-1");
        steps(document).addObject().putObject("restart").put("component", "taskmanager");
    }
    private static ArrayNode steps(ObjectNode document) { return (ArrayNode) document.at("/phases/0/steps"); }
    private static ObjectNode checkpointing(ObjectNode document) { return (ObjectNode) document.at("/workload/jobs/0/checkpointing"); }
    private static ObjectNode fuse(ObjectNode document) { return (ObjectNode) checkpointing(document).at("/storage/fuse"); }
    private static ObjectNode fault(ObjectNode document) { return (ObjectNode) steps(document).get(1).path("checkpoint_storage_fault"); }
}
