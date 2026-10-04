package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.artifact.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.spec.document.*;
import org.savonitar.flink.stability.core.spec.resolution.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import static org.junit.jupiter.api.Assertions.*;

class CustomRuntimePlanTest {
    @TempDir Path artifacts;
    final SpecificationLoader loader = new SpecificationLoader();
    final ExecutableScenarioPlanCompiler compiler = new ExecutableScenarioPlanCompiler();

    private Path fixture(String name) throws Exception {
        return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
    }

    private ResolvedScenarioPlan resolved(Consumer<ObjectNode> edit) throws Exception {
        var original = loader.loadScenario(fixture("custom-runtime.yaml"));
        var document = original.document();
        edit.accept(document);
        return new ScenarioPlanResolver().resolve(new ScenarioBundle(
                loader.validateScenarioDocument(original.source(), document),
                loader.loadExpectedResult(fixture("custom-runtime.expected.yaml"))), ResolutionRequest.none());
    }

    @Test void customSubjectResolvesOfflineAndBindsEverySubstitution() throws Exception {
        var manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
        manifest.getMainAttributes().putValue("Flink-Stability-Workload-Protocol", "v1");
        try (var ignored = new JarOutputStream(Files.newOutputStream(artifacts.resolve("workload.jar")), manifest)) {}
        var resolved = resolved(doc -> {});
        var plan = compiler.compile(resolved);
        assertEquals("2.3", plan.flink().declaredLine().orElseThrow());
        assertEquals("vendor-startup", plan.flink().logMarkers().getFirst().name());
        assertEquals("/opt/flink/lib/flink-dist_2.12-2.3-vendor.jar",
                plan.flink().expectedRuntimeJar().orElseThrow().containerPath());
        assertEquals("generic-kraft", plan.kafka().launchType());
        assertTrue(plan.kafka().transactionVersionBrokerDefault());
        assertTrue(plan.kafka().transactionVersion().isEmpty());
        assertEquals(Map.of("transaction.two.phase.commit.enable", "true"), plan.kafka().brokerConfig());
        assertEquals(1, plan.kafka().runtimeTarget().resolvedLaunches().size());
        var config = plan.job().materializeFlinkConfiguration("kafka:9092", Map.of(0, 10L), 1, "abcdef01");
        assertEquals("true", config.get("execution.checkpointing.unaligned.enabled"));
        assertEquals("200", config.get("pipeline.auto-watermark-interval"));
        assertEquals("connector-default", config.get("flink-stability.workload.v1.sink.transaction-id-naming-strategy"));
        try (var prepared = new ArtifactPlanResolver().resolve(resolved, new ArtifactResolutionOptions(artifacts, true))) {
            var bound = compiler.bind(prepared, plan);
            assertEquals(plan.flink().config(), bound.flinkRuntimeTarget().config());
            assertEquals(plan.flink().declaredLine(), bound.flinkRuntimeTarget().declaredLine());
            assertEquals(plan.flink().expectedRuntimeJar(), bound.flinkRuntimeTarget().expectedRuntimeJar());
            assertEquals(plan.flink().logMarkers(), bound.flinkRuntimeTarget().logMarkers());
            assertEquals(1, bound.connectorBundle().imageConnectors().size());
            assertEquals("/opt/flink/lib/vendor-connector.jar", bound.connectorBundle().imageConnectors().getFirst().containerPath());
            assertEquals("image", prepared.connectorPrimaries().getFirst().origin());
            assertNull(prepared.connectorPrimaries().getFirst().observedSha256());
        }
    }

    @Test void rejectsReservedFlinkKeysBeforeArtifactsOrDocker() throws Exception {
        for (String key : List.of("jobmanager.rpc.address", "taskmanager.numberOfTaskSlots",
                "execution.checkpointing.storage", "execution.checkpointing.interval", "parallelism.default",
                "state.backend.type", "high-availability", "high-availability.cluster-id",
                "env.java.opts.taskmanager", "flink-stability.workload.protocol")) {
            rejects(doc -> ((ObjectNode)doc.at("/setup/flink/config")).put(key, "forbidden"),
                    "runner.flink.config-reserved-key");
        }
    }

    @Test void logMarkerDeclarationsResolveAndRejectInvalidPatternsOrDuplicateNames() throws Exception {
        var plan = compiler.compile(resolved(doc -> ((ObjectNode)doc.at("/setup/flink")).putArray("log_markers")
                .addObject().put("name", "patched").put("regex", "CUSTOM-FIX (?<id>[0-9]+)").put("scope", "taskmanager").put("required", true)));
        assertEquals("patched", plan.flink().logMarkers().getFirst().name());
        assertTrue(plan.flink().logMarkers().getFirst().required());
        for (String value : List.of("[", " ")) rejects(doc -> ((ObjectNode)doc.at("/setup/flink")).putArray("log_markers")
                .addObject().put("name", "marker").put("regex", value).put("scope", "taskmanager"), "runner.flink.log-marker-invalid");
        rejects(doc -> {
            var markers = ((ObjectNode)doc.at("/setup/flink")).putArray("log_markers");
            for (int i = 0; i < 2; i++) markers.addObject().put("name", "duplicate").put("regex", "x").put("scope", "jobmanager");
        }, "runner.flink.log-marker-invalid");
        rejects(doc -> ((ObjectNode)doc.at("/setup/flink")).putArray("log_markers").addObject()
                .put("name", "bad name").put("regex", "x").put("scope", "jobmanager"), "runner.flink.log-marker-invalid");
    }

    @Test void logMarkerShapeRejectsMissingScopeAndNonbooleanRequired() {
        for (String invalid : List.of("scope", "required")) {
            var failure = assertThrows(SpecificationException.class, () -> resolved(doc -> {
                var marker = ((ObjectNode)doc.at("/setup/flink")).putArray("log_markers").addObject()
                        .put("name", "marker").put("regex", "x");
                if (invalid.equals("required")) marker.put("scope", "taskmanager").put("required", "yes");
            }));
            assertTrue(failure.diagnostics().stream().anyMatch(issue -> issue.path().contains("log_markers")));
        }
    }

    @Test void rejectsInvalidConfigurationAndLaunchInputs() throws Exception {
        rejects(doc -> ((ObjectNode)doc.at("/setup/flink/config")).put("bad:key", "value"), "runner.flink.config-invalid");
        rejects(doc -> ((ObjectNode)doc.at("/setup/flink/config")).put("safe.key", "value\ninjected: yes"), "runner.flink.config-invalid");
        rejects(doc -> ((ObjectNode)doc.at("/setup/kafka/clusters/main/launch")).put("type", "unknown"), "runner.kafka.launch-unsupported");
        rejects(doc -> ((ObjectNode)doc.at("/setup/kafka/clusters/main/broker_config")).put("bad:key", "value"), "runner.kafka.config-invalid");
        rejects(doc -> ((ObjectNode)doc.at("/setup/kafka/clusters/main/broker_config")).put("safe.key", "x\ny"), "runner.kafka.config-invalid");
        for (String key : List.of("listeners", "node.id", "cluster.id", "log.dirs", "controller.quorum.voters"))
            rejects(doc -> ((ObjectNode)doc.at("/setup/kafka/clusters/main/broker_config")).put(key, "forbidden"),
                    "runner.kafka.config-reserved-key");
    }

    @Test void genericLayoutDefaultsToApacheAndResolvesConfluentCommands() throws Exception {
        var defaultPlan = compiler.compile(resolved(doc -> {}));
        assertEquals("apache", defaultPlan.kafka().layout());
        assertEquals("apache", defaultPlan.kafka().runtimeTarget().resolvedLaunches().getFirst().layout());
        for (String layout : List.of("apache", "confluent-platform")) {
            var plan = compiler.compile(resolved(doc ->
                    ((ObjectNode)doc.at("/setup/kafka/clusters/main/launch")).put("layout", layout)));
            assertEquals(layout, plan.kafka().layout());
            assertEquals(layout, plan.kafka().runtimeTarget().resolvedLaunches().getFirst().layout());
            String executable = "apache".equals(layout) ? "/opt/kafka/bin/kafka-storage.sh" : "/usr/bin/kafka-storage";
            assertTrue(plan.kafka().runtimeTarget().resolvedLaunches().getFirst().command().getLast().contains(executable));
        }
    }

    @Test void rejectsUnknownOrInapplicableKafkaLayoutWithExactReasons() throws Exception {
        rejects(doc -> ((ObjectNode)doc.at("/setup/kafka/clusters/main/launch")).put("layout", "unknown"),
                "runner.kafka.layout-unsupported");
        for (String layout : List.of("apache", "confluent-platform")) {
            rejects(doc -> ((ObjectNode)doc.at("/setup/kafka/clusters/main/launch"))
                    .put("type", "apache-kafka").put("layout", layout), "runner.kafka.layout-not-applicable");
        }
    }

    @Test void apacheLauncherRejectsAmbiguousAndWrapperOwnedPropertyKeysWithExactReasons() throws Exception {
        for (String key : List.of("custom..flag", "custom._flag", "custom_-flag")) {
            rejects(doc -> {
                ((ObjectNode)doc.at("/setup/kafka/clusters/main/launch")).put("type", "apache-kafka");
                ((ObjectNode)doc.at("/setup/kafka/clusters/main/broker_config")).put(key, "literal");
            }, "runner.kafka.config-invalid");
        }
        for (String key : List.of("opts", "heap.opts", "version", "log4j.loggers", "jmx.hostname")) {
            rejects(doc -> {
                ((ObjectNode)doc.at("/setup/kafka/clusters/main/launch")).put("type", "apache-kafka");
                ((ObjectNode)doc.at("/setup/kafka/clusters/main/broker_config")).put(key, "literal");
            }, "runner.kafka.config-reserved-key");
        }
        var generic = compiler.compile(resolved(doc -> {
            ((ObjectNode)doc.at("/setup/kafka/clusters/main/broker_config")).put("custom..flag", "literal");
            ((ObjectNode)doc.at("/setup/kafka/clusters/main/broker_config")).put("heap.opts", "literal");
        }));
        assertEquals("literal", generic.kafka().runtimeTarget().resolvedLaunches().getFirst().brokerProperties().get("custom..flag"));
        assertEquals("literal", generic.kafka().runtimeTarget().resolvedLaunches().getFirst().brokerProperties().get("heap.opts"));
    }

    private void rejects(Consumer<ObjectNode> edit, String code) throws Exception {
        var failure = assertThrows(SpecificationException.class, () -> compiler.compile(resolved(edit)));
        assertTrue(failure.diagnostics().stream().anyMatch(d -> d.code().equals(code)), failure.getMessage());
    }
}
