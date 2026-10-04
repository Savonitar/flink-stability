package org.savonitar.flink.stability.core.execution.plan;

import org.savonitar.flink.stability.core.artifact.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.artifact.PreparedScenarioPlan;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler;
import org.savonitar.flink.stability.core.execution.plan.PreparedExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.FlinkRuntimeTarget;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.savonitar.flink.stability.core.spec.document.SpecificationAssertions.assertFailsAt;

class ExecutableScenarioPlanCompilerTest {
    private final SpecificationLoader loader = new SpecificationLoader();
    private final ExecutableScenarioPlanCompiler compiler =
            new ExecutableScenarioPlanCompiler();

    @TempDir
    Path artifactRoot;

    @Test void savepointRestoreRequiresOneFinalEosTransitionWithinCapacityAndSupportedStrategy() {
        for (String invalid : List.of("none", "capacity", "overflow", "reverse", "non-eos", "later", "loop", "wrong-job", "long-timeout")) {
            Runnable compile = () -> {
                var plan = compiler.compile(resolved(document -> {
                    ((ObjectNode)document.at("/setup/flink")).put("taskmanagers",2);
                    var sink=(ObjectNode)document.at("/workload/jobs/0/sink");
                    if(invalid.equals("reverse"))sink.put("transaction_id_naming_strategy","POOLING");
                    if(invalid.equals("non-eos")){sink.put("delivery_guarantee","AT_LEAST_ONCE");sink.remove(List.of("transactional_id_prefix","transaction_id_naming_strategy"));}
                    var steps=replaceSteps(document);
                    if(invalid.equals("loop"))steps=steps.addObject().putObject("loop").put("times",2).putArray("steps");
                    steps.addObject().putObject("savepoint_restore").put("job",invalid.equals("wrong-job")?"other":"eos-job")
                            .put("parallelism",invalid.equals("overflow")?4294967300L:invalid.equals("capacity")?100:4).put("transaction_id_naming_strategy","INCREMENTING")
                            .put("timeout",invalid.equals("long-timeout")?"6m":"3m");
                    if(invalid.equals("later"))steps.addObject().putObject("wait").put("duration","1s");
                }));
                assertEquals(4,((ExecutableScenarioPlan.SavepointRestore)plan.phases().getFirst().steps().getFirst()).parallelism());
            };
            if(invalid.equals("none"))compile.run();else assertThrows(SpecificationException.class,compile::run,invalid);
        }
    }

    @Test void atLeastOnceOracleIsExplicitAndRequiresAnAtLeastOnceSink() {
        java.util.function.Consumer<ObjectNode> aloSink = document -> {
            var sink = (ObjectNode)document.at("/workload/jobs/0/sink");
            sink.put("delivery_guarantee", "AT_LEAST_ONCE");
            sink.remove(List.of("transactional_id_prefix", "transaction_id_naming_strategy"));
        };
        var strict = compiler.compile(resolved(aloSink));
        assertEquals(ExecutableScenarioPlan.IdSetMode.EXACTLY_ONCE,
                strict.terminalValidation().mode());
        var alo = compiler.compile(resolved(document -> {
            aloSink.accept(document);
            ((ObjectNode)document.at("/terminal_validations/0")).put("mode", "at-least-once");
        }));
        assertEquals(ExecutableScenarioPlan.IdSetMode.AT_LEAST_ONCE,
                alo.terminalValidation().mode());
        assertThrows(SpecificationException.class, () -> compiler.compile(resolved(document ->
                ((ObjectNode)document.at("/terminal_validations/0")).put("mode", "at-least-once"))));
    }

    @Test void compilesPacketTargetsAndRejectsInvalidNamespaceOrParameters() {
        for (String mode : List.of("loss", "delay", "blackhole")) {
            var plan = resolved(document -> {
                ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
                document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode)topic).put("replication_factor",3));
                var packet = replaceSteps(document).addObject().putObject("packet_fault").put("taskmanager","taskmanager-1")
                        .put("mode",mode).put("duration","45s").put("timeout","2m");
                if (mode.equals("loss")) packet.put("loss_percent",25);
                if (mode.equals("delay")) packet.put("delay_ms",100).put("jitter_ms",20);
                var target = packet.putObject("target").put("kind","selector").put("role","broker").put("cluster","main");
                if (mode.equals("delay")) target.put("type","all-brokers");
                else target.put("type","partition-leader").put("topic","output").put("partition",0);
            });
            var packet = (ExecutableScenarioPlan.PacketFault)compiler.compile(plan).phases().getFirst().steps().getFirst();
            assertEquals(org.savonitar.flink.stability.runtime.api.PacketFaultControl.IMAGE,packet.request().image());
            assertEquals(mode.equals("delay") ? 2 : 0,packet.request().additionalBrokers().size());
        }
        for (String invalid : List.of("undeclared", "wrong-cluster", "missing-loss", "all-loss", "short-timeout")) {
            assertThrows(SpecificationException.class, () -> compiler.compile(resolved(document -> {
                ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers",3);
                document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode)topic).put("replication_factor",3));
                var packet = replaceSteps(document).addObject().putObject("packet_fault").put("taskmanager",invalid.equals("undeclared")?"taskmanager-2":"taskmanager-1")
                        .put("mode","loss").put("duration","45s").put("timeout",invalid.equals("short-timeout")?"1s":"2m");
                if (!invalid.equals("missing-loss")) packet.put("loss_percent",25);
                var target=packet.putObject("target").put("kind","selector").put("role","broker").put("cluster",invalid.equals("wrong-cluster")?"other":"main");
                if (invalid.equals("all-loss")) target.put("type","all-brokers");
                else target.put("type","partition-leader").put("topic","output").put("partition",0);
            })), invalid);
        }
    }

    @Test void compilesRuntimeBrokerSelectorsAndRejectsWrongReferencesAndBounds() {
        var valid = resolved(document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
            document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode) topic).put("replication_factor", 3));
            var fault = replaceSteps(document).addObject().putObject("broker_fault").put("mode", "pause").put("duration", "20s").put("timeout", "2m");
            fault.putObject("target").put("kind", "selector").put("role", "broker").put("cluster", "main")
                    .put("type", "transaction-coordinator").put("job", "eos-job");
        });
        var fault = (ExecutableScenarioPlan.BrokerFault) compiler.compile(valid).phases().getFirst().steps().getFirst();
        assertEquals(org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.Action.PAUSE, fault.request().action());
        assertEquals(org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR, fault.request().target().kind());
        assertThrows(SpecificationException.class, () -> resolved(document -> {
            var node = replaceSteps(document).addObject().putObject("broker_fault").put("mode", "kill").put("duration", "1s").put("timeout", "2m");
            node.putObject("target").put("kind", "selector").put("role", "broker").put("cluster", "absent")
                    .put("type", "partition-leader").put("topic", "output").put("partition", 500);
        }));
    }

    @Test void requiresExplicitTransactionVersionAndIncrementingIdsForCommitWitness() {
        for (boolean complete : List.of(true, false)) {
            var plan = resolved(document -> {
                ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
                if (complete) ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("transaction_version", 2);
                ((ObjectNode) document.at("/workload/jobs/0/sink")).put("transaction_id_naming_strategy", "INCREMENTING");
                document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode) topic).put("replication_factor", 3));
                var fault = replaceSteps(document).addObject().putObject("broker_fault").put("mode", "kill").put("duration", "45s").put("timeout", "2m").put("require_commit", true);
                fault.putObject("target").put("kind", "selector").put("role", "broker").put("cluster", "main").put("type", "transaction-coordinator").put("job", "eos-job");
            });
            if (complete) assertEquals(2, ((ExecutableScenarioPlan.BrokerFault) compiler.compile(plan).phases().getFirst().steps().getFirst()).request().commitTransactionVersion());
            else assertThrows(SpecificationException.class, () -> compiler.compile(plan));
        }
    }

    @Test
    void compilesThreeBrokersWithNamedKillRestartAndRejectsUnhealedOrWrongTopology() {
        for (boolean heal : List.of(true, false)) {
            var resolved = resolved(document -> {
                ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
                document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode) topic).put("replication_factor", 3));
                var steps = replaceSteps(document);
                steps.addObject().putObject("kill").putObject("target").put("kind", "named").put("role", "broker").put("name", "broker-1");
                if (heal) steps.addObject().putObject("restart").put("component", "kafka").put("name", "broker-1");
            });
            if (!heal) {
                assertTrue(assertThrows(SpecificationException.class, () -> compiler.compile(resolved)).diagnostics().stream()
                        .anyMatch(issue -> issue.code().equals("runner.phase.broker-kill-unhealed")));
            } else {
                var plan = compiler.compile(resolved);
                assertEquals(3, plan.kafka().runtimeTarget().brokers());
                assertEquals("2", plan.kafka().brokerPolicy().kafkaConfiguration().get("min.insync.replicas"));
                assertEquals(new ExecutableScenarioPlan.BrokerOperation("broker-1", false), plan.phases().getFirst().steps().getFirst());
                assertEquals(new ExecutableScenarioPlan.BrokerOperation("broker-1", true), plan.phases().getFirst().steps().getLast());
            }
        }
        var single = resolved(document -> {
            var steps = replaceSteps(document);
            steps.addObject().putObject("kill").putObject("target").put("kind", "named").put("role", "broker").put("name", "broker-1");
            steps.addObject().putObject("restart").put("component", "kafka").put("name", "broker-1");
        });
        assertThrows(SpecificationException.class, () -> compiler.compile(single));
        var tooMany = resolved(document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
            document.at("/setup/kafka/clusters/main/topics").forEach(topic ->
                    ((ObjectNode) topic).put("replication_factor", 3).put("partitions", 129));
        });
        assertTrue(assertThrows(SpecificationException.class, () -> compiler.compile(tooMany)).diagnostics().stream()
                .anyMatch(issue -> issue.code().equals("runner.kafka.partition-bound-exceeded")));
    }

    @Test
    void submittedJobFaultRequiresExplicitJobProofAndSupportedMode() throws IOException {
        for (String scope : List.of("bootstrap", "submitted-job")) {
            for (String mode : List.of("delay", "fail", "linkage-error")) {
                var resolved = resolved(document -> {
                    enableHa(document);
                    ((ObjectNode) document.at("/setup/flink")).putObject("token_provider")
                            .put("renewal_interval", "2s").put("proof_scope", scope);
                    var token = replaceSteps(document).addObject().putObject("leader_fault")
                            .put("mode", "isolate-zookeeper").put("duration", "15s").put("timeout", "2m")
                            .putObject("token_fault").put("mode", mode).put("target", "submitted-job");
                    if (mode.equals("delay")) token.put("delay", "5s");
                });
                if (scope.equals("submitted-job") && !mode.equals("linkage-error")) {
                    var fault = (ExecutableScenarioPlan.LeaderFault) compiler.compile(resolved).phases().getFirst().steps().getFirst();
                    assertTrue(fault.request().tokenFault().orElseThrow().submittedJob());
                } else {
                    assertThrows(SpecificationException.class, () -> compiler.compile(resolved));
                }
            }
        }
    }

    @Test
    void bindsHaAndSyntheticTokensWithoutChangingTheWorkloadContract() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        ResolvedScenarioPlan resolved = resolved(document -> {
            useLocalArtifacts(document);
            enableHa(document);
            ((ObjectNode) document.at("/setup/flink")).putObject("token_provider")
                    .put("renewal_interval", "2s").put("retry_backoff", "3s");
            ObjectNode fault = replaceSteps(document).addObject().putObject("leader_fault")
                    .put("mode", "isolate-zookeeper").put("duration", "15s").put("timeout", "2m");
            fault.putObject("token_fault").put("mode", "delay").put("delay", "5s");
        });
        ExecutableScenarioPlan plan = compiler.compile(resolved);
        assertEquals(2, plan.flink().jobmanagers());
        assertEquals(Optional.of(Duration.ofSeconds(3)),
                plan.flink().tokenProvider().orElseThrow().retryBackoff());
        assertTrue(plan.flink().tokenProvider().orElseThrow().proofScope().isEmpty());
        assertEquals(3, plan.flink().expectedComponents().size());
        var step = (ExecutableScenarioPlan.LeaderFault) plan.phases().getFirst().steps().getFirst();
        assertEquals(org.savonitar.flink.stability.runtime.api.FlinkHaControl.Mode.ISOLATE_ZOOKEEPER,
                step.request().mode());
        assertEquals(Duration.ofSeconds(5), step.request().tokenFault().orElseThrow().delay());
        assertTrue(step.recoveryBarrier().isEmpty(), "existing leader fault semantics are unchanged");
        try (PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(resolved,
                new ArtifactResolutionOptions(artifactRoot, true))) {
            PreparedExecutableScenarioPlan bound = compiler.bind(prepared, plan);
            assertEquals(plan.flink().highAvailability(), bound.flinkRuntimeTarget().highAvailability());
            assertEquals(plan.flink().tokenProvider(), bound.flinkRuntimeTarget().tokenProvider());
            assertEquals(2, bound.flinkRuntimeTarget().jobManagers());
        }
    }

    @Test
    void tokenProofScopeSurvivesBindingWithoutInferringItFromTheRuntimeVersion() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        for (String scope : List.of("", "bootstrap", "submitted-job")) {
            ResolvedScenarioPlan resolved = resolved(document -> {
                useLocalArtifacts(document);
                ObjectNode flink = (ObjectNode) document.at("/setup/flink");
                flink.put("image", "flink:2.4-SNAPSHOT").put("image_id", "sha256:" + "a".repeat(64));
                flink.putObject("runtime_jar")
                        .put("container_path", "/opt/flink/lib/flink-dist-2.4-SNAPSHOT.jar")
                        .put("sha256", "b".repeat(64));
                ObjectNode provider = flink.putObject("token_provider").put("renewal_interval", "2s");
                if (!scope.isEmpty()) provider.put("proof_scope", scope);
            });
            Optional<FlinkRuntimeTarget.TokenProofScope> expected = switch (scope) {
                case "bootstrap" -> Optional.of(FlinkRuntimeTarget.TokenProofScope.BOOTSTRAP);
                case "submitted-job" -> Optional.of(FlinkRuntimeTarget.TokenProofScope.SUBMITTED_JOB);
                default -> Optional.empty();
            };
            ExecutableScenarioPlan plan = compiler.compile(resolved);
            assertEquals(expected, plan.flink().tokenProvider().orElseThrow().proofScope());
            try (PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(resolved,
                    new ArtifactResolutionOptions(artifactRoot, true))) {
                PreparedExecutableScenarioPlan bound = compiler.bind(prepared, plan);
                assertEquals(expected, bound.flinkRuntimeTarget().tokenProvider().orElseThrow().proofScope());
                var differentScope = expected.isPresent()
                        ? Optional.<FlinkRuntimeTarget.TokenProofScope>empty()
                        : Optional.of(FlinkRuntimeTarget.TokenProofScope.BOOTSTRAP);
                var mismatch = bound.flinkRuntimeTarget().withTokenProvider(
                        new FlinkRuntimeTarget.TokenProvider(Duration.ofSeconds(2), Optional.empty(), differentScope));
                assertThrows(IllegalArgumentException.class, () -> new PreparedExecutableScenarioPlan(
                        prepared, plan, bound.workloadArtifact(), bound.connectorBundle(), mismatch));
            }
        }
    }

    @Test
    void recoveryBarrierIsExplicitAndRequiresSyntheticTokens() {
        for (boolean tokens : List.of(false, true)) {
            ResolvedScenarioPlan resolved = resolved(document -> {
                enableHa(document);
                if (tokens) ((ObjectNode) document.at("/setup/flink")).putObject("token_provider")
                        .put("renewal_interval", "2s");
                replaceSteps(document).addObject().putObject("leader_fault")
                        .put("mode", "isolate-zookeeper").put("duration", "15s").put("timeout", "2m")
                        .put("recovery_barrier", "token-checkpoint");
            });
            if (tokens) {
                var plan = compiler.compile(resolved);
                var fault = (ExecutableScenarioPlan.LeaderFault) plan.phases().getFirst().steps().getFirst();
                assertEquals(Optional.of(ExecutableScenarioPlan.RecoveryBarrier.TOKEN_CHECKPOINT), fault.recoveryBarrier());
                assertTrue(fault.request().tokenFault().isEmpty());
            } else {
                SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved));
                assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                        issue.code().equals("runner.phase.token-provider-required")));
            }
        }
    }

    @Test
    void rejectsMixedOrdinaryAndSynchronizedLeaderFaults() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            enableHa(document);
            ((ObjectNode) document.at("/setup/flink")).putObject("token_provider").put("renewal_interval", "2s");
            ArrayNode steps = replaceSteps(document);
            steps.addObject().putObject("leader_fault").put("mode", "isolate-zookeeper")
                    .put("duration", "15s").put("timeout", "2m").put("recovery_barrier", "token-checkpoint");
            steps.addObject().putObject("loop").put("times", 2).putArray("steps")
                    .addObject().putObject("leader_fault").put("mode", "isolate-zookeeper")
                    .put("duration", "15s").put("timeout", "2m");
        });
        SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved));
        assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                issue.code().equals("runner.phase.mixed-recovery-barriers")));
    }

    @Test
    void rejectsLeaderFaultWithoutHaAndTokenFaultWithoutTheFixture() {
        for (boolean ha : List.of(false, true)) {
            ResolvedScenarioPlan resolved = resolved(document -> {
                if (ha) enableHa(document);
                ObjectNode fault = replaceSteps(document).addObject().putObject("leader_fault")
                        .put("mode", "pause").put("duration", "15s").put("timeout", "2m");
                fault.putObject("token_fault").put("mode", "fail");
            });
            SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(resolved));
            assertTrue(failure.diagnostics().stream().anyMatch(issue -> issue.code().equals(
                    ha ? "runner.phase.token-provider-required" : "runner.phase.ha-required")));
        }
    }

    @Test
    void preservesDefaultTokenRetryUnlessExplicitlyBounded() {
        ResolvedScenarioPlan defaults = resolved(document ->
                ((ObjectNode) document.at("/setup/flink")).putObject("token_provider")
                        .put("renewal_interval", "2s"));
        assertTrue(compiler.compile(defaults).flink().tokenProvider().orElseThrow()
                .retryBackoff().isEmpty());
        for (String backoff : List.of("0s", "999ms", "6m")) {
            ResolvedScenarioPlan invalid = resolved(document ->
                    ((ObjectNode) document.at("/setup/flink")).putObject("token_provider")
                            .put("renewal_interval", "2s").put("retry_backoff", backoff));
            SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(invalid));
            assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                    issue.code().equals(backoff.equals("0s")
                            ? "runner.value.duration-unsupported" : "runner.duration.out-of-range")));
        }
    }

    @Test
    void rejectsHaWithVolatileCheckpointStorageOrUnboundedFaultDuration() {
        for (boolean volatileStorage : List.of(false, true)) {
            ResolvedScenarioPlan resolved = resolved(document -> {
                enableHa(document);
                if (volatileStorage) ((ObjectNode) document.at("/workload/jobs/0/checkpointing"))
                        .putObject("storage").put("type", "jobmanager");
                replaceSteps(document).addObject().putObject("leader_fault")
                        .put("mode", "kill").put("duration", "3m").put("timeout", "2m");
            });
            SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(resolved));
            assertTrue(failure.diagnostics().stream().anyMatch(issue -> issue.code().equals(
                    volatileStorage ? "runner.flink.ha-checkpoint-storage-required"
                            : "runner.phase.ha-timeout-invalid")));
        }
    }

    @Test
    void boundsExpandedLeaderFaultsWithoutExpandingLargeLoops() {
        for (int repetitions : List.of(3, 100, 101, Integer.MAX_VALUE)) {
            ResolvedScenarioPlan resolved = resolved(document -> {
                enableHa(document);
                ObjectNode loop = replaceSteps(document).addObject().putObject("loop")
                        .put("times", repetitions);
                loop.putArray("steps").addObject().putObject("leader_fault")
                        .put("mode", "pause").put("duration", "15s").put("timeout", "2m");
            });
            if (repetitions <= 100) {
                compiler.compile(resolved);
            } else {
                SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                        () -> compiler.compile(resolved));
                assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                        issue.code().equals("runner.phase.ha-fault-count-unsupported")));
            }
        }
    }

    @Test
    void rejectsLeaderFaultWhileTaskManagerKillRemainsUnhealed() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            enableHa(document);
            ArrayNode steps = replaceSteps(document);
            addKill(steps, "taskmanager-1");
            steps.addObject().putObject("leader_fault")
                    .put("mode", "pause").put("duration", "15s").put("timeout", "2m");
            steps.addObject().putObject("restart").put("component", "taskmanager");
        });
        SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                () -> compiler.compile(resolved));
        assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                issue.code().equals("runner.phase.ha-taskmanager-overlap-unsupported")));
    }

    @Test
    void reservesHaNetworkAliasesIncludingFutureIncarnations() {
        for (String host : List.of("jobmanager-2", "jobmanager-1-1", "jobmanager-2-101",
                "flink-zookeeper")) {
            ResolvedScenarioPlan resolved = resolved(document -> {
                enableHa(document);
                routeSinkThroughProxy(document);
                ((ObjectNode) document.at("/setup/proxies/kafka-proxy"))
                        .put("listen", host + ":9092");
            });
            SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(resolved));
            assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                    issue.code().equals("runner.kafka.proxy-listen-unsupported")));
        }
    }

    private static void enableHa(ObjectNode document) {
        ObjectNode flink = (ObjectNode) document.at("/setup/flink");
        flink.put("jobmanagers", 2).putObject("high_availability")
                .put("zookeeper_image", "zookeeper:3.9.3").put("session_timeout", "6s");
        ((ObjectNode) document.at("/workload/jobs/0/checkpointing"))
                .putObject("storage").put("type", "filesystem");
    }

    @Test
    void compilesTheMinimalPlainScenarioIntoTypedRuntimeValues() {
        ResolvedScenarioPlan resolved = resolved(document -> {});

        ExecutableScenarioPlan plan = compiler.compile(resolved);

        assertSame(resolved, plan.sourcePlan());
        assertTrue(plan.flink().expectedImageId().isEmpty());
        assertTrue(plan.flink().expectedRuntimeJar().isEmpty());
        assertEquals("minimal", plan.scenarioName());
        assertEquals(new ExecutableScenarioPlan.InvocationPolicy(1, 0), plan.invocation());
        assertEquals("main", plan.kafka().alias());
        assertEquals("apache/kafka:4.0.0", plan.kafka().imageReference());
        assertEquals(ExecutableScenarioPlan.KafkaMode.KRAFT, plan.kafka().mode());
        assertTrue(plan.kafka().transactionVersion().isEmpty());
        assertEquals(Duration.ofHours(2),
                plan.kafka().brokerPolicy().transactionMaxTimeout());
        assertEquals(Map.of(
                        "group.initial.rebalance.delay.ms", "0",
                        "offsets.topic.replication.factor", "1",
                        "transaction.max.timeout.ms", "7200000",
                        "transaction.state.log.min.isr", "1",
                        "transaction.state.log.replication.factor", "1"),
                plan.kafka().brokerPolicy().kafkaConfiguration());
        assertEquals(2, plan.kafka().topics().size());
        assertEquals(10, plan.input().totalRecords());
        assertEquals("eos-job", plan.job().alias());
        assertEquals(ExecutableScenarioPlan.StartMode.AUTO, plan.job().startMode());
        assertEquals(Duration.ofSeconds(5), plan.job().checkpointing().interval());
        assertEquals(ExecutableScenarioPlan.CheckpointMode.EXACTLY_ONCE,
                plan.job().checkpointing().mode());
        assertEquals(ExecutableScenarioPlan.CheckpointStorage.JOBMANAGER,
                plan.job().checkpointing().storage());
        assertEquals(ExecutableScenarioPlan.StateBackend.HASHMAP,
                plan.job().stateBackend());
        assertEquals(ExecutableScenarioPlan.RestartStrategy.FLINK_DEFAULT,
                plan.job().restartStrategy());
        assertEquals(Map.of(
                        "execution.checkpointing.interval", "5000",
                        "execution.checkpointing.mode", "EXACTLY_ONCE",
                        "execution.checkpointing.storage", "jobmanager",
                        "parallelism.default", "1",
                        "state.backend.type", "hashmap"),
                plan.job().standardFlinkConfiguration());
        assertFalse(plan.job().standardFlinkConfiguration()
                .containsKey("restart-strategy.type"));
        assertFalse(plan.job().stateTtl().enabled());
        assertEquals(ExecutableScenarioPlan.WatermarkStrategy.NO_WATERMARKS,
                plan.job().watermarks().strategy());
        assertEquals(Duration.ofMinutes(2), plan.jobCompletionTimeout());
        assertEquals(Duration.ofMinutes(2), plan.terminalValidation().timeout());
        assertEquals(ExecutableScenarioPlan.ExpectedOutcome.pass(), plan.expectedOutcome());
        assertEquals(ExecutableScenarioPlan.Sink.exactlyOnce(
                        new ExecutableScenarioPlan.TopicReference("main", "output"),
                        "minimal",
                        ExecutableScenarioPlan.TransactionIdNamingStrategy.INCREMENTING),
                plan.job().sink());
        assertTrue(plan.phases().getFirst().steps().getFirst()
                instanceof ExecutableScenarioPlan.AwaitJobState);
    }

    @Test
    void bindsDistributedTopologyAndPreservesParallelismThroughSubmissionConfiguration()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        ResolvedScenarioPlan resolved = resolved(document -> {
            useLocalArtifacts(document);
            ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
            ((ObjectNode) document.at("/workload/jobs/0")).put("parallelism", 4);
            for (var topic : document.at("/setup/kafka/clusters/main/topics")) {
                ((ObjectNode) topic).put("partitions", 4);
            }
            ArrayNode steps = replaceSteps(document);
            addKill(steps, "taskmanager-2");
            steps.addObject().putObject("restart").put("component", "taskmanager")
                    .put("name", "taskmanager-2");
        });
        ExecutableScenarioPlan plan = compiler.compile(resolved);
        assertEquals(2, plan.flink().taskmanagers());
        assertEquals(3, plan.flink().expectedComponents().size());
        assertEquals(4, plan.job().parallelism());
        assertEquals(4, plan.input().partitions());
        assertEquals("4", plan.job().standardFlinkConfiguration().get("parallelism.default"));
        assertEquals("4", plan.job().materializeFlinkConfiguration(
                "kafka:9092", Map.of(0, 3L, 1, 3L, 2, 2L, 3, 2L), 1, "1234abcd")
                .get("parallelism.default"));
        assertEquals(new ExecutableScenarioPlan.RestartTaskManager("taskmanager-2"),
                plan.phases().getFirst().steps().get(1));
        try (PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(artifactRoot))) {
            assertEquals(2, compiler.bind(prepared, plan).flinkRuntimeTarget().taskManagers());
        }
    }

    @Test
    void rejectsParallelismBeyondAvailableSlotsBeforeProvisioning() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
            ((ObjectNode) document.at("/workload/jobs/0")).put("parallelism", 5);
        });
        SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                () -> compiler.compile(resolved));
        assertEquals("runner.workload.insufficient-task-slots",
                failure.diagnostics().getFirst().code());
    }

    @Test
    void rejectsOversizedTopologiesBeforeAllocatingComponentMaps() {
        for (int count : List.of(17, Integer.MAX_VALUE)) {
            ResolvedScenarioPlan resolved = resolved(document ->
                    ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", count));
            SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(resolved));
            assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                    issue.code().equals("runner.flink.taskmanager-count-unsupported")));
        }
    }

    @Test
    void typedProcessOperationsRejectNoncanonicalTaskManagerNames() {
        for (String name : List.of("jobmanager-1", "taskmanager-0", "taskmanager-01",
                "taskmanager-2147483648")) {
            assertThrows(IllegalArgumentException.class,
                    () -> new ExecutableScenarioPlan.KillTaskManager(name));
            assertThrows(IllegalArgumentException.class,
                    () -> new ExecutableScenarioPlan.RestartTaskManager(name));
        }
    }

    @Test
    void requiresNamedRestartsAndTracksEveryTargetIndependently() {
        for (String restartName : List.of("", "taskmanager-1", "taskmanager-2")) {
            ResolvedScenarioPlan resolved = resolved(document -> {
                ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
                ArrayNode steps = replaceSteps(document);
                addKill(steps, "taskmanager-2");
                ObjectNode restart = steps.addObject().putObject("restart")
                        .put("component", "taskmanager");
                if (!restartName.isEmpty()) {
                    restart.put("name", restartName);
                }
            });
            if (restartName.equals("taskmanager-2")) {
                compiler.compile(resolved);
            } else {
                SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                        () -> compiler.compile(resolved));
                assertTrue(failure.diagnostics().stream().anyMatch(issue -> issue.code().equals(
                        restartName.isEmpty() ? "runner.phase.restart-target-required"
                                : "runner.phase.taskmanager-already-running")));
                assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                        issue.code().equals("runner.phase.taskmanager-kill-unhealed")));
            }
        }
    }

    @Test
    void repeatedLoopsMustBalanceEachTargetNotJustTheTotalRunningCount() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
            ArrayNode steps = replaceSteps(document);
            addKill(steps, "taskmanager-1");
            ObjectNode loop = steps.addObject().putObject("loop").put("times", 2);
            ArrayNode body = loop.putArray("steps");
            body.addObject().putObject("restart").put("component", "taskmanager")
                    .put("name", "taskmanager-1");
            addKill(body, "taskmanager-2");
            steps.addObject().putObject("restart").put("component", "taskmanager")
                    .put("name", "taskmanager-2");
        });
        SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                () -> compiler.compile(resolved));
        assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                issue.code().equals("runner.phase.loop-taskmanager-lifecycle-unstable")));
    }

    @Test
    void rejectsOverlappingKillsEvenWhenBothTargetsAreEventuallyRestarted() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
            ObjectNode loop = replaceSteps(document).addObject().putObject("loop").put("times", 3);
            ArrayNode steps = loop.putArray("steps");
            addKill(steps, "taskmanager-1");
            addKill(steps, "taskmanager-2");
            for (String name : List.of("taskmanager-2", "taskmanager-1")) {
                steps.addObject().putObject("restart").put("component", "taskmanager")
                        .put("name", name);
            }
        });
        SpecificationException failure = assertFailsAt(Stage.RUNNER_CAPABILITY,
                () -> compiler.compile(resolved));
        assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                issue.code().equals("runner.phase.taskmanager-kill-overlap-unsupported")));
    }

    private static void addKill(ArrayNode steps, String name) {
        steps.addObject().putObject("kill").putObject("target")
                .put("kind", "named").put("role", "taskmanager").put("name", name);
    }

    @Test
    void preservesExplicitTransactionFeatureVersionsWithoutChangingBrokerStartupPolicy() {
        ExecutableScenarioPlan.KafkaCluster unselected =
                compiler.compile(resolved(document -> {})).kafka();

        for (int version : List.of(1, 2)) {
            ExecutableScenarioPlan.KafkaCluster selected = compiler.compile(resolved(document ->
                    ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                            .put("transaction_version", version))).kafka();

            assertEquals(Optional.of(version), selected.transactionVersion());
            assertEquals(unselected.runtimeTarget(), selected.runtimeTarget());
        }
    }

    @Test
    void rejectsUnsupportedTransactionFeatureVersionsInTypedPlans() {
        ExecutableScenarioPlan.KafkaCluster kafka =
                compiler.compile(resolved(document -> {})).kafka();

        for (int version : List.of(-1, 0, 3)) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> new ExecutableScenarioPlan.KafkaCluster(
                            kafka.alias(), kafka.imageReference(), kafka.mode(), kafka.brokers(),
                            kafka.brokerPolicy(), kafka.topics(), Optional.of(version),
                            kafka.proxy()));
            assertEquals("transactionVersion must be 1 or 2", failure.getMessage());
        }
    }

    @Test
    void materializesTheExactWorkloadProtocolAfterInputOffsetsAreCaptured() {
        ExecutableScenarioPlan plan = compiler.compile(resolved(document -> {}));

        Map<String, String> values = plan.job().materializeFlinkConfiguration(
                "kafka:9092", Map.of(0, 10L), 2, "a1b2c3d4");

        assertEquals("5000", values.get("execution.checkpointing.interval"));
        assertEquals("hashmap", values.get("state.backend.type"));
        assertFalse(values.containsKey("execution.checkpointing.dir"));
        assertEquals("v1", values.get("flink-stability.workload.protocol"));
        assertEquals("eos-job", values.get(
                "flink-stability.workload.v1.job-alias"));
        assertEquals("kafka:9092", values.get(
                "flink-stability.workload.v1.source.bootstrap-servers"));
        assertEquals("input", values.get("flink-stability.workload.v1.source.topic"));
        assertEquals("flink-stability-v1-minimal-eos-job-2-a1b2c3d4", values.get(
                "flink-stability.workload.v1.source.group-id"));
        assertEquals("committed-or-earliest", values.get(
                "flink-stability.workload.v1.source.starting-offsets"));
        assertEquals("read_uncommitted", values.get(
                "flink-stability.workload.v1.source.isolation-level"));
        assertEquals("0:10", values.get(
                "flink-stability.workload.v1.source.stopping-offsets"));
        assertEquals("output", values.get("flink-stability.workload.v1.sink.topic"));
        assertEquals("7200000", values.get(
                "flink-stability.workload.v1.sink.transaction-timeout-ms"));
        assertEquals("false", values.get(
                "flink-stability.workload.v1.state-ttl.enabled"));
        assertEquals("no-watermarks", values.get(
                "flink-stability.workload.v1.watermarks.strategy"));
        assertEquals(values.keySet().stream().sorted().toList(),
                new ArrayList<>(values.keySet()));
        assertThrows(UnsupportedOperationException.class,
                () -> values.put("ignored", "value"));
    }

    @Test
    void materializesAnAttemptScopedFilesystemCheckpointDirectory() {
        ExecutableScenarioPlan plan = compiler.compile(resolved(document ->
                ((ObjectNode) document.at("/workload/jobs/0/checkpointing"))
                        .putObject("storage")
                        .put("type", "filesystem")));

        assertEquals(ExecutableScenarioPlan.CheckpointStorage.FILESYSTEM,
                plan.job().checkpointing().storage());
        assertEquals("filesystem", plan.job().standardFlinkConfiguration()
                .get("execution.checkpointing.storage"));
        assertFalse(plan.job().standardFlinkConfiguration()
                .containsKey("execution.checkpointing.dir"));

        Map<String, String> values = plan.job().materializeFlinkConfiguration(
                "kafka:9092", Map.of(0, 10L), 2, "a1b2c3d4");

        assertEquals("file:/flink/checkpoints/attempt-2-a1b2c3d4/eos-job",
                values.get("execution.checkpointing.dir"));
    }

    @Test
    void carriesSeparateCompletionAndPostFenceValidationTimeouts() {
        ExecutableScenarioPlan plan = compiler.compile(resolved(document -> {
            document.put("completion_timeout", "3m");
            ((ObjectNode) document.at("/terminal_validations/0")).put("timeout", "45s");
        }));

        assertEquals(Duration.ofMinutes(3), plan.jobCompletionTimeout());
        assertEquals(Duration.ofSeconds(45), plan.terminalValidation().timeout());
    }

    @Test
    void resolvedSemanticBoundaryRejectsACustomKafkaImageBeforeCompilation() {
        SpecificationException exception = assertFailsAt(
                Stage.RESOLUTION,
                () -> resolved(document ->
                        ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                                .put("image", "custom/kafka:4.0.0")));

        assertEquals(List.of("runner.kafka.image-version-unsupported"),
                exception.diagnostics().stream().map(Diagnostic::code).toList());
        assertEquals(List.of("$/setup/kafka/clusters/main/image"),
                exception.diagnostics().stream().map(Diagnostic::path).toList());
    }

    @Test
    void acceptsAnOfficialKafka40PatchPinnedBySha256Digest() {
        String image = "apache/kafka:4.0.17@sha256:" + "a".repeat(64);
        ResolvedScenarioPlan resolved = resolved(document ->
                ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                        .put("image", image));

        assertEquals(image, compiler.compile(resolved).kafka().imageReference());
    }

    @Test
    void acceptsTheExactGeneratedInputLimitAndRejectsTheFirstLargerTotal() {
        assertEquals(1_000_000L,
                ExecutableScenarioPlan
                        .FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS);
        ExecutableScenarioPlan boundary = compiler.compile(resolved(document ->
                ((ObjectNode) document.at(
                        "/setup/kafka/clusters/main/topics/0/input_source"))
                        .put("total",
                                ExecutableScenarioPlan
                                        .FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS)));

        assertEquals(
                ExecutableScenarioPlan
                        .FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS,
                boundary.input().totalRecords());

        ResolvedScenarioPlan tooLarge = resolved(document ->
                ((ObjectNode) document.at(
                        "/setup/kafka/clusters/main/topics/0/input_source"))
                        .put("total",
                                ExecutableScenarioPlan
                                        .FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS + 1));
        SpecificationException failure = assertFailsAt(
                Stage.RUNNER_CAPABILITY,
                () -> compiler.compile(tooLarge));

        assertEquals(1, failure.diagnostics().size());
        assertEquals("runner.input.total-unsupported",
                failure.diagnostics().getFirst().code());
        assertEquals(
                "$/setup/kafka/clusters/main/topics/0/input_source/total",
                failure.diagnostics().getFirst().path());
    }

    @Test
    void compilesCheckpointInProgressTimeoutAndPolicy() {
        for (String policy : List.of("fail", "inconclusive")) {
            var plan = compiler.compile(resolved(document -> {
                var phases = document.putArray("phases");
                var await = phases.addObject().put("name", "window").putArray("steps")
                        .addObject().putObject("await");
                await.putObject("condition").put("type", "checkpoint-in-progress").put("job", "eos-job");
                await.put("timeout", "1500ms").put("on_timeout", policy);
            }));
            var step = (ExecutableScenarioPlan.AwaitCheckpointInProgress) plan.phases().getFirst().steps().getFirst();
            assertEquals(Duration.ofMillis(1500), step.timeout());
            assertEquals(policy.toUpperCase(java.util.Locale.ROOT), step.onTimeout().name());
        }
    }

    @Test
    void mapsSupportedTypedOptionsAndBalancedTaskmanagerChaos() {
        ExecutableScenarioPlan plan = compiler.compile(resolved(document -> {
            ObjectNode job = (ObjectNode) document.at("/workload/jobs/0");
            ObjectNode ttl = job.putObject("state_ttl");
            ttl.put("enabled", true);
            ttl.put("ttl", "10s");
            ttl.put("cleanup", "incremental");
            ObjectNode watermarks = job.putObject("watermarks");
            watermarks.put("strategy", "bounded-out-of-orderness");
            watermarks.put("max_out_of_orderness", "250ms");
            watermarks.put("idleness", "1s");
            job.putArray("program_args").add("--processingDelayMs").add("7");

            ArrayNode phases = document.withArray("phases");
            phases.removeAll();
            ArrayNode warmup = phases.addObject().put("name", "warmup")
                    .putArray("steps");
            ObjectNode await = warmup.addObject().putObject("await");
            ObjectNode condition = await.putObject("condition");
            condition.put("type", "checkpoint-completed");
            condition.put("job", "eos-job");
            condition.put("count", 2);
            await.put("timeout", "2m");
            await.put("on_timeout", "fail");
            ArrayNode chaos = phases.addObject().put("name", "chaos")
                    .putArray("steps");
            ObjectNode loop = chaos.addObject().putObject("loop");
            loop.put("times", 3);
            ArrayNode loopSteps = loop.putArray("steps");
            ObjectNode target = loopSteps.addObject().putObject("kill").putObject("target");
            target.put("kind", "named");
            target.put("role", "taskmanager");
            target.put("name", "taskmanager-1");
            loopSteps.addObject().putObject("wait").put("duration", "100ms");
            loopSteps.addObject().putObject("restart").put("component", "taskmanager");
        }));

        assertEquals(Duration.ofSeconds(10), plan.job().stateTtl().ttl().orElseThrow());
        assertEquals(ExecutableScenarioPlan.StateTtlCleanup.INCREMENTAL,
                plan.job().stateTtl().cleanup());
        assertEquals(Duration.ofMillis(250),
                plan.job().watermarks().maxOutOfOrderness().orElseThrow());
        assertEquals(List.of("--processingDelayMs", "7"),
                plan.job().programArguments().values());
        assertTrue(plan.phases().get(1).steps().getFirst()
                instanceof ExecutableScenarioPlan.Loop);
        Map<String, String> materialized = plan.job().workloadConfiguration().materialize(
                "kafka:9092", Map.of(0, 10L), 1, "1234abcd");
        assertEquals("10000", materialized.get(
                "flink-stability.workload.v1.state-ttl.ttl-ms"));
        assertEquals("incremental", materialized.get(
                "flink-stability.workload.v1.state-ttl.cleanup"));
        assertEquals("250", materialized.get(
                "flink-stability.workload.v1.watermarks.max-out-of-orderness-ms"));
    }

    @Test
    void rejectsRocksDbCompactionCleanupWithTheHashMapRunnerBackend() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            ObjectNode ttl = ((ObjectNode) document.at("/workload/jobs/0"))
                    .putObject("state_ttl");
            ttl.put("enabled", true);
            ttl.put("ttl", "10s");
            ttl.put("cleanup", "rocksdb-compaction-filter");
        });

        SpecificationException failure = assertFailsAt(
                Stage.RUNNER_CAPABILITY,
                () -> compiler.compile(resolved));

        assertEquals("runner.workload.state-ttl-cleanup-unsupported",
                failure.diagnostics().getFirst().code());
        assertEquals("$/workload/jobs/0/state_ttl/cleanup",
                failure.diagnostics().getFirst().path());
    }

    @Test
    void preservesExternalWorkloadProgramArgumentsWithoutInterpretingThem() {
        ResolvedScenarioPlan resolved = resolved(document ->
                ((ObjectNode) document.at("/workload/jobs/0"))
                        .putArray("program_args")
                        .add("--custom-mode")
                        .add("strict")
                        .add("one value with spaces")
                        .add(" "));

        ExecutableScenarioPlan plan = compiler.compile(resolved);

        assertEquals(List.of("--custom-mode", "strict", "one value with spaces", " "),
                plan.job().programArguments().values());
    }

    @Test
    void rejectsOnlyRegisteredHarnessOwnedProgramArgumentConflicts() {
        for (String token : List.of(
                "--flink-stability.workload.protocol=v2",
                "--flink-stability.workload.v1.source.topic=other",
                "--bootstrapServers",
                "--bootstrapServers=other:9092")) {
            ResolvedScenarioPlan resolved = resolved(document ->
                    ((ObjectNode) document.at("/workload/jobs/0"))
                            .putArray("program_args")
                            .add(token));

            SpecificationException failure = assertFailsAt(
                    Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(resolved),
                    token);

            assertEquals("runner.workload.program-args-conflict",
                    failure.diagnostics().getFirst().code(), token);
            assertEquals("$/workload/jobs/0/program_args",
                    failure.diagnostics().getFirst().path(), token);
        }
    }

    @Test
    void aggregatesUnsupportedValidV1FeaturesInDeterministicPathOrder() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            document.put("runs", 2);
            document.put("health_retry_limit", 1);
            ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
            ObjectNode job = (ObjectNode) document.at("/workload/jobs/0");
            job.put("start", "manual");
            job.put("parallelism", 2);
            job.put("state_backend", "rocksdb");
            ObjectNode restartStrategy = job.putObject("restart_strategy");
            restartStrategy.put("type", "fixed-delay");
            restartStrategy.put("attempts", 1);
            restartStrategy.put("delay", "1s");
            job.putArray("program_args").add("--bootstrapServers").add("wrong:9092");
        });

        SpecificationException exception = assertFailsAt(
                Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved));

        assertEquals(List.of(
                        "$/health_retry_limit",
                        "$/runs",
                        "$/workload/jobs/0/program_args",
                        "$/workload/jobs/0/restart_strategy/type",
                        "$/workload/jobs/0/start",
                        "$/workload/jobs/0/state_backend"),
                exception.diagnostics().stream().map(Diagnostic::path).toList());
        assertEquals(exception.diagnostics(), exception.diagnostics().stream()
                .sorted(java.util.Comparator
                        .comparing((Diagnostic issue) -> issue.source().toString())
                        .thenComparing(Diagnostic::scope)
                        .thenComparing(Diagnostic::path)
                        .thenComparing(Diagnostic::code)
                        .thenComparing(Diagnostic::message))
                .toList());
    }

    @Test
    void rejectsDocumentedExactTopicConnectorAndFreeFormConfigBoundaries() {
        record CapabilityCase(
                String code,
                String path,
                Consumer<ObjectNode> mutation) {}

        List<CapabilityCase> cases = List.of(
                new CapabilityCase(
                        "runner.kafka.topic-count-unsupported",
                        "$/setup/kafka/clusters/main/topics",
                        document -> ((ArrayNode) document.at(
                                "/setup/kafka/clusters/main/topics"))
                                .addObject()
                                .put("name", "audit-extra")
                                .put("partitions", 1)
                                .put("replication_factor", 1)),
                new CapabilityCase(
                        "runner.connector.count-unsupported",
                        "$/subject/connectors",
                        document -> {
                            ObjectNode connectors = (ObjectNode) document.at(
                                    "/subject/connectors");
                            connectors.set("second", connectors.get("kafka").deepCopy());
                            ((ArrayNode) document.at("/workload/jobs/0/connectors"))
                                    .add("second");
                        }),
                new CapabilityCase(
                        "runner.flink.config-unsupported",
                        "$/setup/flink/config",
                        document -> ((ObjectNode) document.at("/setup/flink"))
                                .putObject("config")
                                .put("taskmanager.numberOfTaskSlots", 1)));

        for (CapabilityCase capabilityCase : cases) {
            SpecificationException failure = assertFailsAt(
                    Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(resolved(capabilityCase.mutation())),
                    capabilityCase.code());

            assertTrue(failure.diagnostics().stream().anyMatch(issue ->
                    issue.code().equals(capabilityCase.code())
                            && issue.path().equals(capabilityCase.path())),
                    capabilityCase.code());
        }
    }

    @Test
    void rejectsUnsupportedStepsAndAnUnhealedTaskmanagerKill() {
        ResolvedScenarioPlan unsupported = resolved(document -> {
            ArrayNode steps = replaceSteps(document);
            ObjectNode stop = steps.addObject().putObject("stop");
            ObjectNode target = stop.putObject("target");
            target.put("kind", "named");
            target.put("role", "taskmanager");
            target.put("name", "taskmanager-1");
            stop.put("duration", "1s");
            stop.put("heal", "restart-same-container");
        });
        SpecificationException unsupportedFailure = assertFailsAt(
                Stage.RUNNER_CAPABILITY, () -> compiler.compile(unsupported));
        assertEquals("runner.phase.step-unsupported",
                unsupportedFailure.diagnostics().getFirst().code());

        ResolvedScenarioPlan danglingKill = resolved(document -> {
            ObjectNode target = replaceSteps(document).addObject()
                    .putObject("kill").putObject("target");
            target.put("kind", "named");
            target.put("role", "taskmanager");
            target.put("name", "taskmanager-1");
        });
        SpecificationException lifecycleFailure = assertFailsAt(
                Stage.RUNNER_CAPABILITY, () -> compiler.compile(danglingKill));
        assertTrue(lifecycleFailure.diagnostics().stream().anyMatch(issue ->
                issue.code().equals("runner.phase.taskmanager-kill-unhealed")
                        && issue.path().equals("$/phases/0/steps/0/kill")));
    }

    @Test
    void compilesAPinnedKafkaIdSetFailureAsTheExpectation() {
        ExpectedResultSpecification expected = expected(document -> {
            ObjectNode outcome = document.withObject("default");
            outcome.removeAll();
            outcome.put("outcome", "fail");
            outcome.put("oracle", "kafka.id-set");
            outcome.put("reason", "validator.kafka.id-set.duplicate-ids");
        });

        ExecutableScenarioPlan plan = compiler.compile(resolved(document -> {}, expected));

        assertEquals(ExecutableScenarioPlan.ExpectedOutcome.failure(
                        "kafka.id-set", "validator.kafka.id-set.duplicate-ids"),
                plan.expectedOutcome());
    }

    @Test
    void rejectsAnExpectedFailureOfAnOracleTheRunnerCannotEvaluate() {
        ExpectedResultSpecification expected = expected(document -> {
            ObjectNode outcome = document.withObject("default");
            outcome.removeAll();
            outcome.put("outcome", "fail");
            outcome.put("oracle", "kafka.no-hanging-transactions");
            outcome.put("reason", "validator.kafka.transaction.ongoing-after-timeout");
        });
        ResolvedScenarioPlan resolved = resolved(document -> {
            ObjectNode transactions = ((ArrayNode) document.get("terminal_validations"))
                    .addObject();
            transactions.put("type", "kafka.no-hanging-transactions");
            transactions.put("cluster", "main");
            transactions.put("transactional_id_prefix", "minimal");
            transactions.put("stabilization_timeout", "30s");
        }, expected);

        SpecificationException exception = assertFailsAt(
                Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved));

        Diagnostic issue = exception.diagnostics().stream()
                .filter(candidate -> candidate.code()
                        .equals("runner.expectation.outcome-unsupported"))
                .findFirst()
                .orElseThrow();
        assertEquals("$/default/oracle", issue.path());
        assertEquals(expected.source().toAbsolutePath().normalize(), issue.source());
    }

    @Test
    void compilesAnAtLeastOnceSinkWithoutTransactionSettings() {
        ExecutableScenarioPlan plan = compiler.compile(resolved(document -> {
            ObjectNode sink = (ObjectNode) document.at("/workload/jobs/0/sink");
            sink.put("delivery_guarantee", "AT_LEAST_ONCE");
            sink.remove("transactional_id_prefix");
            sink.remove("transaction_id_naming_strategy");
        }));

        assertEquals(ExecutableScenarioPlan.Sink.atLeastOnce(
                        new ExecutableScenarioPlan.TopicReference("main", "output")),
                plan.job().sink());
        Map<String, String> values = plan.job().materializeFlinkConfiguration(
                "kafka:9092", Map.of(0, 10L), 1, "a1b2c3d4");
        assertEquals("AT_LEAST_ONCE",
                values.get("flink-stability.workload.v1.sink.delivery-guarantee"));
        assertTrue(values.keySet().stream().noneMatch(key -> key.startsWith(
                "flink-stability.workload.v1.sink.transaction")));
    }

    @Test
    void mapsADeclaredExactlyOnceTransactionTimeout() {
        ExecutableScenarioPlan plan = compiler.compile(resolved(document ->
                ((ObjectNode) document.at("/workload/jobs/0/sink"))
                        .put("transaction_timeout", "3s")));

        assertEquals("3000", plan.job().materializeFlinkConfiguration(
                        "kafka:9092", Map.of(0, 10L), 1, "a1b2c3d4")
                .get("flink-stability.workload.v1.sink.transaction-timeout-ms"));
    }

    @Test
    void rejectsATransactionTimeoutAboveTheBrokerMaximum() {
        ResolvedScenarioPlan resolved = resolved(document ->
                ((ObjectNode) document.at("/workload/jobs/0/sink"))
                        .put("transaction_timeout", "3h"));

        SpecificationException exception = assertFailsAt(
                Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved));

        Diagnostic issue = exception.diagnostics().getFirst();
        assertEquals("runner.workload.transaction-timeout-unsupported", issue.code());
        assertEquals("$/workload/jobs/0/sink/transaction_timeout", issue.path());
    }

    @Test
    void schemaRejectsATransactionTimeoutOnANonTransactionalSink() {
        assertFailsAt(Stage.DOCUMENT, () -> resolved(document -> {
            ObjectNode sink = (ObjectNode) document.at("/workload/jobs/0/sink");
            sink.put("delivery_guarantee", "AT_LEAST_ONCE");
            sink.remove("transactional_id_prefix");
            sink.remove("transaction_id_naming_strategy");
            sink.put("transaction_timeout", "10s");
        }));
    }

    @Test
    void rejectsASinkWithoutADeliveryGuarantee() {
        ResolvedScenarioPlan resolved = resolved(document -> {
            ObjectNode sink = (ObjectNode) document.at("/workload/jobs/0/sink");
            sink.put("delivery_guarantee", "NONE");
            sink.remove("transactional_id_prefix");
            sink.remove("transaction_id_naming_strategy");
        });

        SpecificationException exception = assertFailsAt(
                Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved));

        assertEquals(List.of("runner.workload.delivery-guarantee-unsupported"),
                exception.diagnostics().stream().map(Diagnostic::code).toList());
    }

    @Test
    void rejectsExperimentsAtTheArtifactFreeBoundary() {
        ScenarioSpecification base = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode document = base.document();
        document.put("health_retry_limit", 0);
        ObjectNode backend = document.putObject("parameters").putObject("backend");
        backend.put("type", "string");
        backend.put("default", "hashmap");
        ((ObjectNode) document.at("/workload/jobs/0")).put("state_backend", "${backend}");
        ObjectNode experiment = document.putObject("experiment");
        experiment.put("claim", "EOS holds for both state backends.");
        experiment.putArray("varies").add("backend");
        experiment.putObject("baseline").put("backend", "hashmap");
        experiment.putObject("candidate").put("backend", "rocksdb");
        ScenarioSpecification scenario = loader.validateScenarioDocument(base.source(), document);
        ExpectedResultSpecification expected = expected(expectedDocument -> {
            ObjectNode expectation = expectedDocument.withObject("default");
            expectation.removeAll();
            expectation.putObject("baseline").put("outcome", "pass");
            expectation.putObject("candidate").put("outcome", "pass");
        });
        ResolvedScenarioPlan resolved = new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected), ResolutionRequest.none());

        SpecificationException exception = assertFailsAt(
                Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved));

        assertEquals(1, exception.diagnostics().size());
        assertEquals("runner.topology.experiment-unsupported",
                exception.diagnostics().getFirst().code());
        assertEquals(ResolutionScope.COMMON, exception.diagnostics().getFirst().scope());
        assertEquals("$/experiment", exception.diagnostics().getFirst().path());
    }

    @Test
    void bindsOnlyAProtocolMarkedPreparedWorkloadAndRetainsItsWorkspaceOwner()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        ResolvedScenarioPlan resolved = resolved(this::useLocalArtifacts);
        ExecutableScenarioPlan executable = compiler.compile(resolved);

        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(artifactRoot));
        Path preparedJob;
        try {
            PreparedExecutableScenarioPlan bound = compiler.bind(prepared, executable);
            preparedJob = bound.workloadArtifact().preparedPath();
            assertSame(prepared, bound.preparedScenarioPlan());
            assertSame(executable, bound.executablePlan());
            assertTrue(Files.isRegularFile(preparedJob));
            assertEquals("flink:2.2.0", bound.flinkRuntimeTarget().imageReference());
            assertEquals(
                    bound.connectorBundle().classpathManifestSha256(),
                    bound.flinkRuntimeTarget().connectorBundle()
                            .classpathManifest().manifestSha256());
            assertEquals(List.of("kafka"), bound.connectorBundle().aliases());
        } finally {
            prepared.close();
        }
        assertFalse(Files.exists(preparedJob));
    }

    @Test
    void carriesExpectedRuntimeIdentitiesFromScenarioThroughArtifactBinding() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        String imageId = "sha256:" + "b".repeat(64);
        ResolvedScenarioPlan resolved = resolved(document -> {
            useLocalArtifacts(document);
            ((ObjectNode) document.at("/setup/flink")).put("image_id", imageId);
            ((ObjectNode) document.at("/setup/flink")).putObject("runtime_jar")
                    .put("container_path", "/opt/flink/lib/flink-dist-2.2.0.jar")
                    .put("sha256", "c".repeat(64));
        });
        ExecutableScenarioPlan executable = compiler.compile(resolved);
        assertEquals(imageId, executable.flink().expectedImageId().orElseThrow());
        assertEquals("c".repeat(64), executable.flink().expectedRuntimeJar().orElseThrow().sha256());
        try (PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(artifactRoot))) {
            var runtime = compiler.bind(prepared, executable).flinkRuntimeTarget();
            assertEquals(imageId, runtime.expectedImageId().orElseThrow());
            assertEquals(executable.flink().expectedRuntimeJar(), runtime.expectedRuntimeJar());
        }
    }

    @Test
    void rejectsAWrongSubjectWhoseDependencySuppliesTheConnectorClasses() throws IOException {
        // Review finding F3: an unrelated local primary plus the released connector as a
        // runtime dependency. Every installed byte verifies, yet the release would be tested.
        createJar(artifactRoot.resolve("connector.jar"), List.of("example.Unrelated"));
        createJar(artifactRoot.resolve("released-connector.jar"),
                ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        ResolvedScenarioPlan resolved = resolved(document -> {
            useLocalArtifacts(document);
            ((ObjectNode) document.at("/subject/connectors/kafka"))
                    .putArray("runtime_dependencies").add("released-connector.jar");
        });
        ExecutableScenarioPlan executable = compiler.compile(resolved);
        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(artifactRoot));
        try {
            SpecificationException exception = assertFailsAt(
                    Stage.RUNNER_CAPABILITY,
                    () -> compiler.bind(prepared, executable));

            assertEquals(
                    List.of("runner.subject.entry-class-conflict",
                            "runner.subject.entry-class-missing"),
                    exception.diagnostics().stream().map(Diagnostic::code).toList());
            assertTrue(exception.diagnostics().stream().allMatch(issue ->
                    issue.path().equals("$/subject/connectors/kafka/artifact")));
            assertTrue(exception.diagnostics().getFirst().message()
                    .contains("$/subject/connectors/kafka/runtime_dependencies/0"));
        } finally {
            prepared.close();
        }
    }

    @Test
    void rejectsAWorkloadJarThatShadowsTheSubjectConnectorClasses() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
        manifest.getMainAttributes().putValue(
                ExecutableScenarioPlanCompiler.WORKLOAD_PROTOCOL_ATTRIBUTE, "v1");
        createJar(artifactRoot.resolve("job.jar"), manifest, List.of(
                "example.Main", "org.apache.flink.connector.kafka.sink.KafkaSink"));
        ResolvedScenarioPlan resolved = resolved(this::useLocalArtifacts);
        ExecutableScenarioPlan executable = compiler.compile(resolved);
        PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(artifactRoot));
        try {
            SpecificationException exception = assertFailsAt(
                    Stage.RUNNER_CAPABILITY,
                    () -> compiler.bind(prepared, executable));

            Diagnostic issue = exception.diagnostics().getFirst();
            assertEquals("runner.subject.entry-class-conflict", issue.code());
            assertEquals("$/workload/jobs/0/jar", issue.path());
            assertTrue(issue.message().contains("child-first"), issue.message());
        } finally {
            prepared.close();
        }
    }

    @Test
    void rejectsVersionedSubjectClassesInPrimaryDependencyAndWorkloadJars() throws IOException {
        for (String artifact : List.of("connector.jar", "dependency.jar", "job.jar")) {
            createJar(artifactRoot.resolve("connector.jar"), false, null);
            createJar(artifactRoot.resolve("dependency.jar"), List.of("example.Unrelated"));
            createJar(artifactRoot.resolve("job.jar"), true, "v1");
            Manifest manifest = new Manifest();
            manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
            manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "example.Main");
            manifest.getMainAttributes().putValue("Multi-Release", "true");
            manifest.getMainAttributes().putValue(
                    ExecutableScenarioPlanCompiler.WORKLOAD_PROTOCOL_ATTRIBUTE, "v1");
            createJar(artifactRoot.resolve(artifact), manifest, List.of("example.Main",
                    "META-INF/versions/21/org.apache.flink.connector.kafka.source.KafkaSource",
                    "META-INF/versions/21/org.apache.flink.connector.kafka.sink.KafkaSink"));
            ResolvedScenarioPlan resolved = resolved(document -> {
                useLocalArtifacts(document);
                ((ObjectNode) document.at("/subject/connectors/kafka"))
                        .putArray("runtime_dependencies").add("dependency.jar");
            });
            ExecutableScenarioPlan executable = compiler.compile(resolved);
            try (PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                    resolved, ArtifactResolutionOptions.online(artifactRoot))) {
                SpecificationException exception = assertFailsAt(
                        Stage.RUNNER_CAPABILITY, () -> compiler.bind(prepared, executable));

                assertTrue(exception.diagnostics().stream().anyMatch(issue -> issue.code()
                                .equals("runner.subject.entry-class-versioned-unsupported")),
                        artifact + ": " + exception.diagnostics());
            }
        }
    }

    @Test
    void permitsMultiReleaseDependenciesWithoutVersionedSubjectClasses() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Multi-Release", "true");
        createJar(artifactRoot.resolve("dependency.jar"), manifest,
                List.of("example.Unrelated", "META-INF/versions/21/example.Unrelated"));
        ResolvedScenarioPlan resolved = resolved(document -> {
            useLocalArtifacts(document);
            ((ObjectNode) document.at("/subject/connectors/kafka"))
                    .putArray("runtime_dependencies").add("dependency.jar");
        });
        ExecutableScenarioPlan executable = compiler.compile(resolved);
        try (PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(artifactRoot))) {
            assertEquals(executable, compiler.bind(prepared, executable).executablePlan());
        }
    }

    @Test
    void artifactPreparationRejectsAWorkloadWithoutTheProtocolMarker() throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, null);
        ResolvedScenarioPlan resolved = resolved(this::useLocalArtifacts);

        SpecificationException exception = assertFailsAt(
                Stage.ARTIFACT,
                () -> new ArtifactPlanResolver().resolve(
                        resolved, ArtifactResolutionOptions.online(artifactRoot)));

        assertEquals("artifact.workload.protocol-missing",
                exception.diagnostics().getFirst().code());
        assertEquals("$/workload/jobs/0/jar", exception.diagnostics().getFirst().path());
    }

    @Test
    void revalidatesPreparedWorkloadBytesBeforeTrustingItsProtocolMarker()
            throws IOException {
        createJar(artifactRoot.resolve("connector.jar"), false, null);
        createJar(artifactRoot.resolve("job.jar"), true, "v1");
        ResolvedScenarioPlan resolved = resolved(this::useLocalArtifacts);
        ExecutableScenarioPlan executable = compiler.compile(resolved);

        try (PreparedScenarioPlan prepared = new ArtifactPlanResolver().resolve(
                resolved, ArtifactResolutionOptions.online(artifactRoot))) {
            Path snapshot = prepared.artifact(
                            ScenarioSide.SINGLE, "$/workload/jobs/0/jar")
                    .orElseThrow()
                    .preparedPath();
            createJar(snapshot, true, "v2");

            SpecificationException exception = assertFailsAt(
                    Stage.RUNNER_CAPABILITY,
                    () -> compiler.bind(prepared, executable));
            assertEquals("runner.workload.artifact-changed",
                    exception.diagnostics().getFirst().code());
        }
    }

    @Test
    void rejectsIncompleteOrNonAttemptScopedRuntimeMaterialization() {
        ExecutableScenarioPlan.WorkloadConfiguration configuration = compiler
                .compile(resolved(document -> {})).job().workloadConfiguration();

        assertThrows(IllegalArgumentException.class,
                () -> configuration.materialize(
                        "kafka:9092", Map.of(0, 9L), 1, "1234abcd"));
        assertThrows(IllegalArgumentException.class,
                () -> configuration.materialize(
                        "kafka:9092", Map.of(0, 10L), 0, "1234abcd"));
        assertThrows(IllegalArgumentException.class,
                () -> configuration.materialize(
                        "kafka:9092", Map.of(0, 10L), 1, "too-short"));
    }

    @Test
    void compilesAProxyRoutedSinkAndACountedProtocolFault() {
        ExecutableScenarioPlan plan = compiler.compile(resolved(document -> {
            routeSinkThroughProxy(document);
            ObjectNode fault = replaceSteps(document).addObject().putObject("network_fault");
            fault.put("proxy", "kafka-proxy");
            fault.putObject("target").put("cluster", "main");
            fault.putObject("match")
                    .put("api", "end-txn")
                    .put("result", "commit")
                    .put("transactional_id_prefix", "minimal");
            fault.putObject("fault").put("type", "drop-response");
            fault.put("occurrences", 2);
            fault.put("trigger_deadline", "90s");
            fault.put("heal", "restore-proxy-rule");
        }));

        ExecutableScenarioPlan.KafkaProxy proxy =
                new ExecutableScenarioPlan.KafkaProxy("kafka-proxy", "kafka-proxy", 9092);
        assertEquals(java.util.Optional.of(proxy), plan.kafka().proxy());
        assertEquals(java.util.Optional.of(proxy), plan.job().sink().proxy());
        assertEquals(List.of(new ExecutableScenarioPlan.ProtocolFault(
                        "kafka-proxy",
                        java.util.Optional.of(ExecutableScenarioPlan.TransactionResult.COMMIT),
                        java.util.Optional.of("minimal"),
                        ExecutableScenarioPlan.NetworkFaultAction.DROP_RESPONSE,
                        2,
                        Duration.ofSeconds(90))),
                plan.phases().getFirst().steps());
        Map<String, String> values = plan.job().materializeFlinkConfiguration(
                "kafka-main:19092", Map.of(0, 10L), 1, "1234abcd");
        String prefix = ExecutableScenarioPlan.WorkloadConfiguration.PREFIX;
        assertEquals("kafka-main:19092", values.get(prefix + "source.bootstrap-servers"));
        assertEquals("kafka-proxy:9092", values.get(prefix + "sink.bootstrap-servers"));
    }

    @Test
    void rejectsProxiesAndNetworkFaultsTheRunnerCannotExecute() {
        record Case(String code, Consumer<ObjectNode> mutation) {}
        List<Case> cases = List.of(
                new Case("runner.network-fault.type-unsupported", document -> {
                    ObjectNode fault = dropFault(document, "delay");
                    ((ObjectNode) fault.get("fault")).put("latency", "1s");
                    fault.remove(List.of("occurrences", "trigger_deadline"));
                    fault.put("duration", "5s");
                }),
                new Case("runner.network-fault.loop-unsupported", document -> {
                    ObjectNode fault = dropFault(document, "drop-request");
                    ArrayNode steps = replaceSteps(document);
                    ObjectNode loop = steps.addObject().putObject("loop");
                    loop.put("times", 2);
                    loop.putArray("steps").addObject().set("network_fault", fault);
                }),
                new Case("runner.kafka.proxy-bootstrap-unsupported", document -> {
                    dropFault(document, "drop-request");
                    ((ObjectNode) document.at("/setup/proxies/kafka-proxy")).putObject("bootstrap")
                            .put("address", "kafka-main:19092");
                }),
                new Case("runner.kafka.proxy-listen-unsupported", document -> {
                    dropFault(document, "drop-request");
                    ((ObjectNode) document.at("/setup/flink")).put("taskmanagers", 2);
                    ((ObjectNode) document.at("/setup/proxies/kafka-proxy"))
                            .put("listen", "taskmanager-2:9092");
                }),
                new Case("runner.kafka.proxy-count-unsupported", document -> {
                    dropFault(document, "drop-request");
                    ObjectNode second = ((ObjectNode) document.at("/setup/proxies/kafka-proxy"))
                            .deepCopy().put("listen", "second-proxy:9092");
                    ((ObjectNode) document.at("/setup/proxies")).set("second-proxy", second);
                }),
                new Case("runner.kafka.proxy-route-unsupported", document -> {
                    dropFault(document, "drop-request");
                    ((ObjectNode) document.at("/workload/jobs/0/source"))
                            .put("connect_via_proxy", "kafka-proxy");
                }),
                new Case("runner.kafka.proxy-route-unsupported", document -> {
                    dropFault(document, "drop-request");
                    ((ObjectNode) document.at("/setup/kafka/clusters/main/topics/0/input_source"))
                            .put("connect_via_proxy", "kafka-proxy");
                }));

        for (Case rejected : cases) {
            SpecificationException failure = assertFailsAt(
                    Stage.RUNNER_CAPABILITY,
                    () -> compiler.compile(resolved(rejected.mutation())),
                    rejected.code());
            assertTrue(failure.diagnostics().stream()
                            .anyMatch(diagnostic -> diagnostic.code().equals(rejected.code())),
                    failure.getMessage());
        }
    }

    @Test
    void compilesCountedProtocolFaultsAndRejectsExcessiveDelay() {
        for (String api : java.util.List.of("end-txn", "init-producer-id", "produce", "add-partitions-to-txn", "add-offsets-to-txn", "txn-offset-commit", "find-coordinator")) {
            for (String action : java.util.List.of("drop-request", "drop-response", "delay", "error-response")) {
                var plan = compiler.compile(resolved(document -> {
                    var fault = dropFault(document, action);
                    ((ObjectNode) fault.get("match")).put("api", api).put("transactional_id_prefix", "minimal");
                    if (action.equals("delay")) ((ObjectNode) fault.get("fault")).put("latency", "3s");
                    if (action.equals("error-response")) ((ObjectNode) fault.get("fault")).put("error", api.equals("produce") ? "request-timed-out" : "coordinator-not-available");
                }));
                assertEquals(api, ((ExecutableScenarioPlan.ProtocolFault) plan.phases().getFirst().steps().getFirst()).api());
            }
        }
        assertFailsAt(Stage.RUNNER_CAPABILITY, () -> compiler.compile(resolved(document -> {
            var fault = dropFault(document, "delay");
            ((ObjectNode) fault.get("fault")).put("latency", "6s");
        })));
    }

    @Test
    void compilesOnlyTheSafeCountedErrorAfterAppend() {
        var plan = compiler.compile(resolved(document -> {
            var fault = dropFault(document, "error-after-append");
            ((ObjectNode) fault.get("match")).put("api", "produce").put("transactional_id_prefix", "minimal");
            ((ObjectNode) fault.get("fault")).put("error", "request-timed-out");
        }));
        assertEquals(ExecutableScenarioPlan.NetworkFaultAction.ERROR_AFTER_APPEND,
                ((ExecutableScenarioPlan.ProtocolFault) plan.phases().getFirst().steps().getFirst()).action());
        for (String api : java.util.List.of("produce", "end-txn")) {
            org.junit.jupiter.api.Assertions.assertThrows(SpecificationException.class, () -> compiler.compile(resolved(document -> {
                var fault = dropFault(document, "error-after-append");
                ((ObjectNode) fault.get("match")).put("api", api);
                ((ObjectNode) fault.get("fault")).put("error", api.equals("produce") ? "not-enough-replicas" : "request-timed-out");
            })));
        }
        org.junit.jupiter.api.Assertions.assertThrows(SpecificationException.class, () -> compiler.compile(resolved(document -> {
            var fault = dropFault(document, "error-after-append");
            ((ObjectNode) fault.get("match")).put("api", "produce");
            ((ObjectNode) fault.get("fault")).put("error", "request-timed-out");
            fault.remove("occurrences");
        })));
    }

    @Test void poolingRecoveryCompilesOnlyConcreteSelectorsAndRunningTaskManagers() {
        for (String api : List.of("describe-producers", "list-transactions")) {
            for (String invalid : List.of("none", "prefix", "topic", "unknown-tm", "stopped-tm")) {
                Runnable compile = () -> {
                    var plan = compiler.compile(resolved(document -> {
                        var fault = dropFault(document, "drop-response");
                        var match = (ObjectNode) fault.get("match"); match.put("api", api);
                        if (api.equals("describe-producers")) match.put("topic", "output");
                        fault.putObject("restart").put("component", "taskmanager");
                        if (invalid.equals("prefix")) match.put("transactional_id_prefix", "minimal");
                        if (invalid.equals("topic")) { if (api.equals("describe-producers")) match.remove("topic"); else match.put("topic", "output"); }
                        if (invalid.equals("unknown-tm")) ((ObjectNode) fault.get("restart")).put("name", "taskmanager-9");
                        if (invalid.equals("stopped-tm")) {
                            var steps = (ArrayNode) document.at("/phases/0/steps");
                            var kill = document.objectNode(); kill.putObject("kill").putObject("target").put("kind", "named").put("role", "taskmanager").put("name", "taskmanager-1");
                            steps.insert(0, kill);
                        }
                    }));
                    assertEquals(Optional.of("taskmanager-1"), ((ExecutableScenarioPlan.ProtocolFault) plan.phases().getFirst().steps().getFirst()).restartTaskManager());
                };
                if (invalid.equals("none")) compile.run(); else assertThrows(SpecificationException.class, compile::run, api + invalid);
            }
        }
    }

    @Test void atLeastOnceRollingUsesFixedOrderAndARealPartitionLeader() {
        for (String guarantee : List.of("AT_LEAST_ONCE", "EXACTLY_ONCE")) {
            Runnable compile = () -> compiler.compile(resolved(document -> {
                ((ObjectNode)document.at("/setup/kafka/clusters/main")).put("brokers",3);
                document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode)topic).put("replication_factor",3));
                var sink = (ObjectNode)document.at("/workload/jobs/0/sink");
                sink.put("delivery_guarantee",guarantee);
                if (guarantee.equals("AT_LEAST_ONCE")) sink.remove(List.of("transactional_id_prefix","transaction_id_naming_strategy"));
                var fault = replaceSteps(document).addObject().putObject("broker_fault").put("mode","rolling-restart")
                        .put("order","fixed").put("timeout","2m");
                fault.putObject("target").put("kind","selector").put("role","broker").put("cluster","main")
                        .put("type","partition-leader").put("topic","output").put("partition",0);
            }));
            if (guarantee.equals("AT_LEAST_ONCE")) compile.run(); else assertThrows(SpecificationException.class, compile::run);
        }
    }

    @Test void rollingBrokerRestartRequiresCoordinatorOrderAndRejectsHoldOrCommitOptions() {
        for (String invalid : List.of("none", "duration", "require_commit", "named", "timeout", "order")) {
            Runnable compile = () -> {
                var plan = compiler.compile(resolved(document -> {
                    ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 3);
                    document.at("/setup/kafka/clusters/main/topics").forEach(topic -> ((ObjectNode) topic).put("replication_factor", 3));
                    var fault = replaceSteps(document).addObject().putObject("broker_fault").put("mode", "rolling-restart")
                            .put("timeout", "5m").put("order", "coordinator-last").put("preferred_election", true);
                    var target = fault.putObject("target").put("kind", "selector").put("role", "broker").put("cluster", "main")
                            .put("type", "transaction-coordinator").put("job", "eos-job");
                    if (invalid.equals("duration")) fault.put("duration", "1s");
                    if (invalid.equals("require_commit")) fault.put("require_commit", true);
                    if (invalid.equals("named")) { target.removeAll(); target.put("kind","named").put("role","broker").put("cluster","main").put("name","broker-1"); }
                    if (invalid.equals("timeout")) fault.put("timeout", "6m");
                    if (invalid.equals("order")) fault.remove("order");
                }));
                var request = ((ExecutableScenarioPlan.BrokerFault) plan.phases().getFirst().steps().getFirst()).request();
                assertEquals(org.savonitar.flink.stability.runtime.api.KafkaBrokerControl.RollingOrder.COORDINATOR_LAST, request.order());
                assertTrue(request.preferredElection()); assertEquals(Duration.ZERO, request.duration());
            };
            if (invalid.equals("none")) compile.run(); else assertThrows(SpecificationException.class, compile::run, invalid);
        }
    }

    /** Declares kafka-proxy and routes the sink through it. */
    private static void routeSinkThroughProxy(ObjectNode document) {
        ObjectNode proxy = ((ObjectNode) document.at("/setup")).putObject("proxies")
                .putObject("kafka-proxy");
        proxy.put("type", "kroxylicious");
        proxy.put("cluster", "main");
        proxy.put("listen", "kafka-proxy:9092");
        proxy.putObject("bootstrap").put("cluster", "main");
        ((ObjectNode) document.at("/workload/jobs/0/sink"))
                .put("connect_via_proxy", "kafka-proxy");
    }

    /** Routes the sink through kafka-proxy and makes the only step an EndTxn drop fault. */
    private static ObjectNode dropFault(ObjectNode document, String type) {
        routeSinkThroughProxy(document);
        ObjectNode fault = replaceSteps(document).addObject().putObject("network_fault");
        fault.put("proxy", "kafka-proxy");
        fault.putObject("target").put("cluster", "main");
        fault.putObject("match").put("api", "end-txn");
        fault.putObject("fault").put("type", type);
        fault.put("occurrences", 1);
        fault.put("trigger_deadline", "1m");
        fault.put("heal", "restore-proxy-rule");
        return fault;
    }

    private ResolvedScenarioPlan resolved(Consumer<ObjectNode> mutation) {
        return resolved(mutation, expected(document -> {}));
    }

    private ResolvedScenarioPlan resolved(
            Consumer<ObjectNode> mutation,
            ExpectedResultSpecification expected) {
        ScenarioSpecification base = loader.loadScenario(resource("minimal.yaml"));
        ObjectNode document = base.document();
        document.put("health_retry_limit", 0);
        mutation.accept(document);
        ScenarioSpecification scenario = loader.validateScenarioDocument(base.source(), document);
        return new ScenarioPlanResolver().resolve(
                new ScenarioBundle(scenario, expected), ResolutionRequest.none());
    }

    private ExpectedResultSpecification expected(Consumer<ObjectNode> mutation) {
        ExpectedResultSpecification base = loader.loadExpectedResult(
                resource("minimal.expected.yaml"));
        ObjectNode document = base.document();
        mutation.accept(document);
        return loader.validateExpectedResultDocument(base.source(), document);
    }

    private void useLocalArtifacts(ObjectNode document) {
        ObjectNode connector = (ObjectNode) document.at("/subject/connectors/kafka");
        connector.put("artifact", "connector.jar");
        connector.putArray("runtime_dependencies");
        ((ObjectNode) document.at("/workload/jobs/0")).put("jar", "job.jar");
    }

    private static ArrayNode replaceSteps(ObjectNode document) {
        ArrayNode steps = (ArrayNode) document.at("/phases/0/steps");
        steps.removeAll();
        return steps;
    }

    private static Path createJar(Path path, boolean executable, String protocol)
            throws IOException {
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (executable) {
            attributes.put(Attributes.Name.MAIN_CLASS, "example.Main");
        }
        if (protocol != null) {
            attributes.putValue(
                    ExecutableScenarioPlanCompiler.WORKLOAD_PROTOCOL_ATTRIBUTE, protocol);
        }
        List<String> classes = new ArrayList<>(List.of("example.Main"));
        if (!executable) {
            // A connector fixture supplies what the subject entry-class check requires.
            classes.addAll(ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES);
        }
        return createJar(path, manifest, classes);
    }

    private static Path createJar(Path path, List<String> classes) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        return createJar(path, manifest, classes);
    }

    private static Path createJar(Path path, Manifest manifest, List<String> classes)
            throws IOException {
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(path), manifest)) {
            for (String className : classes) {
                output.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
                output.write(new byte[] {0, 1, 2, 3});
                output.closeEntry();
            }
        }
        return path;
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
