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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    @Test
    void compilesTheMinimalPlainScenarioIntoTypedRuntimeValues() {
        ResolvedScenarioPlan resolved = resolved(document -> {});

        ExecutableScenarioPlan plan = compiler.compile(resolved);

        assertSame(resolved, plan.sourcePlan());
        assertEquals("minimal", plan.scenarioName());
        assertEquals(new ExecutableScenarioPlan.InvocationPolicy(1, 0), plan.invocation());
        assertEquals("main", plan.kafka().alias());
        assertEquals("apache/kafka:4.0.0", plan.kafka().imageReference());
        assertEquals(ExecutableScenarioPlan.KafkaMode.KRAFT, plan.kafka().mode());
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
                        "$/setup/flink/taskmanagers",
                        "$/workload/jobs/0/parallelism",
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
    void compilesAProxyRoutedSinkAndACountedEndTxnFault() {
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
        assertEquals(List.of(new ExecutableScenarioPlan.EndTxnFault(
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
                new Case("runner.network-fault.api-unsupported", document -> {
                    ObjectNode fault = dropFault(document, "drop-request");
                    ((ObjectNode) fault.get("match")).put("api", "produce").put("topic", "output");
                }),
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
                    ((ObjectNode) document.at("/setup/proxies/kafka-proxy"))
                            .put("listen", "taskmanager-1:9092");
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
