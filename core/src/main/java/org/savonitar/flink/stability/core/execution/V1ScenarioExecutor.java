package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.kafka.KafkaInputPreparationException;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.core.execution.kafka.KafkaInputPreparer;
import org.savonitar.flink.stability.core.execution.kafka.PreparedKafkaInput;
import org.savonitar.flink.stability.core.flink.FlinkJobHandle;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobSubmission;
import org.savonitar.flink.stability.core.flink.FlinkRestApiClient;
import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.execution.plan.PreparedExecutableScenarioPlan;
import org.savonitar.flink.stability.core.validation.kafka.KafkaIdSetValidationResult;
import org.savonitar.flink.stability.core.validation.kafka.KafkaAdminTransactionLister;
import org.savonitar.flink.stability.core.validation.kafka.KafkaIdSetValidator;
import org.savonitar.flink.stability.core.validation.kafka.KafkaTransactionListing;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntime;
import org.savonitar.flink.stability.runtime.api.V1AttemptRuntimeFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Executes the first bounded, plain v1 scenario vertical without legacy-schema fallback. */
public final class V1ScenarioExecutor {
    static final Duration DEFAULT_ATTEMPT_CLEANUP_TIMEOUT = Duration.ofMinutes(2);
    static final Duration SINK_TRANSACTION_LISTING_TIMEOUT = Duration.ofSeconds(30);
    /** A Flink process loaded a subject entry class from anywhere but its primary (R5.6d). */
    public static final String SUBJECT_ORIGIN_MISMATCH = "subject.connector.origin-mismatch";
    /** The class-load logs do not show the subject's entry classes running (R5.6d). */
    public static final String SUBJECT_ORIGIN_UNCONFIRMED = "subject.connector.origin-unconfirmed";
    private static final List<String> ENTRY_CLASSES =
            ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES;

    private final V1AttemptRuntimeFactory runtimeFactory;
    private final InputPreparation inputPreparation;
    private final FlinkControlFactory flinkControlFactory;
    private final TerminalValidation terminalValidation;
    private final TransactionListing transactionListing;
    private final AttemptCleanupBoundary cleanupBoundary;

    public V1ScenarioExecutor(V1AttemptRuntimeFactory runtimeFactory) {
        this(
                runtimeFactory,
                new KafkaInputPreparer()::prepare,
                FlinkRestApiClient::new,
                new KafkaIdSetValidator()::validate,
                new KafkaAdminTransactionLister()::list,
                DEFAULT_ATTEMPT_CLEANUP_TIMEOUT);
    }

    V1ScenarioExecutor(
            V1AttemptRuntimeFactory runtimeFactory,
            InputPreparation inputPreparation,
            FlinkControlFactory flinkControlFactory,
            TerminalValidation terminalValidation) {
        this(
                runtimeFactory,
                inputPreparation,
                flinkControlFactory,
                terminalValidation,
                (bootstrapServers, prefix, timeout) -> new KafkaTransactionListing(
                        prefix, List.of()),
                DEFAULT_ATTEMPT_CLEANUP_TIMEOUT);
    }

    V1ScenarioExecutor(
            V1AttemptRuntimeFactory runtimeFactory,
            InputPreparation inputPreparation,
            FlinkControlFactory flinkControlFactory,
            TerminalValidation terminalValidation,
            TransactionListing transactionListing,
            Duration cleanupTimeout) {
        this.runtimeFactory = Objects.requireNonNull(runtimeFactory, "runtimeFactory");
        this.inputPreparation = Objects.requireNonNull(
                inputPreparation, "inputPreparation");
        this.flinkControlFactory = Objects.requireNonNull(
                flinkControlFactory, "flinkControlFactory");
        this.terminalValidation = Objects.requireNonNull(
                terminalValidation, "terminalValidation");
        this.transactionListing = Objects.requireNonNull(
                transactionListing, "transactionListing");
        this.cleanupBoundary = new AttemptCleanupBoundary(cleanupTimeout);
    }

    public V1ScenarioExecutionResult execute(
            PreparedExecutableScenarioPlan prepared,
            V1AttemptContext context) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(context, "context");
        AttemptResources resources = new AttemptResources();
        final V1ScenarioExecutionResult result;
        try {
            result = executeAttempt(prepared, context, resources);
        } catch (RuntimeException | Error fatal) {
            RuntimeException cleanupFailure = resources.cleanup(cleanupBoundary);
            if (cleanupFailure != null) {
                fatal.addSuppressed(cleanupFailure);
            }
            throw fatal;
        }
        RuntimeException cleanupFailure = resources.cleanup(cleanupBoundary);
        return cleanupFailure == null ? result : result.withCleanupFailure(cleanupFailure);
    }

    private V1ScenarioExecutionResult executeAttempt(
            PreparedExecutableScenarioPlan prepared,
            V1AttemptContext context,
            AttemptResources resources) {
        ExecutableScenarioPlan plan = prepared.executablePlan();

        V1AttemptRuntime runtime = null;
        FlinkScenarioControl flink = null;
        FlinkJobHandle job = null;
        PreparedKafkaInput input = null;
        KafkaInputManifest inputEvidence = null;
        PhaseExecutionEvidence phases = null;
        FlinkTerminalWriteFence.Evidence fence = null;
        FlinkProcessWriteFenceEvidence processFence = null;
        FlinkJobObservation.Attempt finalJob = null;
        KafkaIdSetValidationResult validation = null;
        KafkaTransactionListing sinkTransactions = null;
        SubjectClassOrigins subjectOrigins = null;
        List<String> evidenceDiagnostics = new ArrayList<>();
        V1ScenarioExecutionResult result;
        Stage stage = Stage.RUNTIME_CREATION;
        try {
            runtime = runtimeFactory.create(context.checkpointStorageRoot());
            resources.runtime = runtime;
            stage = Stage.KAFKA_START;
            KafkaRuntimeEndpoints endpoints = runtime.startKafka(plan.kafka().runtimeTarget());
            stage = Stage.INPUT_PREPARATION;
            input = inputPreparation.prepare(plan, endpoints);
            inputEvidence = input.inputManifest();

            stage = Stage.FLINK_START;
            String jobManagerRestUrl = runtime.startFlink(prepared.flinkRuntimeTarget());
            flink = flinkControlFactory.open(jobManagerRestUrl);
            resources.flink = flink;

            stage = Stage.JOB_SUBMISSION;
            String uploadedJarId = flink.uploadJar(
                    prepared.workloadArtifact().preparedPath(),
                    prepared.workloadArtifact().sha256());
            var configuration = plan.job().materializeFlinkConfiguration(
                    endpoints.internalBootstrapServers(),
                    input.stoppingOffsets(),
                    context.attemptOrdinal(),
                    context.attemptNonce8());
            job = flink.submit(new FlinkJobSubmission(
                    uploadedJarId,
                    plan.job().parallelism(),
                    configuration,
                    plan.job().programArguments().values()));
            stage = Stage.PHASES;
            phases = new ExecutablePhaseExecutor(flink, runtime).execute(plan, job);

            stage = Stage.WRITE_FENCE;
            fence = new FlinkTerminalWriteFence(
                    flink, runtime::stopAllFlinkProcesses)
                    .awaitBoundedCompletion(job, plan.jobCompletionTimeout());
            processFence = fence.processFenceEvidence();
            finalJob = fence.jobBeforeFence();
            // Every Flink JVM is dead now, so each class-load log is complete.
            subjectOrigins = subjectOrigins(runtime, prepared);

            stage = Stage.TERMINAL_VALIDATION;
            validation = terminalValidation.validate(
                    endpoints.hostBootstrapServers(),
                    plan.terminalValidation().output().topic(),
                    input.inputManifest().presentIds(),
                    plan.terminalValidation().timeout());
            // Only a transactional (exactly-once) sink has a prefix to list.
            String bootstrapServers = endpoints.hostBootstrapServers();
            sinkTransactions = plan.job().sink().transactionalIdPrefix()
                    .map(prefix -> listSinkTransactions(
                            bootstrapServers, prefix, evidenceDiagnostics))
                    .orElse(null);
            // A failed oracle stays FAIL whatever the faults did: the output is wrong either
            // way. A passing oracle shows recovery only if every kill disrupted the job.
            Optional<TaskManagerKillEffect> unconfirmedKill = TaskManagerKillEffect.evaluate(
                            phases.taskManagerKills(), Optional.of(finalJob))
                    .stream()
                    .filter(effect -> !effect.outcome().confirmed())
                    .findFirst();
            SubjectClassOrigins.Outcome subjectUse = subjectOrigins.outcome(ENTRY_CLASSES);
            V1ScenarioExecutionResult.Status status;
            String reason;
            String message;
            if (validation.status() != KafkaIdSetValidationResult.Status.PASS) {
                status = V1ScenarioExecutionResult.Status.FAIL;
                reason = validation.reason();
                message = validation.message();
            } else if (subjectUse != SubjectClassOrigins.Outcome.CONFIRMED) {
                status = V1ScenarioExecutionResult.Status.INCONCLUSIVE;
                reason = subjectUse == SubjectClassOrigins.Outcome.MISMATCH
                        ? SUBJECT_ORIGIN_MISMATCH
                        : SUBJECT_ORIGIN_UNCONFIRMED;
                message = "The terminal oracle passed, but the run does not show that the"
                        + " subject connector's code ran: "
                        + subjectOrigins.detail(ENTRY_CLASSES);
            } else if (unconfirmedKill.isPresent()) {
                TaskManagerKillEffect effect = unconfirmedKill.orElseThrow();
                status = V1ScenarioExecutionResult.Status.INCONCLUSIVE;
                reason = ExecutablePhaseExecutor.TASKMANAGER_KILL_EFFECT_UNCONFIRMED;
                message = "The terminal oracle passed, but the TaskManager kill at "
                        + effect.kill().path() + " did not observably affect the job ("
                        + effect.outcome() + "): " + effect.detail();
            } else {
                status = V1ScenarioExecutionResult.Status.PASS;
                reason = validation.reason();
                message = phases.taskManagerKills().isEmpty()
                        ? "The terminal oracle passed"
                        : "The terminal oracle passed and every TaskManager kill had a"
                                + " confirmed effect";
            }
            result = result(
                    status,
                    reason,
                    message,
                    inputEvidence,
                    phases,
                    fence,
                    processFence,
                    finalJob,
                    validation,
                    sinkTransactions,
                    subjectOrigins,
                    runtime,
                    evidenceDiagnostics);
        } catch (KafkaInputPreparationException failure) {
            inputEvidence = failure.evidence().orElse(inputEvidence);
            result = result(
                    V1ScenarioExecutionResult.Status.INCONCLUSIVE,
                    failure.reasonCode(),
                    failure.getMessage(),
                    inputEvidence,
                    phases,
                    fence,
                    processFence,
                    finalJob,
                    validation,
                    sinkTransactions,
                    subjectOrigins,
                    runtime,
                    diagnostics(failure));
        } catch (PhaseExecutionException failure) {
            phases = failure.evidence();
            // Read-only: explains a failed await (e.g. a restart loop) and judges earlier kills.
            finalJob = FlinkJobObservation.Attempt.of(flink, job);
            result = result(
                    failure.outcome() == PhaseExecutionException.Outcome.FAIL
                            ? V1ScenarioExecutionResult.Status.FAIL
                            : V1ScenarioExecutionResult.Status.INCONCLUSIVE,
                    failure.reason(),
                    failure.getMessage(),
                    inputEvidence,
                    phases,
                    fence,
                    processFence,
                    finalJob,
                    validation,
                    sinkTransactions,
                    subjectOrigins,
                    runtime,
                    diagnostics(failure));
        } catch (TerminalWriteFenceException failure) {
            processFence = failure.processFenceEvidence().orElse(null);
            finalJob = failure.jobBeforeFence();
            result = result(
                    V1ScenarioExecutionResult.Status.FAIL,
                    failure.reason(),
                    failure.getMessage(),
                    inputEvidence,
                    phases,
                    fence,
                    processFence,
                    finalJob,
                    validation,
                    sinkTransactions,
                    subjectOrigins,
                    runtime,
                    diagnostics(failure));
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            result = result(
                    stage.failureStatus(),
                    stage.reason(),
                    "Attempt infrastructure failed during " + stage.displayName(),
                    inputEvidence,
                    phases,
                    fence,
                    processFence,
                    finalJob,
                    validation,
                    sinkTransactions,
                    subjectOrigins,
                    runtime,
                    diagnostics(failure));
        } catch (Exception failure) {
            result = result(
                    stage.failureStatus(),
                    stage.reason(),
                    "Attempt infrastructure failed during " + stage.displayName(),
                    inputEvidence,
                    phases,
                    fence,
                    processFence,
                    finalJob,
                    validation,
                    sinkTransactions,
                    subjectOrigins,
                    runtime,
                    diagnostics(failure));
        }

        return result;
    }

    private static SubjectClassOrigins subjectOrigins(
            V1AttemptRuntime runtime,
            PreparedExecutableScenarioPlan prepared) {
        String expectedSource = prepared.connectorBundle()
                .primaryEntry(prepared.executablePlan().job().connectorAlias())
                .map(entry -> entry.containerPath())
                .orElseThrow(() -> new IllegalStateException(
                        "The bound connector bundle has no subject primary entry"));
        try {
            return SubjectClassOrigins.read(
                    runtime.flinkClassLoadLogs(), ENTRY_CLASSES, expectedSource);
        } catch (RuntimeException unavailable) {
            return new SubjectClassOrigins(expectedSource, List.of(), Optional.of(
                    "Cannot list Flink class-load logs: " + unavailable.getMessage()));
        }
    }

    /**
     * Evidence only: after the fence nothing but a broker timeout resolves an open transaction,
     * so the listing names what may pin a last stable offset. A failure becomes a diagnostic.
     */
    private KafkaTransactionListing listSinkTransactions(
            String bootstrapServers,
            String transactionalIdPrefix,
            List<String> diagnostics) {
        try {
            return transactionListing.list(
                    bootstrapServers, transactionalIdPrefix, SINK_TRANSACTION_LISTING_TIMEOUT);
        } catch (Exception unavailable) {
            if (unavailable instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            diagnostics.add("kafka.sink-transactions-unavailable: "
                    + unavailable.getClass().getSimpleName() + ": " + unavailable.getMessage());
            return null;
        }
    }

    private static V1ScenarioExecutionResult result(
            V1ScenarioExecutionResult.Status status,
            String reason,
            String message,
            KafkaInputManifest inputEvidence,
            PhaseExecutionEvidence phases,
            FlinkTerminalWriteFence.Evidence fence,
            FlinkProcessWriteFenceEvidence processFence,
            FlinkJobObservation.Attempt finalJob,
            KafkaIdSetValidationResult validation,
            KafkaTransactionListing sinkTransactions,
            SubjectClassOrigins subjectOrigins,
            V1AttemptRuntime runtime,
            List<String> diagnostics) {
        List<FlinkComponentProvisioningEvidence> provisioning = runtime == null
                ? List.of()
                : runtime.flinkProvisioningEvidence();
        return new V1ScenarioExecutionResult(
                status,
                reason,
                message,
                Optional.ofNullable(inputEvidence),
                Optional.ofNullable(phases),
                Optional.ofNullable(fence),
                Optional.ofNullable(processFence),
                Optional.ofNullable(finalJob),
                Optional.ofNullable(validation),
                Optional.ofNullable(sinkTransactions),
                Optional.ofNullable(subjectOrigins),
                provisioning,
                diagnostics);
    }

    private static List<String> diagnostics(Throwable failure) {
        List<String> diagnostics = new ArrayList<>();
        Throwable current = failure;
        while (current != null) {
            diagnostics.add(current.getClass().getSimpleName() + ": "
                    + String.valueOf(current.getMessage()));
            for (Throwable suppressed : current.getSuppressed()) {
                diagnostics.add("suppressed " + suppressed.getClass().getSimpleName() + ": "
                        + String.valueOf(suppressed.getMessage()));
            }
            current = current.getCause();
        }
        return List.copyOf(diagnostics);
    }

    private static RuntimeException append(RuntimeException aggregate, RuntimeException next) {
        if (aggregate == null) {
            return next;
        }
        if (aggregate != next) {
            aggregate.addSuppressed(next);
        }
        return aggregate;
    }

    private static final class AttemptResources {
        private FlinkScenarioControl flink;
        private V1AttemptRuntime runtime;
        private boolean cleanupStarted;

        private RuntimeException cleanup(AttemptCleanupBoundary boundary) {
            if (cleanupStarted) {
                return null;
            }
            cleanupStarted = true;
            return boundary.await(this::cleanupSynchronously);
        }

        private RuntimeException cleanupSynchronously() {
            RuntimeException failure = null;
            if (flink != null) {
                try {
                    flink.close();
                } catch (RuntimeException closeFailure) {
                    failure = append(failure, closeFailure);
                }
            }
            if (runtime != null) {
                try {
                    runtime.close();
                } catch (RuntimeException closeFailure) {
                    failure = append(failure, closeFailure);
                }
            }
            return failure;
        }
    }

    @FunctionalInterface
    interface InputPreparation {
        PreparedKafkaInput prepare(
                ExecutableScenarioPlan plan, KafkaRuntimeEndpoints endpoints);
    }

    @FunctionalInterface
    interface FlinkControlFactory {
        FlinkScenarioControl open(String jobManagerRestUrl);
    }

    @FunctionalInterface
    interface TransactionListing {
        KafkaTransactionListing list(
                String bootstrapServers,
                String transactionalIdPrefix,
                Duration timeout) throws Exception;
    }

    @FunctionalInterface
    interface TerminalValidation {
        KafkaIdSetValidationResult validate(
                String bootstrapServers,
                String topic,
                long[] expectedIds,
                java.time.Duration timeout);
    }

    private enum Stage {
        RUNTIME_CREATION("infrastructure.attempt-runtime-creation-failed", "runtime creation"),
        KAFKA_START("infrastructure.kafka-start-failed", "Kafka startup"),
        INPUT_PREPARATION("infrastructure.kafka-input-setup-failed", "input preparation"),
        FLINK_START("infrastructure.flink-start-failed", "Flink startup"),
        JOB_SUBMISSION("infrastructure.flink-submission-failed", "job submission"),
        PHASES("infrastructure.phase-execution-failed", "phase execution"),
        WRITE_FENCE(
                "verification.flink.job-terminalization-failed",
                "write fencing",
                V1ScenarioExecutionResult.Status.FAIL),
        TERMINAL_VALIDATION(
                "verification.kafka.snapshot-unavailable",
                "terminal validation",
                V1ScenarioExecutionResult.Status.FAIL);

        private final String reason;
        private final String displayName;

        Stage(String reason, String displayName) {
            this(reason, displayName, V1ScenarioExecutionResult.Status.INCONCLUSIVE);
        }

        Stage(
                String reason,
                String displayName,
                V1ScenarioExecutionResult.Status failureStatus) {
            this.reason = reason;
            this.displayName = displayName;
            this.failureStatus = failureStatus;
        }

        private final V1ScenarioExecutionResult.Status failureStatus;

        String reason() {
            return reason;
        }

        String displayName() {
            return displayName;
        }

        V1ScenarioExecutionResult.Status failureStatus() {
            return failureStatus;
        }
    }
}
