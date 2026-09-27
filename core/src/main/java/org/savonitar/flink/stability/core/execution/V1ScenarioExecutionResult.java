package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.execution.kafka.KafkaInputManifest;
import org.savonitar.flink.stability.core.execution.kafka.KafkaTransactionVersion;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.validation.kafka.KafkaIdSetValidationResult;
import org.savonitar.flink.stability.core.validation.kafka.KafkaTransactionListing;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkProcessWriteFenceEvidence;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Structured first-vertical outcome; PASS is possible only after a successful write fence. */
public record V1ScenarioExecutionResult(
        Status status,
        String reason,
        String message,
        Optional<KafkaInputManifest> inputManifest,
        Optional<PhaseExecutionEvidence> phaseEvidence,
        Optional<FlinkTerminalWriteFence.Evidence> writeFenceEvidence,
        Optional<FlinkProcessWriteFenceEvidence> processFenceEvidence,
        Optional<FlinkJobObservation.Attempt> finalJobObservation,
        Optional<KafkaIdSetValidationResult> terminalValidation,
        Optional<KafkaTransactionListing> sinkTransactions,
        Optional<SubjectClassOrigins> subjectClassOrigins,
        List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence,
        FlinkRuntimeIdentity.ExpectedTarget expectedFlinkRuntime,
        Optional<SubjectClassOrigins> runtimeClassOrigins,
        KafkaTransactionVersion.Selection kafkaTransactionVersion,
        FlinkHaEvidence.Expected expectedHa,
        Optional<FlinkHaEvidence.TokenEvidence> tokenEvidence,
        List<String> diagnostics) {

    public V1ScenarioExecutionResult {
        Objects.requireNonNull(status, "status");
        reason = requireNonBlank(reason, "reason");
        message = requireNonBlank(message, "message");
        inputManifest = Objects.requireNonNull(inputManifest, "inputManifest");
        phaseEvidence = Objects.requireNonNull(phaseEvidence, "phaseEvidence");
        writeFenceEvidence = Objects.requireNonNull(
                writeFenceEvidence, "writeFenceEvidence");
        processFenceEvidence = Objects.requireNonNull(
                processFenceEvidence, "processFenceEvidence");
        finalJobObservation = Objects.requireNonNull(
                finalJobObservation, "finalJobObservation");
        terminalValidation = Objects.requireNonNull(
                terminalValidation, "terminalValidation");
        sinkTransactions = Objects.requireNonNull(sinkTransactions, "sinkTransactions");
        subjectClassOrigins = Objects.requireNonNull(
                subjectClassOrigins, "subjectClassOrigins");
        flinkProvisioningEvidence = List.copyOf(Objects.requireNonNull(
                flinkProvisioningEvidence, "flinkProvisioningEvidence"));
        expectedFlinkRuntime = Objects.requireNonNull(expectedFlinkRuntime, "expectedFlinkRuntime");
        runtimeClassOrigins = Objects.requireNonNull(runtimeClassOrigins, "runtimeClassOrigins");
        Objects.requireNonNull(kafkaTransactionVersion, "kafkaTransactionVersion");
        Objects.requireNonNull(expectedHa, "expectedHa");
        Objects.requireNonNull(tokenEvidence, "tokenEvidence");
        if (status == Status.PASS && FlinkHaEvidence.evaluate(expectedHa, phaseEvidence, tokenEvidence, flinkProvisioningEvidence)
                .outcome() != FlinkHaEvidence.Outcome.CONFIRMED) {
            throw new IllegalArgumentException("PASS requires every requested leadership and token fault to be confirmed");
        }
        if (status == Status.PASS && !kafkaTransactionVersion.permitsPass()) {
            throw new IllegalArgumentException("PASS requires confirmed requested Kafka transaction.version");
        }
        diagnostics = List.copyOf(Objects.requireNonNull(diagnostics, "diagnostics"));
        if (status == Status.PASS
                && (writeFenceEvidence.isEmpty()
                        || processFenceEvidence.isEmpty()
                        || terminalValidation.map(KafkaIdSetValidationResult::status)
                                .orElse(KafkaIdSetValidationResult.Status.FAIL)
                                != KafkaIdSetValidationResult.Status.PASS)) {
            throw new IllegalArgumentException(
                    "PASS requires both write-fence and terminal-validation evidence");
        }
        if (status == Status.PASS && TaskManagerKillEffect.evaluate(
                        phaseEvidence.map(PhaseExecutionEvidence::taskManagerKills)
                                .orElse(List.of()),
                        finalJobObservation).stream()
                .anyMatch(effect -> !effect.outcome().confirmed())) {
            throw new IllegalArgumentException(
                    "PASS requires every TaskManager kill to have a confirmed effect");
        }
        if (status == Status.PASS && phaseEvidence
                .map(phases -> phases.networkFaults().stream()
                        .anyMatch(fault -> !fault.triggered()))
                .orElse(false)) {
            throw new IllegalArgumentException(
                    "PASS requires every network fault to have dropped all its occurrences");
        }
        if (status == Status.PASS && subjectClassOrigins
                .map(origins -> origins.outcome(
                        ExecutableScenarioPlan.PROTOCOL_V1_SUBJECT_ENTRY_CLASSES))
                .orElse(SubjectClassOrigins.Outcome.UNCONFIRMED)
                        != SubjectClassOrigins.Outcome.CONFIRMED) {
            throw new IllegalArgumentException(
                    "PASS requires runtime evidence that the subject connector's code ran");
        }
        if (reason.startsWith("verification.") && status != Status.FAIL) {
            throw new IllegalArgumentException(
                    "Every verification.* outcome is an authoritative FAIL");
        }
        if (status == Status.PASS && FlinkRuntimeIdentity.evaluate(expectedFlinkRuntime,
                flinkProvisioningEvidence, processFenceEvidence, phaseEvidence).outcome()
                != FlinkRuntimeIdentity.Outcome.CONFIRMED) {
            throw new IllegalArgumentException("PASS requires confirmed Flink runtime image identities");
        }
        if (status == Status.PASS && FlinkRuntimeIdentity.evaluateRuntimeJar(expectedFlinkRuntime,
                flinkProvisioningEvidence, runtimeClassOrigins)
                .filter(identity -> identity.outcome() != FlinkRuntimeIdentity.Outcome.CONFIRMED)
                .isPresent()) {
            throw new IllegalArgumentException("PASS requires the requested runtime JAR provenance");
        }
        if (writeFenceEvidence.isPresent()
                && !processFenceEvidence.equals(Optional.of(
                        writeFenceEvidence.orElseThrow().processFenceEvidence()))) {
            throw new IllegalArgumentException(
                    "Write-fence and process-fence evidence must identify the same fence");
        }
        if (writeFenceEvidence.isPresent()
                && !finalJobObservation.equals(Optional.of(
                        writeFenceEvidence.orElseThrow().jobBeforeFence()))) {
            throw new IllegalArgumentException(
                    "Write-fence evidence and the result must carry the same job observation");
        }
        if (terminalValidation.isPresent() && processFenceEvidence.isEmpty()) {
            throw new IllegalArgumentException(
                    "Terminal validation requires confirmed process-fence evidence");
        }
    }

    /** Existing non-HA callers still undergo all requested runtime identity checks. */
    public V1ScenarioExecutionResult(
            Status status, String reason, String message,
            Optional<KafkaInputManifest> inputManifest,
            Optional<PhaseExecutionEvidence> phaseEvidence,
            Optional<FlinkTerminalWriteFence.Evidence> writeFenceEvidence,
            Optional<FlinkProcessWriteFenceEvidence> processFenceEvidence,
            Optional<FlinkJobObservation.Attempt> finalJobObservation,
            Optional<KafkaIdSetValidationResult> terminalValidation,
            Optional<KafkaTransactionListing> sinkTransactions,
            Optional<SubjectClassOrigins> subjectClassOrigins,
            List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence,
            FlinkRuntimeIdentity.ExpectedTarget expectedFlinkRuntime,
            Optional<SubjectClassOrigins> runtimeClassOrigins,
            KafkaTransactionVersion.Selection kafkaTransactionVersion,
            List<String> diagnostics) {
        this(status, reason, message, inputManifest, phaseEvidence, writeFenceEvidence,
                processFenceEvidence, finalJobObservation, terminalValidation, sinkTransactions,
                subjectClassOrigins, flinkProvisioningEvidence, expectedFlinkRuntime,
                runtimeClassOrigins, kafkaTransactionVersion,
                new FlinkHaEvidence.Expected(List.of(), false), Optional.empty(), diagnostics);
    }

    /** Existing callers do not supply runtime class evidence; requested checks still fail closed. */
    public V1ScenarioExecutionResult(
            Status status, String reason, String message,
            Optional<KafkaInputManifest> inputManifest,
            Optional<PhaseExecutionEvidence> phaseEvidence,
            Optional<FlinkTerminalWriteFence.Evidence> writeFenceEvidence,
            Optional<FlinkProcessWriteFenceEvidence> processFenceEvidence,
            Optional<FlinkJobObservation.Attempt> finalJobObservation,
            Optional<KafkaIdSetValidationResult> terminalValidation,
            Optional<KafkaTransactionListing> sinkTransactions,
            Optional<SubjectClassOrigins> subjectClassOrigins,
            List<FlinkComponentProvisioningEvidence> flinkProvisioningEvidence,
            FlinkRuntimeIdentity.ExpectedTarget expectedFlinkRuntime,
            List<String> diagnostics) {
        this(status, reason, message, inputManifest, phaseEvidence, writeFenceEvidence,
                processFenceEvidence, finalJobObservation, terminalValidation, sinkTransactions,
                subjectClassOrigins, flinkProvisioningEvidence, expectedFlinkRuntime,
                Optional.empty(), KafkaTransactionVersion.Selection.notRequested(), diagnostics);
    }

    public enum Status {
        PASS,
        FAIL,
        INCONCLUSIVE
    }

    /** Effect evidence for every confirmed TaskManager kill, in execution order. */
    public List<TaskManagerKillEffect> taskManagerKillEffects() {
        return TaskManagerKillEffect.evaluate(
                phaseEvidence.map(PhaseExecutionEvidence::taskManagerKills).orElse(List.of()),
                finalJobObservation);
    }

    public FlinkRuntimeIdentity flinkRuntimeIdentity() {
        return FlinkRuntimeIdentity.evaluate(expectedFlinkRuntime,
                flinkProvisioningEvidence, processFenceEvidence, phaseEvidence);
    }

    public Optional<FlinkRuntimeIdentity> runtimeJarIdentity() {
        return FlinkRuntimeIdentity.evaluateRuntimeJar(
                expectedFlinkRuntime, flinkProvisioningEvidence, runtimeClassOrigins);
    }

    public FlinkHaEvidence haEvidence() {
        return FlinkHaEvidence.evaluate(expectedHa, phaseEvidence, tokenEvidence, flinkProvisioningEvidence);
    }

    public V1ScenarioExecutionResult withCleanupFailure(Throwable failure) {
        return withCleanupFailure(
                failure,
                "infrastructure.attempt-cleanup-failed",
                "The scenario checks passed, but attempt cleanup failed");
    }

    public V1ScenarioExecutionResult withPreparedArtifactCleanupFailure(Throwable failure) {
        return withCleanupFailure(
                failure,
                "infrastructure.prepared-artifact-cleanup-failed",
                "The scenario checks passed, but prepared artifact cleanup failed");
    }

    private V1ScenarioExecutionResult withCleanupFailure(
            Throwable failure,
            String diagnosticCode,
            String passMessage) {
        Objects.requireNonNull(failure, "failure");
        List<String> updated = new java.util.ArrayList<>(diagnostics);
        updated.add(diagnosticCode + ": "
                + failure.getClass().getSimpleName() + ": "
                + String.valueOf(failure.getMessage()));
        for (Throwable suppressed : failure.getSuppressed()) {
            updated.add(diagnosticCode + ": suppressed "
                    + suppressed.getClass().getSimpleName() + ": "
                    + String.valueOf(suppressed.getMessage()));
        }
        if (status == Status.PASS) {
            return new V1ScenarioExecutionResult(
                    Status.INCONCLUSIVE,
                    diagnosticCode,
                    passMessage,
                    inputManifest,
                    phaseEvidence,
                    writeFenceEvidence,
                    processFenceEvidence,
                    finalJobObservation,
                    terminalValidation,
                    sinkTransactions,
                    subjectClassOrigins,
                    flinkProvisioningEvidence,
                    expectedFlinkRuntime,
                    runtimeClassOrigins,
                    kafkaTransactionVersion,
                    expectedHa,
                    tokenEvidence,
                    updated);
        }
        return new V1ScenarioExecutionResult(
                status,
                reason,
                message,
                inputManifest,
                phaseEvidence,
                writeFenceEvidence,
                processFenceEvidence,
                finalJobObservation,
                terminalValidation,
                sinkTransactions,
                subjectClassOrigins,
                flinkProvisioningEvidence,
                expectedFlinkRuntime,
                runtimeClassOrigins,
                kafkaTransactionVersion,
                expectedHa,
                tokenEvidence,
                updated);
    }
}
