package org.savonitar.flink.stability.core.execution.kafka;

import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/** Creates an exact bounded Kafka input and fails closed unless all bounds are evidenced. */
public final class KafkaInputPreparer {

    public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofMinutes(2);
    /** Keeps generated input/output evidence available beyond any single v1 attempt. */
    public static final Duration HARNESS_TOPIC_RETENTION = Duration.ofDays(7);

    private final KafkaInputOperations.Factory operationsFactory;
    private final Duration preparationTimeout;
    private final LongSupplier nanoTime;

    public KafkaInputPreparer() {
        this(
                DEFAULT_OPERATION_TIMEOUT,
                System::nanoTime,
                (host, timeout) -> new KafkaClientInputOperations(host, timeout));
    }

    KafkaInputPreparer(KafkaInputOperations.Factory operationsFactory) {
        this(DEFAULT_OPERATION_TIMEOUT, System::nanoTime, operationsFactory);
    }

    KafkaInputPreparer(
            Duration preparationTimeout,
            LongSupplier nanoTime,
            KafkaInputOperations.Factory operationsFactory) {
        this.preparationTimeout = Objects.requireNonNull(preparationTimeout, "preparationTimeout");
        if (preparationTimeout.isZero() || preparationTimeout.isNegative()) {
            throw new IllegalArgumentException("preparationTimeout must be positive");
        }
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.operationsFactory = Objects.requireNonNull(operationsFactory, "operationsFactory");
    }

    public PreparedKafkaInput prepare(
            ExecutableScenarioPlan plan, KafkaRuntimeEndpoints endpoints) {
        Objects.requireNonNull(plan, "plan");
        return prepare(plan.kafka(), plan.input(), endpoints);
    }

    public PreparedKafkaInput prepare(
            ExecutableScenarioPlan.KafkaCluster cluster,
            ExecutableScenarioPlan.GeneratedIntegerSequenceInput input,
            KafkaRuntimeEndpoints endpoints) {
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(endpoints, "endpoints");
        validateRuntimeIdentity(cluster, input, endpoints);
        if (input.totalRecords()
                > ExecutableScenarioPlan.FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    "The v1 in-memory evidence manifest exceeds the first-runner limit");
        }

        List<KafkaInputOperations.TopicDefinition> topics = cluster.topics().stream()
                .map(topic -> new KafkaInputOperations.TopicDefinition(
                        topic.name(),
                        topic.partitions(),
                        (short) topic.replicationFactor(),
                        KafkaInputOperations.TopicCleanupPolicy.DELETE,
                        HARNESS_TOPIC_RETENTION))
                .toList();

        KafkaInputPreparationDeadline deadline = new KafkaInputPreparationDeadline(
                preparationTimeout, nanoTime);
        KafkaInputOperations operations = null;
        KafkaInputManifest inputEvidence = null;
        PreparedKafkaInput prepared = null;
        KafkaInputPreparationException primaryFailure = null;
        try {
            operations = operationsFactory.open(
                    endpoints.hostBootstrapServers(),
                    deadline.requireRemaining("opening the Kafka client boundary"));
            deadline.requireRemaining("creating declared Kafka topics");
            operations.createTopics(topics, deadline);
            deadline.requireRemaining("producing generated Kafka input");
            List<KafkaInputOperations.ProducedRecord> produced = operations.produceAndAwait(
                    input.topic(), input.partitions(), input.totalRecords(), deadline);
            List<KafkaInputManifest.RecordAcknowledgement> acknowledgedRecords =
                    validateAcknowledgements(input, produced);
            deadline.requireRemaining("capturing Kafka input offsets");
            KafkaInputOperations.OffsetSnapshot offsets = operations.readOffsets(
                    input.topic(), input.partitions(), deadline);
            ValidatedOffsets validatedOffsets = validateOffsets(input, offsets);
            KafkaInputOperations.ReconciliationSnapshot reconciliation;
            try {
                deadline.requireRemaining("reconciling generated Kafka input");
                reconciliation = operations.reconcileFromZeroThrough(
                        input.topic(), validatedOffsets.exclusiveEnds(), deadline);
            } catch (KafkaInputOperations.ReconciliationCloseException closeFailure) {
                try {
                    inputEvidence = validateAndCreateManifest(
                            input,
                            acknowledgedRecords,
                            validatedOffsets,
                            closeFailure.snapshot());
                } catch (KafkaInputPreparationException substantiveFailure) {
                    substantiveFailure.addSuppressed(closeFailure);
                    throw substantiveFailure;
                }
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                        "Kafka input reconciliation consumer failed to close after its "
                                + "evidence was captured",
                        closeFailure,
                        inputEvidence);
            }
            inputEvidence = validateAndCreateManifest(
                    input, acknowledgedRecords, validatedOffsets, reconciliation);
            prepared = new PreparedKafkaInput(endpoints, inputEvidence);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            if (inputEvidence == null) {
                primaryFailure = new KafkaInputPreparationException(
                        KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                        "Kafka input preparation was interrupted", failure);
            } else {
                primaryFailure = new KafkaInputPreparationException(
                        KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                        "Kafka input preparation was interrupted after its bounds were evidenced",
                        failure,
                        inputEvidence);
            }
        } catch (KafkaInputPreparationException failure) {
            primaryFailure = failure;
        } catch (Exception failure) {
            if (inputEvidence != null) {
                primaryFailure = new KafkaInputPreparationException(
                        KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                        "Kafka input preparation failed after its bounds were evidenced",
                        failure,
                        inputEvidence);
            } else {
                primaryFailure = new KafkaInputPreparationException(
                        KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                        "Kafka input preparation failed before its bounds could be evidenced",
                        failure);
            }
        } finally {
            Exception closeFailure = close(operations, deadline);
            if (primaryFailure == null) {
                try {
                    deadline.requireRemaining("completing Kafka input preparation");
                } catch (KafkaInputPreparationDeadline
                        .KafkaInputPreparationDeadlineExceededException timeoutFailure) {
                    primaryFailure = preparationFailure(
                            "Kafka input preparation timed out after its bounds were evidenced",
                            timeoutFailure,
                            inputEvidence);
                }
            }
            if (closeFailure != null) {
                if (primaryFailure != null) {
                    primaryFailure.addSuppressed(closeFailure);
                } else {
                    primaryFailure = preparationFailure(
                            "Kafka input preparation cleanup failed",
                            closeFailure,
                            inputEvidence);
                }
            }
        }
        if (primaryFailure != null) {
            throw primaryFailure;
        }
        return Objects.requireNonNull(prepared, "prepared");
    }

    private static Exception close(
            KafkaInputOperations operations, KafkaInputPreparationDeadline deadline) {
        if (operations == null) {
            return null;
        }
        try {
            operations.close(deadline.remaining());
            return null;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            return failure;
        } catch (Exception failure) {
            return failure;
        }
    }

    private static KafkaInputPreparationException preparationFailure(
            String message, Exception failure, KafkaInputManifest inputEvidence) {
        if (inputEvidence == null) {
            return new KafkaInputPreparationException(
                    KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                    message,
                    failure);
        }
        return new KafkaInputPreparationException(
                KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                message,
                failure,
                inputEvidence);
    }

    private static ValidatedOffsets validateOffsets(
            ExecutableScenarioPlan.GeneratedIntegerSequenceInput input,
            KafkaInputOperations.OffsetSnapshot offsets) {
        Objects.requireNonNull(offsets, "offsets");
        Map<Integer, Long> beginnings = exactOffsetMap(
                offsets.beginningOffsets(), input.partitions(), "beginning offsets");
        Map<Integer, Long> ends = exactOffsetMap(
                offsets.exclusiveEndOffsets(), input.partitions(), "exclusive end offsets");

        long endSum = 0;
        for (int partition = 0; partition < input.partitions(); partition++) {
            if (beginnings.get(partition) != 0) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        "Source partition " + partition + " did not begin at offset zero");
            }
            long expectedEnd = recordsForPartition(
                    input.totalRecords(), input.partitions(), partition);
            long actualEnd = ends.get(partition);
            if (actualEnd != expectedEnd) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        "Source partition " + partition + " ended at " + actualEnd
                                + " instead of " + expectedEnd);
            }
            try {
                endSum = Math.addExact(endSum, actualEnd);
            } catch (ArithmeticException overflow) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        "Source partition end-offset sum overflowed", overflow);
            }
        }
        if (endSum != input.totalRecords()) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    "Source partition end offsets do not sum to totalRecords");
        }
        return new ValidatedOffsets(beginnings, ends);
    }

    private static KafkaInputManifest validateAndCreateManifest(
            ExecutableScenarioPlan.GeneratedIntegerSequenceInput input,
            List<KafkaInputManifest.RecordAcknowledgement> acknowledgedRecords,
            ValidatedOffsets offsets,
            KafkaInputOperations.ReconciliationSnapshot reconciliation) {
        Objects.requireNonNull(acknowledgedRecords, "acknowledgedRecords");
        Objects.requireNonNull(reconciliation, "reconciliation");
        ReconciliationAnalysis analysis = reconcile(input, reconciliation.records());
        KafkaInputManifest.TerminalDisposition absentDisposition =
                reconciliation.reachedEveryExclusiveEnd()
                        ? KafkaInputManifest.TerminalDisposition.ACKNOWLEDGED_MISSING
                        : KafkaInputManifest.TerminalDisposition.INDETERMINATE;
        List<KafkaInputManifest.RecordAcknowledgement> terminalRecords =
                terminalRecords(acknowledgedRecords, analysis.present(), absentDisposition);
        KafkaInputManifest.ReconciliationEvidence reconciliationEvidence =
                new KafkaInputManifest.ReconciliationEvidence(
                        reconciliation.consumerConfiguration(),
                        analysis.observedIds(),
                        reconciliation.reachedEveryExclusiveEnd());

        if (!reconciliation.reachedEveryExclusiveEnd()) {
            KafkaInputManifest partial = manifest(
                    input,
                    terminalRecords,
                    offsets,
                    KafkaInputManifest.EvidenceStatus.PARTIAL,
                    reconciliationEvidence);
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    "Kafka input reconciliation did not reach every captured source bound",
                    partial);
        }
        if (analysis.failureMessage() != null) {
            KafkaInputManifest partial = manifest(
                    input,
                    terminalRecords,
                    offsets,
                    KafkaInputManifest.EvidenceStatus.PARTIAL,
                    reconciliationEvidence);
            if (analysis.failureCause() == null) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.ACKNOWLEDGED_MISSING,
                        analysis.failureMessage(),
                        partial);
            }
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.ACKNOWLEDGED_MISSING,
                    analysis.failureMessage(),
                    analysis.failureCause(),
                    partial);
        }
        if (analysis.observedIds().size() != input.totalRecords()) {
            KafkaInputManifest partial = manifest(
                    input,
                    terminalRecords,
                    offsets,
                    KafkaInputManifest.EvidenceStatus.PARTIAL,
                    reconciliationEvidence);
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.ACKNOWLEDGED_MISSING,
                    "Kafka reconciliation did not return every acknowledged input record",
                    partial);
        }

        return manifest(
                input,
                terminalRecords,
                offsets,
                KafkaInputManifest.EvidenceStatus.COMPLETE,
                reconciliationEvidence);
    }

    private static List<KafkaInputManifest.RecordAcknowledgement> validateAcknowledgements(
            ExecutableScenarioPlan.GeneratedIntegerSequenceInput input,
            List<KafkaInputOperations.ProducedRecord> produced) {
        Objects.requireNonNull(produced, "produced");
        if (produced.size() != input.totalRecords()) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    "Kafka did not acknowledge every generated input record");
        }

        List<KafkaInputManifest.RecordAcknowledgement> acknowledgements =
                new ArrayList<>(produced.size());
        for (int index = 0; index < produced.size(); index++) {
            long expectedId = index;
            int expectedPartition = (int) (expectedId % input.partitions());
            long expectedOffset = expectedId / input.partitions();
            KafkaInputOperations.ProducedRecord actual = produced.get(index);
            if (actual == null
                    || actual.id() != expectedId
                    || actual.partition() != expectedPartition
                    || actual.offset() != expectedOffset) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        "Kafka acknowledgement does not match generated record " + expectedId);
            }
            List<KafkaInputOperations.HarnessProducerAttempt> actualAttempts =
                    actual.producerAttempts();
            if (actualAttempts == null || actualAttempts.isEmpty()) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        "Kafka acknowledgement omitted producer attempts for record "
                                + expectedId);
            }
            List<KafkaInputManifest.ProducerAttempt> attempts =
                    new ArrayList<>(actualAttempts.size());
            for (int attemptIndex = 0; attemptIndex < actualAttempts.size(); attemptIndex++) {
                KafkaInputOperations.HarnessProducerAttempt attempt =
                        actualAttempts.get(attemptIndex);
                if (attempt == null
                        || attempt.ordinal() != attemptIndex + 1
                        || attempt.targetPartition() != expectedPartition
                        || attempt.acknowledgedPartition() != expectedPartition
                        || attempt.acknowledgedOffset() < 0) {
                    throw new KafkaInputPreparationException(
                            KafkaInputPreparationException.INDETERMINATE,
                            "Kafka producer attempt does not match generated record "
                                    + expectedId);
                }
                attempts.add(new KafkaInputManifest.ProducerAttempt(
                        attempt.ordinal(),
                        attempt.targetPartition(),
                        attempt.acknowledgedPartition(),
                        attempt.acknowledgedOffset(),
                        KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED));
            }
            KafkaInputManifest.ProducerAttempt terminalAttempt = attempts.getLast();
            if (terminalAttempt.acknowledgedOffset() != expectedOffset) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        "Terminal Kafka producer attempt does not match generated record "
                                + expectedId);
            }
            acknowledgements.add(new KafkaInputManifest.RecordAcknowledgement(
                    expectedId,
                    sha256(Long.toString(expectedId).getBytes(StandardCharsets.UTF_8)),
                    attempts,
                    expectedPartition,
                    expectedOffset,
                    KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED,
                    KafkaInputManifest.TerminalDisposition.INDETERMINATE));
        }
        return List.copyOf(acknowledgements);
    }

    private static KafkaInputManifest manifest(
            ExecutableScenarioPlan.GeneratedIntegerSequenceInput input,
            List<KafkaInputManifest.RecordAcknowledgement> records,
            ValidatedOffsets offsets,
            KafkaInputManifest.EvidenceStatus status,
            KafkaInputManifest.ReconciliationEvidence reconciliation) {
        return new KafkaInputManifest(
                input.cluster(),
                input.topic(),
                input.totalRecords(),
                records,
                offsets.beginnings(),
                offsets.exclusiveEnds(),
                status,
                reconciliation);
    }

    private static List<KafkaInputManifest.RecordAcknowledgement> terminalRecords(
            List<KafkaInputManifest.RecordAcknowledgement> acknowledgedRecords,
            boolean[] present,
            KafkaInputManifest.TerminalDisposition absentDisposition) {
        List<KafkaInputManifest.RecordAcknowledgement> terminal =
                new ArrayList<>(acknowledgedRecords.size());
        for (int index = 0; index < acknowledgedRecords.size(); index++) {
            KafkaInputManifest.RecordAcknowledgement record = acknowledgedRecords.get(index);
            terminal.add(new KafkaInputManifest.RecordAcknowledgement(
                    record.id(),
                    record.payloadSha256(),
                    record.producerAttempts(),
                    record.partition(),
                    record.offset(),
                    record.acknowledgementOutcome(),
                    present[index]
                            ? KafkaInputManifest.TerminalDisposition.PRESENT
                            : absentDisposition));
        }
        return List.copyOf(terminal);
    }

    private static ReconciliationAnalysis reconcile(
            ExecutableScenarioPlan.GeneratedIntegerSequenceInput input,
            List<KafkaInputOperations.ObservedRecord> observed) {
        Objects.requireNonNull(observed, "observed");
        boolean[] present = new boolean[Math.toIntExact(input.totalRecords())];
        String failureMessage = null;
        Throwable failureCause = null;
        for (KafkaInputOperations.ObservedRecord record : observed) {
            if (record == null
                    || record.partition() < 0
                    || record.partition() >= input.partitions()
                    || record.offset() < 0) {
                if (failureMessage == null) {
                    failureMessage =
                            "Kafka reconciliation returned an invalid topic position";
                }
                continue;
            }
            long id;
            try {
                id = Math.addExact(
                        Math.multiplyExact(record.offset(), input.partitions()),
                        record.partition());
            } catch (ArithmeticException overflow) {
                if (failureMessage == null) {
                    failureMessage =
                            "Kafka reconciliation returned an overflowing topic position";
                    failureCause = overflow;
                }
                continue;
            }
            if (id < 0 || id >= input.totalRecords()) {
                if (failureMessage == null) {
                    failureMessage =
                            "Kafka reconciliation returned a record outside the captured bounds";
                }
                continue;
            }
            byte[] canonicalValue = Long.toString(id).getBytes(StandardCharsets.UTF_8);
            if (!Arrays.equals(canonicalValue, record.value())) {
                if (failureMessage == null) {
                    failureMessage =
                            "Acknowledged input record payload mismatched after reconciliation: "
                                    + id;
                }
                continue;
            }
            int index = Math.toIntExact(id);
            if (present[index]) {
                if (failureMessage == null) {
                    failureMessage =
                            "Kafka reconciliation returned a duplicate input record: " + id;
                }
                continue;
            }
            present[index] = true;
        }
        List<Long> observedIds = new ArrayList<>();
        for (int id = 0; id < present.length; id++) {
            if (present[id]) {
                observedIds.add((long) id);
            }
        }
        return new ReconciliationAnalysis(
                present, List.copyOf(observedIds), failureMessage, failureCause);
    }

    private static void validateRuntimeIdentity(
            ExecutableScenarioPlan.KafkaCluster cluster,
            ExecutableScenarioPlan.GeneratedIntegerSequenceInput input,
            KafkaRuntimeEndpoints endpoints) {
        if (!cluster.alias().equals(input.cluster())) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    "Generated input references a different Kafka cluster");
        }
        if (!cluster.alias().equals(endpoints.clusterAlias())
                || !cluster.imageReference().equals(endpoints.imageReference())) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    "Kafka endpoints do not match the capability-compiled cluster");
        }
        ExecutableScenarioPlan.KafkaTopic inputTopic = cluster.topics().stream()
                .filter(topic -> topic.name().equals(input.topic()))
                .findFirst()
                .orElseThrow(() -> new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        "Generated input topic is not declared by the Kafka cluster"));
        if (inputTopic.partitions() != input.partitions()) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    "Generated input partition count does not match the declared topic");
        }
    }

    private static Map<Integer, Long> exactOffsetMap(
            Map<Integer, Long> offsets, int partitions, String description) {
        if (offsets == null || offsets.size() != partitions) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    description + " do not cover every source partition");
        }
        TreeMap<Integer, Long> sorted = new TreeMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            Long offset = offsets.get(partition);
            if (offset == null || offset < 0) {
                throw new KafkaInputPreparationException(
                        KafkaInputPreparationException.INDETERMINATE,
                        description + " contain an invalid value for partition " + partition);
            }
            sorted.put(partition, offset);
        }
        if (!sorted.keySet().equals(offsets.keySet())) {
            throw new KafkaInputPreparationException(
                    KafkaInputPreparationException.INDETERMINATE,
                    description + " contain an undeclared source partition");
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(sorted));
    }

    private static long recordsForPartition(long total, int partitions, int partition) {
        long quotient = total / partitions;
        long remainder = total % partitions;
        return quotient + (partition < remainder ? 1 : 0);
    }

    private static String sha256(byte[] value) {
        try {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private record ValidatedOffsets(
            Map<Integer, Long> beginnings,
            Map<Integer, Long> exclusiveEnds) {}

    private record ReconciliationAnalysis(
            boolean[] present,
            List<Long> observedIds,
            String failureMessage,
            Throwable failureCause) {}
}
