package org.savonitar.flink.stability.core.execution.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeEndpoints;
import org.savonitar.flink.stability.runtime.api.KafkaRuntimeTarget;
import org.savonitar.flink.stability.runtime.api.KafkaBrokerPolicy;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaInputPreparerTest {

    @Test
    void preparesExactTopicsAndImmutableAcknowledgedPresentEvidence() {
        List<String> events = new ArrayList<>();
        FakeOperations operations = new FakeOperations(events, 2, 5);
        KafkaInputPreparer preparer = preparer(events, operations);
        KafkaRuntimeEndpoints runtimeEndpoints = endpoints();

        PreparedKafkaInput prepared = preparer.prepare(cluster(), input(5), runtimeEndpoints);

        assertEquals(runtimeEndpoints, prepared.endpoints());
        assertEquals(
                List.of(
                        topicDefinition("input"),
                        topicDefinition("output")),
                operations.createdTopics);
        assertEquals("input", operations.producedTopic);
        assertEquals(2, operations.producedPartitions);
        assertEquals(5, operations.producedTotal);
        assertEquals(Map.of(0, 3L, 1, 2L), operations.reconciledBounds);
        assertEquals(
                List.of(
                        "open:localhost:19093",
                        "create",
                        "produce",
                        "offsets",
                        "reconcile",
                        "close"),
                events);

        KafkaInputManifest manifest = prepared.inputManifest();
        assertEquals("main", manifest.clusterAlias());
        assertEquals("input", manifest.topic());
        assertEquals(5, manifest.totalRecords());
        assertEquals(Map.of(0, 0L, 1, 0L), manifest.beginningOffsets());
        assertEquals(Map.of(0, 3L, 1, 2L), manifest.exclusiveEndOffsets());
        assertEquals(manifest.exclusiveEndOffsets(), prepared.stoppingOffsets());
        assertEquals(5, manifest.records().size());
        assertEquals(KafkaInputManifest.EvidenceStatus.COMPLETE, manifest.evidenceStatus());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L),
                manifest.reconciliation().observedIds());
        assertEquals(operations.consumerConfiguration,
                manifest.reconciliation().consumerConfiguration());
        assertTrue(manifest.reconciliation().reachedEveryExclusiveEnd());

        KafkaInputManifest.RecordAcknowledgement first = manifest.records().getFirst();
        assertEquals(0, first.id());
        assertEquals(
                "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9",
                first.payloadSha256());
        assertEquals(0, first.partition());
        assertEquals(0, first.offset());
        assertEquals(
                List.of(new KafkaInputManifest.ProducerAttempt(
                        1,
                        0,
                        0,
                        0,
                        KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED)),
                first.producerAttempts());
        assertEquals(
                KafkaInputManifest.AcknowledgementOutcome.ACKNOWLEDGED,
                first.acknowledgementOutcome());
        assertEquals(
                KafkaInputManifest.TerminalDisposition.PRESENT,
                first.terminalDisposition());

        KafkaInputManifest.RecordAcknowledgement last = manifest.records().getLast();
        assertEquals(4, last.id());
        assertEquals(0, last.partition());
        assertEquals(2, last.offset());
        assertThrows(
                UnsupportedOperationException.class,
                () -> manifest.records().add(first));
        assertThrows(
                UnsupportedOperationException.class,
                () -> manifest.exclusiveEndOffsets().put(2, 0L));
        assertThrows(
                UnsupportedOperationException.class,
                () -> first.producerAttempts().clear());
        assertThrows(
                UnsupportedOperationException.class,
                () -> manifest.reconciliation().consumerConfiguration().clear());
        assertThrows(
                UnsupportedOperationException.class,
                () -> manifest.reconciliation().observedIds().clear());
    }

    @Test
    void classifiesAcknowledgedRecordMissingOrMismatchedDuringReconciliation() {
        List<String> events = new ArrayList<>();
        FakeOperations missing = new FakeOperations(events, 2, 5);
        missing.observed.removeLast();

        KafkaInputPreparationException absent = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, missing).prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.ACKNOWLEDGED_MISSING,
                absent.reasonCode());
        KafkaInputManifest absentEvidence = absent.evidence().orElseThrow();
        assertEquals(KafkaInputManifest.EvidenceStatus.PARTIAL,
                absentEvidence.evidenceStatus());
        assertEquals(List.of(0L, 1L, 2L, 3L),
                absentEvidence.reconciliation().observedIds());
        assertEquals(KafkaInputManifest.TerminalDisposition.ACKNOWLEDGED_MISSING,
                absentEvidence.records().get(4).terminalDisposition());
        assertEquals("close", events.getLast());

        events.clear();
        FakeOperations mismatched = new FakeOperations(events, 2, 5);
        mismatched.observed.set(2, new KafkaInputOperations.ObservedRecord(
                "not-2".getBytes(StandardCharsets.UTF_8), 0, 1));
        KafkaInputPreparationException mismatch = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, mismatched).prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.ACKNOWLEDGED_MISSING,
                mismatch.reasonCode());
        KafkaInputManifest mismatchEvidence = mismatch.evidence().orElseThrow();
        assertEquals(List.of(0L, 1L, 3L, 4L),
                mismatchEvidence.reconciliation().observedIds());
        assertEquals(KafkaInputManifest.TerminalDisposition.ACKNOWLEDGED_MISSING,
                mismatchEvidence.records().get(2).terminalDisposition());
        assertEquals("close", events.getLast());
    }

    @Test
    void classifiesIncompleteBoundsOrReconciliationAsIndeterminate() {
        List<String> events = new ArrayList<>();
        FakeOperations nonzeroBeginning = new FakeOperations(events, 2, 5);
        nonzeroBeginning.beginnings.put(0, 1L);

        KafkaInputPreparationException nonzero = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, nonzeroBeginning)
                        .prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.INDETERMINATE, nonzero.reasonCode());
        assertTrue(nonzero.evidence().isEmpty());
        assertFalse(events.contains("reconcile"));
        assertEquals("close", events.getLast());

        events.clear();
        FakeOperations deadline = new FakeOperations(events, 2, 5);
        deadline.observed.removeLast();
        deadline.reachedEveryEnd = false;
        KafkaInputPreparationException incomplete = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, deadline).prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.INDETERMINATE,
                incomplete.reasonCode());
        KafkaInputManifest incompleteEvidence = incomplete.evidence().orElseThrow();
        assertFalse(incompleteEvidence.reconciliation().reachedEveryExclusiveEnd());
        assertEquals(KafkaInputManifest.TerminalDisposition.INDETERMINATE,
                incompleteEvidence.records().get(4).terminalDisposition());
        assertEquals("close", events.getLast());
    }

    @Test
    void evidenceIsDeterministicAndDefensivelyCopiesOperationInputs() {
        List<String> firstEvents = new ArrayList<>();
        FakeOperations firstOperations = new FakeOperations(firstEvents, 2, 5);
        KafkaInputManifest first = preparer(firstEvents, firstOperations)
                .prepare(cluster(), input(5), endpoints())
                .inputManifest();

        firstOperations.consumerConfiguration.put("mutated", "after-prepare");
        firstOperations.observed.clear();

        List<String> secondEvents = new ArrayList<>();
        FakeOperations secondOperations = new FakeOperations(secondEvents, 2, 5);
        KafkaInputManifest second = preparer(secondEvents, secondOperations)
                .prepare(cluster(), input(5), endpoints())
                .inputManifest();

        assertEquals(second, first);
        assertFalse(first.reconciliation().consumerConfiguration().containsKey("mutated"));
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L),
                first.reconciliation().observedIds());
    }

    @Test
    void classifiesClientOrTopicFailureAsInfrastructureAndAlwaysCloses() {
        List<String> events = new ArrayList<>();
        FakeOperations operations = new FakeOperations(events, 2, 5);
        operations.createFailure = new IOException("broker unreachable");

        KafkaInputPreparationException failure = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, operations).prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                failure.reasonCode());
        assertEquals("broker unreachable", failure.getCause().getMessage());
        assertEquals(List.of("open:localhost:19093", "create", "close"), events);
    }

    @Test
    void retainsCompleteManifestWhenReconciliationConsumerCloseFails() {
        List<String> events = new ArrayList<>();
        FakeOperations operations = new FakeOperations(events, 2, 5);
        IllegalStateException closeFailure = new IllegalStateException(
                "consumer close failed");
        operations.reconciliationCloseFailure = closeFailure;

        KafkaInputPreparationException failure = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, operations).prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                failure.reasonCode());
        assertSame(closeFailure, failure.getCause().getCause());
        KafkaInputManifest evidence = failure.evidence().orElseThrow();
        assertEquals(KafkaInputManifest.EvidenceStatus.COMPLETE, evidence.evidenceStatus());
        assertEquals(Map.of(0, 0L, 1, 0L), evidence.beginningOffsets());
        assertEquals(Map.of(0, 3L, 1, 2L), evidence.exclusiveEndOffsets());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L),
                evidence.reconciliation().observedIds());
        assertTrue(evidence.reconciliation().reachedEveryExclusiveEnd());
        assertEquals("close", events.getLast());
    }

    @Test
    void retainsPartialManifestWhenReconciliationFailsMidTraversal() {
        // Review finding F12: three acknowledged records with closed bounds, only ID 0
        // observed, then the consumer throws.
        List<String> events = new ArrayList<>();
        FakeOperations operations = new FakeOperations(events, 2, 3);
        operations.observed.subList(1, 3).clear();
        IllegalStateException pollFailure = new IllegalStateException("poll failed");
        operations.reconciliationTraversalFailure = pollFailure;

        KafkaInputPreparationException failure = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, operations).prepare(cluster(), input(3), endpoints()));

        assertEquals(KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                failure.reasonCode());
        assertSame(pollFailure, failure.getCause().getCause(),
                "the consumer failure stays primary");
        KafkaInputManifest evidence = failure.evidence().orElseThrow();
        assertEquals(KafkaInputManifest.EvidenceStatus.PARTIAL, evidence.evidenceStatus());
        assertEquals(Map.of(0, 2L, 1, 1L), evidence.exclusiveEndOffsets());
        assertEquals(List.of(0L), evidence.reconciliation().observedIds());
        assertFalse(evidence.reconciliation().reachedEveryExclusiveEnd());
        assertEquals(
                List.of(
                        KafkaInputManifest.TerminalDisposition.PRESENT,
                        KafkaInputManifest.TerminalDisposition.INDETERMINATE,
                        KafkaInputManifest.TerminalDisposition.INDETERMINATE),
                evidence.records().stream()
                        .map(KafkaInputManifest.RecordAcknowledgement::terminalDisposition)
                        .toList());
        assertEquals("close", events.getLast());
    }

    @Test
    void clientPollFailureCarriesThePartialSnapshotObservedSoFar() {
        String topic = "input";
        TopicPartition partition = new TopicPartition(topic, 0);
        org.apache.kafka.common.KafkaException pollFailure =
                new org.apache.kafka.common.KafkaException("broker connection lost");
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.updateEndOffsets(Map.of(partition, 3L));
        consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(
                topic, 0, 0, null, "0".getBytes(StandardCharsets.UTF_8))));
        consumer.schedulePollTask(() -> consumer.setPollException(pollFailure));
        KafkaClientInputOperations operations = new KafkaClientInputOperations(
                "localhost:19093",
                Duration.ofSeconds(1),
                unusedAdmin(),
                configuration -> consumer);

        KafkaInputOperations.ReconciliationSnapshotException failure = assertThrows(
                KafkaInputOperations.ReconciliationSnapshotException.class,
                () -> operations.reconcileFromZeroThrough(
                        topic,
                        Map.of(0, 3L),
                        new KafkaInputPreparationDeadline(
                                Duration.ofSeconds(1), System::nanoTime)));

        assertSame(pollFailure, failure.getCause());
        KafkaInputOperations.ReconciliationSnapshot snapshot = failure.snapshot();
        assertFalse(snapshot.reachedEveryExclusiveEnd());
        assertEquals(1, snapshot.records().size());
        assertEquals("0", new String(
                snapshot.records().getFirst().value(), StandardCharsets.UTF_8));
        assertEquals("flink-stability-input-reconciliation",
                snapshot.consumerConfiguration().get("client.id"));
        assertTrue(consumer.closed());
    }

    @Test
    void retainsCompleteManifestWhenAdminCloseFails() {
        List<String> events = new ArrayList<>();
        FakeOperations operations = new FakeOperations(events, 2, 5);
        IOException closeFailure = new IOException("admin close failed");
        operations.closeFailure = closeFailure;

        KafkaInputPreparationException failure = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, operations).prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                failure.reasonCode());
        assertSame(closeFailure, failure.getCause());
        KafkaInputManifest evidence = failure.evidence().orElseThrow();
        assertEquals(KafkaInputManifest.EvidenceStatus.COMPLETE, evidence.evidenceStatus());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L),
                evidence.reconciliation().observedIds());
        assertTrue(evidence.reconciliation().reachedEveryExclusiveEnd());
        assertEquals("close", events.getLast());
    }

    @Test
    void closeFailuresDoNotOverrideSubstantiveInputManifestFailures() {
        List<String> events = new ArrayList<>();
        FakeOperations consumerFailure = new FakeOperations(events, 2, 5);
        consumerFailure.observed.removeLast();
        consumerFailure.reconciliationCloseFailure = new IllegalStateException(
                "consumer close failed");

        KafkaInputPreparationException consumerResult = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, consumerFailure)
                        .prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.ACKNOWLEDGED_MISSING,
                consumerResult.reasonCode());
        assertEquals(KafkaInputManifest.EvidenceStatus.PARTIAL,
                consumerResult.evidence().orElseThrow().evidenceStatus());
        assertEquals(1, consumerResult.getSuppressed().length);
        assertEquals("consumer close failed",
                consumerResult.getSuppressed()[0].getCause().getMessage());

        events.clear();
        FakeOperations adminFailure = new FakeOperations(events, 2, 5);
        adminFailure.observed.removeLast();
        adminFailure.closeFailure = new IOException("admin close failed");

        KafkaInputPreparationException adminResult = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, adminFailure)
                        .prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.ACKNOWLEDGED_MISSING,
                adminResult.reasonCode());
        assertEquals(KafkaInputManifest.EvidenceStatus.PARTIAL,
                adminResult.evidence().orElseThrow().evidenceStatus());
        assertEquals(1, adminResult.getSuppressed().length);
        assertEquals("admin close failed", adminResult.getSuppressed()[0].getMessage());
    }

    @Test
    void preservesInterruptionAndClosesTheClientBoundary() {
        List<String> events = new ArrayList<>();
        FakeOperations operations = new FakeOperations(events, 2, 5);
        operations.interruptProduce = true;

        try {
            KafkaInputPreparationException failure = assertThrows(
                    KafkaInputPreparationException.class,
                    () -> preparer(events, operations)
                            .prepare(cluster(), input(5), endpoints()));

            assertEquals(KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                    failure.reasonCode());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(
                    List.of("open:localhost:19093", "create", "produce", "close"),
                    events);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void usesOneNonResettingDeadlineAcrossKafkaInputPreparationStages() {
        List<String> events = new ArrayList<>();
        MutableNanoClock clock = new MutableNanoClock(-500);
        List<Duration> openTimeouts = new ArrayList<>();
        FakeOperations operations = new FakeOperations(events, 2, 5);
        operations.afterCreate = () -> clock.advance(Duration.ofNanos(30));
        operations.afterProduce = () -> clock.advance(Duration.ofNanos(30));
        operations.afterOffsets = () -> clock.advance(Duration.ofNanos(40));
        KafkaInputPreparer preparer = new KafkaInputPreparer(
                Duration.ofNanos(100),
                clock::nanoTime,
                (host, timeout) -> {
                    events.add("open:" + host);
                    openTimeouts.add(timeout);
                    return operations;
                });

        KafkaInputPreparationException failure = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer.prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                failure.reasonCode());
        assertTrue(failure.getCause().getMessage().contains("timed out"));
        assertEquals(List.of(
                "open:localhost:19093",
                "create",
                "produce",
                "offsets",
                "close"), events);
        assertEquals(List.of(Duration.ofNanos(100)), openTimeouts);
        assertEquals(List.of(
                Duration.ofNanos(100),
                Duration.ofNanos(70),
                Duration.ofNanos(40)), operations.stageBudgets);
        assertEquals(List.of(Duration.ZERO), operations.closeBudgets);
    }

    @Test
    void retainsCompleteEvidenceAndPrimaryDeadlineFailureWhenCleanupAlsoFails() {
        List<String> events = new ArrayList<>();
        MutableNanoClock clock = new MutableNanoClock(0);
        FakeOperations operations = new FakeOperations(events, 2, 5);
        operations.afterReconcile = () -> clock.advance(Duration.ofNanos(100));
        IOException closeFailure = new IOException("admin close failed");
        operations.closeFailure = closeFailure;
        KafkaInputPreparer preparer = new KafkaInputPreparer(
                Duration.ofNanos(100),
                clock::nanoTime,
                (host, timeout) -> {
                    events.add("open:" + host);
                    return operations;
                });

        KafkaInputPreparationException failure = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer.prepare(cluster(), input(5), endpoints()));

        assertEquals(KafkaInputPreparationException.INFRASTRUCTURE_SETUP_FAILED,
                failure.reasonCode());
        assertTrue(failure.getCause().getMessage().contains("timed out"));
        assertEquals(KafkaInputManifest.EvidenceStatus.COMPLETE,
                failure.evidence().orElseThrow().evidenceStatus());
        assertEquals(List.of(closeFailure), List.of(failure.getSuppressed()));
        assertEquals(List.of(Duration.ZERO), operations.closeBudgets);
    }

    @Test
    void defensivelyRejectsInputsBeyondTheSharedInMemoryBoundaryBeforeOpeningKafka() {
        List<String> events = new ArrayList<>();
        long tooMany = ExecutableScenarioPlan
                .FIRST_RUNNER_IN_MEMORY_MAX_GENERATED_INPUT_RECORDS + 1;

        KafkaInputPreparationException failure = assertThrows(
                KafkaInputPreparationException.class,
                () -> preparer(events, new FakeOperations(events, 2, 1))
                        .prepare(cluster(), input(tooMany), endpoints()));

        assertEquals(KafkaInputPreparationException.INDETERMINATE, failure.reasonCode());
        assertEquals(List.of(), events);
    }

    @Test
    void adaptsTheCompiledBrokerPolicyWithoutReinterpretingIt() {
        KafkaRuntimeTarget target = cluster().runtimeTarget();

        assertEquals("main", target.clusterAlias());
        assertEquals("apache/kafka:4.0.0", target.imageReference());
        assertEquals("kafka-main:19092", target.internalBootstrapServers());
        assertEquals(cluster().brokerPolicy().kafkaConfiguration(),
                target.brokerPolicy().kafkaConfiguration());
    }

    @Test
    void configuresBoundedKafkaClientsAndReconciliationWithoutAutoCreation() {
        Duration timeout = Duration.ofMinutes(2);
        Properties producer = KafkaClientInputOperations.producerProperties(
                "localhost:19093", timeout);
        Properties consumer = KafkaClientInputOperations.consumerProperties(
                "localhost:19093", timeout);
        Properties admin = KafkaClientInputOperations.adminProperties(
                "localhost:19093", timeout);

        assertEquals("4.0.0", AppInfoParser.getVersion());
        assertEquals("all", producer.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("true", producer.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("120000", producer.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertEquals("60000", producer.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals("false", consumer.get(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG));
        assertEquals("read_uncommitted", consumer.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        assertEquals("false", consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(
                Map.ofEntries(
                        Map.entry("allow.auto.create.topics", "false"),
                        Map.entry("auto.offset.reset", "earliest"),
                        Map.entry("bootstrap.servers", "localhost:19093"),
                        Map.entry("client.id", "flink-stability-input-reconciliation"),
                        Map.entry("default.api.timeout.ms", "120000"),
                        Map.entry("enable.auto.commit", "false"),
                        Map.entry("isolation.level", "read_uncommitted"),
                        Map.entry(
                                "key.deserializer",
                                "org.apache.kafka.common.serialization.ByteArrayDeserializer"),
                        Map.entry("request.timeout.ms", "60000"),
                        Map.entry(
                                "value.deserializer",
                                "org.apache.kafka.common.serialization.ByteArrayDeserializer")),
                KafkaClientInputOperations.configurationEvidence(consumer));
        assertEquals("localhost:19093", admin.get("bootstrap.servers"));
        assertEquals("120000", admin.get("default.api.timeout.ms"));
        assertEquals(
                Map.of("cleanup.policy", "delete", "retention.ms", "604800000"),
                KafkaClientInputOperations.topicConfiguration(topicDefinition("input")));
    }

    @Test
    void clientConsumerCloseFailureCarriesTheCompletedImmutableSnapshot() {
        String topic = "input";
        TopicPartition partition = new TopicPartition(topic, 0);
        IllegalStateException closeFailure = new IllegalStateException(
                "consumer close failed");
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest") {
            @Override
            public synchronized void close(Duration timeout) {
                throw closeFailure;
            }
        };
        consumer.updateBeginningOffsets(Map.of(partition, 0L));
        consumer.updateEndOffsets(Map.of(partition, 1L));
        consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(
                topic,
                0,
                0,
                null,
                "0".getBytes(StandardCharsets.UTF_8))));
        KafkaClientInputOperations operations = new KafkaClientInputOperations(
                "localhost:19093",
                Duration.ofSeconds(1),
                unusedAdmin(),
                configuration -> consumer);

        KafkaInputOperations.ReconciliationSnapshotException failure = assertThrows(
                KafkaInputOperations.ReconciliationSnapshotException.class,
                () -> operations.reconcileFromZeroThrough(
                        topic,
                        Map.of(0, 1L),
                        new KafkaInputPreparationDeadline(
                                Duration.ofSeconds(1), System::nanoTime)));

        assertSame(closeFailure, failure.getCause());
        KafkaInputOperations.ReconciliationSnapshot snapshot = failure.snapshot();
        assertTrue(snapshot.reachedEveryExclusiveEnd());
        assertEquals(1, snapshot.records().size());
        assertEquals("0", new String(
                snapshot.records().getFirst().value(), StandardCharsets.UTF_8));
        assertEquals("flink-stability-input-reconciliation",
                snapshot.consumerConfiguration().get("client.id"));
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.records().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.consumerConfiguration().clear());
    }

    @Test
    void blockingProducerSendCannotHoldTheCallerPastTheRemainingStageBudget() {
        CountDownLatch sendEntered = new CountDownLatch(1);
        CountDownLatch sendInterrupted = new CountDownLatch(1);
        CountDownLatch releaseSend = new CountDownLatch(1);
        AtomicReference<Duration> closeTimeout = new AtomicReference<>();
        KafkaClientInputOperations.InputProducer producer =
                new KafkaClientInputOperations.InputProducer() {
                    @Override
                    public Future<RecordMetadata> send(
                            ProducerRecord<byte[], byte[]> record) {
                        sendEntered.countDown();
                        boolean released = false;
                        while (!released) {
                            try {
                                releaseSend.await();
                                released = true;
                            } catch (InterruptedException ignored) {
                                sendInterrupted.countDown();
                                // Deliberately ignore cancellation, as a stuck Kafka call may do.
                            }
                        }
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void close(Duration timeout) {
                        closeTimeout.set(timeout);
                    }
                };
        KafkaClientInputOperations operations = new KafkaClientInputOperations(
                "localhost:19093",
                Duration.ofSeconds(1),
                unusedAdmin(),
                properties -> producer,
                properties -> {
                    throw new AssertionError("Unexpected consumer creation");
                });
        MutableNanoClock clock = new MutableNanoClock(0);
        KafkaInputPreparationDeadline deadline = new KafkaInputPreparationDeadline(
                Duration.ofMillis(100), clock::nanoTime);
        clock.advance(Duration.ofMillis(75));

        KafkaInputPreparationDeadline.KafkaInputPreparationDeadlineExceededException failure;
        try {
            failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertThrows(
                            KafkaInputPreparationDeadline
                                    .KafkaInputPreparationDeadlineExceededException.class,
                            () -> operations.produceAndAwait(
                                    "input", 1, 1, deadline)));
        } finally {
            releaseSend.countDown();
        }

        assertTrue(failure.getCause() instanceof TimeoutException);
        assertTrue(await(sendEntered));
        assertTrue(await(sendInterrupted));
        assertEquals(Duration.ofMillis(25), closeTimeout.get());
    }

    @Test
    void expiredDeadlineDoesNotScheduleKafkaInvocation() {
        MutableNanoClock clock = new MutableNanoClock(-1);
        KafkaInputPreparationDeadline deadline = new KafkaInputPreparationDeadline(
                Duration.ofNanos(1), clock::nanoTime);
        clock.advance(Duration.ofNanos(1));
        AtomicBoolean invoked = new AtomicBoolean();

        try (KafkaInputCallBoundary boundary = new KafkaInputCallBoundary(
                "expired-deadline-test")) {
            assertThrows(
                    KafkaInputPreparationDeadline
                            .KafkaInputPreparationDeadlineExceededException.class,
                    () -> boundary.call(
                            "sending after expiry",
                            deadline,
                            () -> {
                                invoked.set(true);
                                return null;
                            }));
        }

        assertFalse(invoked.get());
    }

    private static KafkaInputPreparer preparer(
            List<String> events, FakeOperations operations) {
        return new KafkaInputPreparer((host, timeout) -> {
            events.add("open:" + host);
            return operations;
        });
    }

    private static ExecutableScenarioPlan.KafkaCluster cluster() {
        return new ExecutableScenarioPlan.KafkaCluster(
                "main",
                "apache/kafka:4.0.0",
                ExecutableScenarioPlan.KafkaMode.KRAFT,
                1,
                KafkaBrokerPolicy.v1SingleBroker(),
                List.of(
                        new ExecutableScenarioPlan.KafkaTopic("input", 2, 1),
                        new ExecutableScenarioPlan.KafkaTopic("output", 2, 1)));
    }

    private static ExecutableScenarioPlan.GeneratedIntegerSequenceInput input(long records) {
        return new ExecutableScenarioPlan.GeneratedIntegerSequenceInput(
                "main", "input", 2, records);
    }

    private static KafkaRuntimeEndpoints endpoints() {
        return new KafkaRuntimeEndpoints(
                "main", "apache/kafka:4.0.0", "kafka-main:19092", "localhost:19093");
    }

    private static Admin unusedAdmin() {
        return (Admin) Proxy.newProxyInstance(
                Admin.class.getClassLoader(),
                new Class<?>[] {Admin.class},
                (proxy, method, arguments) -> {
                    throw new AssertionError("Unexpected Admin call: " + method.getName());
                });
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Test was interrupted", failure);
        }
    }

    private static KafkaInputOperations.TopicDefinition topicDefinition(String name) {
        return new KafkaInputOperations.TopicDefinition(
                name,
                2,
                (short) 1,
                KafkaInputOperations.TopicCleanupPolicy.DELETE,
                KafkaInputPreparer.HARNESS_TOPIC_RETENTION);
    }

    private static final class FakeOperations implements KafkaInputOperations {
        private final List<String> events;
        private final List<ProducedRecord> produced = new ArrayList<>();
        private final List<ObservedRecord> observed = new ArrayList<>();
        private final Map<Integer, Long> beginnings = new LinkedHashMap<>();
        private final Map<Integer, Long> ends = new LinkedHashMap<>();
        private final Map<String, String> consumerConfiguration = new LinkedHashMap<>();
        private final List<Duration> stageBudgets = new ArrayList<>();
        private final List<Duration> closeBudgets = new ArrayList<>();
        private List<TopicDefinition> createdTopics;
        private String producedTopic;
        private int producedPartitions;
        private long producedTotal;
        private Map<Integer, Long> reconciledBounds;
        private Exception createFailure;
        private Exception closeFailure;
        private RuntimeException reconciliationCloseFailure;
        private RuntimeException reconciliationTraversalFailure;
        private boolean interruptProduce;
        private boolean reachedEveryEnd = true;
        private Runnable afterCreate = () -> {};
        private Runnable afterProduce = () -> {};
        private Runnable afterOffsets = () -> {};
        private Runnable afterReconcile = () -> {};

        private FakeOperations(List<String> events, int partitions, long records) {
            this.events = events;
            consumerConfiguration.put("bootstrap.servers", "localhost:19093");
            consumerConfiguration.put(
                    "client.id", "flink-stability-input-reconciliation");
            consumerConfiguration.put("enable.auto.commit", "false");
            for (int partition = 0; partition < partitions; partition++) {
                beginnings.put(partition, 0L);
                ends.put(partition, recordsForPartition(records, partitions, partition));
            }
            for (long id = 0; id < records; id++) {
                int partition = (int) (id % partitions);
                long offset = id / partitions;
                produced.add(new ProducedRecord(
                        id,
                        partition,
                        offset,
                        List.of(new HarnessProducerAttempt(
                                1, partition, partition, offset))));
                observed.add(new ObservedRecord(
                        Long.toString(id).getBytes(StandardCharsets.UTF_8),
                        partition,
                        offset));
            }
        }

        @Override
        public void createTopics(
                List<TopicDefinition> topics,
                KafkaInputPreparationDeadline deadline) throws Exception {
            events.add("create");
            createdTopics = List.copyOf(topics);
            if (createFailure != null) {
                throw createFailure;
            }
            stageBudgets.add(deadline.remaining());
            afterCreate.run();
        }

        @Override
        public List<ProducedRecord> produceAndAwait(
                String topic,
                int partitions,
                long totalRecords,
                KafkaInputPreparationDeadline deadline) throws Exception {
            events.add("produce");
            producedTopic = topic;
            producedPartitions = partitions;
            producedTotal = totalRecords;
            if (interruptProduce) {
                throw new InterruptedException("interrupted");
            }
            stageBudgets.add(deadline.remaining());
            afterProduce.run();
            return List.copyOf(produced);
        }

        @Override
        public OffsetSnapshot readOffsets(
                String topic,
                int partitions,
                KafkaInputPreparationDeadline deadline) {
            events.add("offsets");
            stageBudgets.add(deadline.remaining());
            afterOffsets.run();
            return new OffsetSnapshot(Map.copyOf(beginnings), Map.copyOf(ends));
        }

        @Override
        public ReconciliationSnapshot reconcileFromZeroThrough(
                String topic,
                Map<Integer, Long> exclusiveEndOffsets,
                KafkaInputPreparationDeadline deadline) {
            events.add("reconcile");
            stageBudgets.add(deadline.remaining());
            reconciledBounds = Map.copyOf(exclusiveEndOffsets);
            if (reconciliationTraversalFailure != null) {
                throw new ReconciliationSnapshotException(
                        "Kafka input reconciliation failed after observing " + observed.size()
                                + " records",
                        reconciliationTraversalFailure,
                        new ReconciliationSnapshot(observed, false, consumerConfiguration));
            }
            ReconciliationSnapshot snapshot = new ReconciliationSnapshot(
                    observed, reachedEveryEnd, consumerConfiguration);
            if (reconciliationCloseFailure != null) {
                throw new ReconciliationSnapshotException(
                        "Kafka reconciliation consumer failed to close after its snapshot was "
                                + "captured",
                        reconciliationCloseFailure,
                        snapshot);
            }
            afterReconcile.run();
            return snapshot;
        }

        @Override
        public void close(Duration timeout) throws Exception {
            events.add("close");
            closeBudgets.add(timeout);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        private static long recordsForPartition(long total, int partitions, int partition) {
            return total / partitions + (partition < total % partitions ? 1 : 0);
        }
    }

    private static final class MutableNanoClock {
        private long now;

        private MutableNanoClock(long now) {
            this.now = now;
        }

        private long nanoTime() {
            return now;
        }

        private void advance(Duration duration) {
            now += duration.toNanos();
        }
    }
}
