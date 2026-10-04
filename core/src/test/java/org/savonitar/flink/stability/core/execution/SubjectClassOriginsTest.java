package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;
import org.savonitar.flink.stability.runtime.api.FlinkComponentProvisioningEvidence;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.ImageConnectorArtifact;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubjectClassOriginsTest {
    private static final String SOURCE = "org.apache.flink.connector.kafka.source.KafkaSource";
    private static final String SINK = "org.apache.flink.connector.kafka.sink.KafkaSink";
    private static final List<String> ENTRY_CLASSES = List.of(SOURCE, SINK);
    private static final String PRIMARY =
            "/opt/flink/lib/flink-stability-connector-00000000-abc.jar";

    @TempDir
    Path directory;

    @Test
    void confirmsWhenEveryProcessLoadedTheEntryClassesFromThePrimary() throws IOException {
        SubjectClassOrigins origins = SubjectClassOrigins.read(List.of(
                        log("jobmanager-1#1", line(SOURCE, "file:" + PRIMARY)),
                        log("taskmanager-1#1",
                                line("java.lang.String", "jrt:/java.base"),
                                line(SOURCE, "file:" + PRIMARY),
                                line(SINK, "file:" + PRIMARY))),
                ENTRY_CLASSES, PRIMARY);

        assertEquals(SubjectClassOrigins.Outcome.CONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertEquals(List.of(PRIMARY),
                origins.processes().get(1).sources().get(SINK));
    }

    @Test
    void reportsAMismatchWhenAnyProcessLoadedAnotherCopy() throws IOException {
        String jobJar = "/tmp/flink-web-upload/blob_p-1234.jar";
        SubjectClassOrigins origins = SubjectClassOrigins.read(List.of(
                        log("taskmanager-1#1",
                                line(SOURCE, "file:" + PRIMARY),
                                line(SINK, "file:" + jobJar))),
                ENTRY_CLASSES, PRIMARY);

        assertEquals(SubjectClassOrigins.Outcome.MISMATCH, origins.outcome(ENTRY_CLASSES));
        assertTrue(origins.detail(ENTRY_CLASSES).contains(
                "taskmanager-1#1 loaded " + SINK + " from " + jobJar),
                origins.detail(ENTRY_CLASSES));
    }

    @Test
    void isUnconfirmedWhenNoTaskManagerLoadedTheEntryClasses() throws IOException {
        // The JobManager builds the job graph, but no TaskManager ever ran a task.
        SubjectClassOrigins origins = SubjectClassOrigins.read(List.of(
                        log("jobmanager-1#1", line(SOURCE, "file:" + PRIMARY),
                                line(SINK, "file:" + PRIMARY)),
                        log("taskmanager-1#1", line("java.lang.String", "jrt:/java.base"))),
                ENTRY_CLASSES, PRIMARY);

        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED, origins.outcome(ENTRY_CLASSES));
    }

    @Test
    void anUnreadableLogIsUnconfirmedEvidenceRatherThanAnException() {
        SubjectClassOrigins origins = SubjectClassOrigins.read(List.of(
                        new FlinkClassLoadLog("taskmanager-1#1", directory.resolve("missing.log"))),
                ENTRY_CLASSES, PRIMARY);

        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertTrue(origins.failure().orElseThrow().contains("missing.log"));
    }

    @Test
    void aConfirmedReplacementCannotHideItsMissingPredecessorLog() throws IOException {
        SubjectClassOrigins origins = SubjectClassOrigins.read(List.of(
                        new FlinkClassLoadLog("taskmanager-1#1", directory.resolve("missing.log")),
                        log("taskmanager-1#2", line(SOURCE, "file:" + PRIMARY),
                                line(SINK, "file:" + PRIMARY))),
                ENTRY_CLASSES, PRIMARY);

        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertTrue(origins.failure().orElseThrow().contains("taskmanager-1#1"));
        assertEquals(List.of("taskmanager-1#2"), origins.processes().stream()
                .map(SubjectClassOrigins.ProcessOrigin::process).toList(),
                "retain the readable replacement evidence despite the missing predecessor");
    }

    @Test
    void requiresOneTaskManagerToHaveLoadedEveryEntryClass() throws IOException {
        SubjectClassOrigins origins = SubjectClassOrigins.read(List.of(
                        log("taskmanager-1#1", line(SOURCE, "file:" + PRIMARY)),
                        log("taskmanager-1#2", line(SINK, "file:" + PRIMARY))),
                ENTRY_CLASSES, PRIMARY);

        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED, origins.outcome(ENTRY_CLASSES));
    }

    @Test
    void imageSubjectRequiresEveryTaskManagerIncarnationAndVerifiedBytes() throws IOException {
        var subject = new ImageConnectorArtifact("kafka", PRIMARY, "a".repeat(64));
        var first = provisioned("first", "taskmanager-1#1", subject);
        var second = provisioned("second", "taskmanager-1#2", subject);
        var complete = SubjectClassOrigins.read(List.of(
                log("taskmanager-1#1", line(SOURCE, PRIMARY), line(SINK, PRIMARY)),
                log("taskmanager-1#2", line(SOURCE, PRIMARY), line(SINK, PRIMARY))), ENTRY_CLASSES, PRIMARY);
        assertEquals(SubjectClassOrigins.Outcome.CONFIRMED,
                complete.requireEveryTaskManager(List.of(first, second), ENTRY_CLASSES, subject).outcome(ENTRY_CLASSES));
        var partial = SubjectClassOrigins.read(List.of(
                log("taskmanager-1#1", line(SOURCE, PRIMARY)),
                log("taskmanager-1#2", line(SOURCE, PRIMARY), line(SINK, PRIMARY))), ENTRY_CLASSES, PRIMARY);
        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED,
                partial.requireEveryTaskManager(List.of(first, second), ENTRY_CLASSES, subject).outcome(ENTRY_CLASSES));
        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED,
                complete.requireEveryTaskManager(List.of(first.withImageConnectorArtifacts(List.of()), second),
                        ENTRY_CLASSES, subject).outcome(ENTRY_CLASSES));
        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED,
                complete.requireEveryTaskManager(List.of(first, first), ENTRY_CLASSES, subject).outcome(ENTRY_CLASSES));
        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED,
                complete.requireEveryTaskManager(List.of(first), ENTRY_CLASSES, subject).outcome(ENTRY_CLASSES));
    }

    @Test
    void imageSubjectRejectsForeignImplementationClassesDespiteMatchingEntryPoints() throws IOException {
        String imagePrimary = "/vendor/subject.jar";
        String auxiliary = "/vendor/auxiliary/connector-implementation.jar";
        var subject = new ImageConnectorArtifact("kafka", imagePrimary, "a".repeat(64));
        for (String implementation : List.of(
                "org.apache.flink.connector.kafka.sink.KafkaWriter",
                "org.apache.flink.streaming.connectors.kafka.internals.FlinkKafkaInternalProducer")) {
            SubjectClassOrigins origins = SubjectClassOrigins.readImageSubject(List.of(
                    log("taskmanager-1#1", line(SOURCE, imagePrimary), line(SINK, imagePrimary),
                            line(implementation, "file:" + auxiliary))), ENTRY_CLASSES, imagePrimary)
                    .requireEveryTaskManager(List.of(provisioned("first", "taskmanager-1#1", subject)),
                            ENTRY_CLASSES, subject);
            assertEquals(SubjectClassOrigins.Outcome.MISMATCH, origins.outcome(ENTRY_CLASSES));
            assertEquals(List.of(auxiliary), origins.processes().getFirst().sources().get(implementation));
            assertTrue(origins.detail(ENTRY_CLASSES).contains(implementation));
            assertTrue(origins.detail(ENTRY_CLASSES).contains(auxiliary));
        }
    }

    @Test
    void imageSubjectRetainsMatchingImplementationOriginsAndIgnoresUnrelatedLibraries() throws IOException {
        String implementation = "org.apache.flink.connector.kafka.sink.KafkaWriter";
        SubjectClassOrigins origins = SubjectClassOrigins.readImageSubject(List.of(
                log("taskmanager-1#1", line(SOURCE, PRIMARY), line(SINK, PRIMARY),
                        line(implementation, "file:" + PRIMARY),
                        line("org.apache.kafka.clients.producer.KafkaProducer", "/vendor/kafka-clients.jar"),
                        line("com.vendor.flink.connector.Helper", "/vendor/helper.jar"))), ENTRY_CLASSES, PRIMARY);
        assertEquals(SubjectClassOrigins.Outcome.CONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertEquals(Map.of(SOURCE, List.of(PRIMARY), SINK, List.of(PRIMARY),
                implementation, List.of(PRIMARY)), origins.processes().getFirst().sources());
    }

    @Test
    void implementationOriginsCannotReplaceMissingRequiredEntryPointInAnIncarnation() throws IOException {
        var subject = new ImageConnectorArtifact("kafka", PRIMARY, "a".repeat(64));
        SubjectClassOrigins origins = SubjectClassOrigins.readImageSubject(List.of(
                log("taskmanager-1#1", line(SOURCE, PRIMARY),
                        line("org.apache.flink.connector.kafka.sink.KafkaWriter", PRIMARY)),
                log("taskmanager-1#2", line(SOURCE, PRIMARY), line(SINK, PRIMARY))), ENTRY_CLASSES, PRIMARY)
                .requireEveryTaskManager(List.of(provisioned("first", "taskmanager-1#1", subject),
                        provisioned("second", "taskmanager-1#2", subject)), ENTRY_CLASSES, subject);
        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertTrue(origins.failure().orElseThrow().contains("taskmanager-1#1"));
    }

    @Test
    void existingMavenAndLocalSubjectEvidenceKeepsOnlyTheRegisteredEntryClasses() throws IOException {
        SubjectClassOrigins origins = SubjectClassOrigins.read(List.of(
                log("taskmanager-1#1", line(SOURCE, PRIMARY), line(SINK, PRIMARY),
                        line("org.apache.flink.connector.kafka.sink.KafkaWriter", "/vendor/other.jar"))),
                ENTRY_CLASSES, PRIMARY);
        assertEquals(SubjectClassOrigins.Outcome.CONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertEquals(Map.of(SOURCE, List.of(PRIMARY), SINK, List.of(PRIMARY)),
                origins.processes().getFirst().sources());
    }

    @Test
    void recordedHiddenLambdaShapesKeepTheirConcreteHostOriginsAuthoritative() throws IOException {
        String writer = "org.apache.flink.connector.kafka.sink.KafkaWriter";
        String outer = "org.apache.flink.connector.kafka.sink.internal.TransactionOwnership";
        String inner = outer + "$2";
        SubjectClassOrigins origins = SubjectClassOrigins.readImageSubject(List.of(
                log("taskmanager-1#1", line(SOURCE, PRIMARY), line(SINK, PRIMARY),
                        line(writer, PRIMARY), line(outer, PRIMARY), line(inner, PRIMARY),
                        line(writer + "$$Lambda$937/0x000000b80166ed48", writer),
                        line(inner + "$$Lambda$1265/0x000000a0017a2f20", outer),
                        line(writer + "$$Lambda$938/0x000000b80166ed49", "__JVM_LookupDefineClass__"))),
                ENTRY_CLASSES, PRIMARY);
        assertEquals(SubjectClassOrigins.Outcome.CONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertEquals(Map.of(SOURCE, List.of(PRIMARY), SINK, List.of(PRIMARY),
                writer, List.of(PRIMARY), outer, List.of(PRIMARY), inner, List.of(PRIMARY)),
                origins.processes().getFirst().sources());
    }

    @Test
    void hiddenSpellingCannotHideForeignConcreteOrUnexpectedJarOrigins() throws IOException {
        String writer = "org.apache.flink.connector.kafka.sink.KafkaWriter";
        String hidden = writer + "$$Lambda$1/0x0123abcd";
        for (String[] loads : List.of(
                new String[]{line(writer, "/vendor/foreign.jar"), line(hidden, writer)},
                new String[]{line(writer, PRIMARY), line(hidden, "/vendor/foreign.jar")},
                new String[]{line(writer, "__JVM_LookupDefineClass__")},
                new String[]{line(writer, PRIMARY), line(writer + "$$Lambda$1/not-a-hidden-id", writer)})) {
            var lines = new ArrayList<>(List.of(line(SOURCE, PRIMARY), line(SINK, PRIMARY)));
            lines.addAll(List.of(loads));
            var origins = SubjectClassOrigins.readImageSubject(List.of(log("taskmanager-1#1",
                    lines.toArray(String[]::new))), ENTRY_CLASSES, PRIMARY);
            assertEquals(SubjectClassOrigins.Outcome.MISMATCH, origins.outcome(ENTRY_CLASSES));
        }
    }

    @Test
    void hiddenLambdaWithoutItsConcreteHostIsUnconfirmed() throws IOException {
        String writer = "org.apache.flink.connector.kafka.sink.KafkaWriter";
        var origins = SubjectClassOrigins.readImageSubject(List.of(
                log("taskmanager-1#1", line(SOURCE, PRIMARY), line(SINK, PRIMARY),
                        line(writer + "$$Lambda$1/0x0123abcd", writer))), ENTRY_CLASSES, PRIMARY);
        assertEquals(SubjectClassOrigins.Outcome.UNCONFIRMED, origins.outcome(ENTRY_CLASSES));
        assertTrue(origins.failure().orElseThrow().contains("declaring-class origin: " + writer));
    }

    private static FlinkComponentProvisioningEvidence provisioned(
            String id, String process, ImageConnectorArtifact subject) {
        return FlinkComponentProvisioningEvidence.verified(
                "taskmanager-1", FlinkComponentRole.TASK_MANAGER,
                id, "flink:2.2.0", "sha256:" + "b".repeat(64), "c".repeat(64), "d".repeat(64), List.of())
                .withProcessConfiguration(Map.of(), process).withImageConnectorArtifacts(List.of(subject));
    }

    private FlinkClassLoadLog log(String process, String... lines) throws IOException {
        Path file = directory.resolve(process.replace('#', '-') + ".log");
        Files.writeString(file, String.join("\n", lines));
        return new FlinkClassLoadLog(process, file);
    }

    private static String line(String className, String source) {
        return "[1.234s][info][class,load] " + className + " source: " + source;
    }
}
