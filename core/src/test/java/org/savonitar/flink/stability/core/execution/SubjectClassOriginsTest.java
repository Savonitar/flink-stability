package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.FlinkClassLoadLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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

    private FlinkClassLoadLog log(String process, String... lines) throws IOException {
        Path file = directory.resolve(process.replace('#', '-') + ".log");
        Files.writeString(file, String.join("\n", lines));
        return new FlinkClassLoadLog(process, file);
    }

    private static String line(String className, String source) {
        return "[1.234s][info][class,load] " + className + " source: " + source;
    }
}
