package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.FlinkComponentLog;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;

class ComponentErrorEvidenceTest {
    @TempDir Path directory;

    private FlinkComponentLog log(String process, String text) throws Exception {
        return new FlinkComponentLog(process, Files.writeString(directory.resolve(process + ".log"), text),
                false, Optional.empty());
    }

    @Test void realFlinkHeadersDeduplicateCopiesButKeepIncarnationsAndTransactionKeys() throws Exception {
        String fixture;
        try (var input = getClass().getResourceAsStream("/component-errors/kafka-committer.log")) {
            fixture = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
        var first = log("taskmanager-2#1", fixture + fixture);
        var replacement = log("taskmanager-2#2", fixture);
        var evidence = ComponentErrorEvidence.collect(List.of(first, first, replacement));
        assertEquals(6, evidence.events().size());
        assertEquals(4, evidence.events().stream().filter(e -> e.level().equals("ERROR")
                && e.logger().equals("org.apache.flink.connector.kafka.sink.internal.KafkaCommitter")).count());
        assertEquals(2, evidence.events().stream().filter(e -> e.level().equals("INFO")
                && e.logger().equals("org.apache.kafka.clients.producer.internals.TransactionManager")).count());
        assertTrue(evidence.diagnostics().isEmpty());
        var event = evidence.events().getFirst();
        assertEquals("2026-10-01 19:43:38,488", event.timestamp());
        assertEquals(Optional.of("12"), event.producerId());
        assertEquals(Optional.of("0"), event.epoch());
        assertEquals(Optional.of("ha-token-delay-1-3"), event.transactionalId());
        assertEquals(List.of("producer-fenced"), event.kinds());
        assertTrue(evidence.events().stream().anyMatch(e -> e.producerId().isEmpty()), "Never invent missing producer IDs");
    }

    @Test void groupsKindsAtOneKeyAndIgnoresStackTraceAndUnrelatedLogger() throws Exception {
        String header = "2026-10-01 19:43:38,488 WARN org.apache.kafka.clients.producer.internals.TransactionManager [] - ";
        var evidence = ComponentErrorEvidence.collect(List.of(log("tm#1", header
                + "transactionalId='tx', producerId=12, epoch=2 InvalidPidMappingException InvalidTxnStateException transaction aborted and expired\n"
                + "org.apache.kafka.common.errors.ProducerFencedException: a stack trace\n"
                + header.replace("org.apache.kafka", "org.example") + "transaction expired\n")));
        assertEquals(1, evidence.events().size());
        assertEquals(List.of("invalid-pid-mapping", "invalid-txn-state", "transaction-aborted", "transaction-expired"), evidence.events().getFirst().kinds());
    }

    @Test void identicalTransactionKeysPreserveSeverityAndLoggerWithoutCountingCopies() throws Exception {
        String line = "2026-10-01 19:43:38,488 ERROR org.apache.flink.connector.kafka.sink.internal.KafkaCommitter [] - "
                + "ProducerFencedException producerId=12 epoch=0 transactionalId=tx\n";
        String info = line.replace("ERROR", "INFO").replace("org.apache.flink.connector.kafka.sink.internal.KafkaCommitter",
                "org.apache.kafka.clients.producer.internals.TransactionManager");
        var evidence = ComponentErrorEvidence.collect(List.of(log("tm#1", info + line + info + line)));
        assertEquals(2, evidence.events().size());
        assertEquals(List.of("INFO", "ERROR"), evidence.events().stream().map(ComponentErrorEvidence.Event::level).toList());
    }

    @Test void retainsRealCommitterRetryFailureAndInterruptionHeadersWithoutGuessingExceptionTypes() throws Exception {
        String header = "2026-10-02 15:00:00,001 WARN org.apache.flink.connector.kafka.sink.internal.KafkaCommitter [] - ";
        String retry = "Encountered retriable exception while committing sink-0-2.";
        var evidence = ComponentErrorEvidence.collect(List.of(log("tm#1", header + retry + "\n"
                + "org.apache.kafka.common.errors.NotCoordinatorException: stack trace\n"
                + header.replace("WARN", "ERROR") + "Unable to commit transaction (request) unknown producer.\n"
                + header.replace("WARN", "INFO") + "Committing transaction (request) was interrupted.\n"
                + header.replace("sink.internal.KafkaCommitter", "sink.KafkaCommitter") + retry + "\n")));
        assertEquals(3, evidence.events().size());
        var retried = evidence.events().getFirst();
        assertEquals(List.of("commit-retriable"), retried.kinds());
        assertEquals(Optional.of("sink-0-2"), retried.transactionalId());
        assertEquals(retry, retried.message()); assertEquals("WARN", retried.level());
        assertEquals(List.of("commit-failed"), evidence.events().get(1).kinds());
        assertEquals(List.of("commit-interrupted"), evidence.events().get(2).kinds());
    }

    @Test void capsUniqueEventsAndReportsIncompleteSourcesWithoutInventingEvents() throws Exception {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < 600; i++) text.append("2026-10-01 19:43:38,488 ERROR org.apache.kafka.Foo [] - ProducerFencedException producerId=").append(i).append('\n');
        text.append("x".repeat(9000)).append('\n');
        text.append("2026-10-01 19:43:38,999 ERROR org.apache.kafka.Foo [] - ProducerFencedException");
        var evidence = ComponentErrorEvidence.collect(List.of(log("tm#1", text.toString()),
                new FlinkComponentLog("tm#2", directory.resolve("absent"), true, Optional.of("capture failed"))));
        assertEquals(512, evidence.events().size());
        assertTrue(evidence.diagnostics().stream().anyMatch(x -> x.contains("retention limit")));
        assertTrue(evidence.diagnostics().stream().anyMatch(x -> x.contains("unfinished")));
        assertTrue(evidence.diagnostics().stream().anyMatch(x -> x.contains("unavailable")));
    }
}
