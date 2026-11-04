package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.StorageFaultControl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StorageJournalEvidenceRendererTest {
    private static final StorageFaultControl.Instance INSTANCE = new StorageFaultControl.Instance(
            "taskmanager-1", "taskmanager-1#1", "container-1", "mount-1", FlinkComponentRole.TASK_MANAGER, 123);
    private static final String PATH = "namespace/job/chk-1/state";

    @Test void tenThousandOperationsRemainReferencedWithEveryBoundReceiptInline() throws Exception {
        var operations = new ArrayList<StorageFaultControl.Operation>();
        for (int i = 0; i < 9990; i++) operations.add(operation(operations.size() + 1, "getattr", PATH, 0));
        for (String control : List.of("START", "READY", "ARM", "HEAL", "ABORT"))
            operations.add(operation(operations.size() + 1, control, "", 0));
        operations.add(operation(9996, "create", "namespace/job/chk-2/state", 5));
        operations.add(operation(9997, "write", "namespace/job/chk-2/state", 5));
        operations.add(operation(9998, "write", PATH, 0));
        operations.add(operation(9999, "read", PATH, 0));
        operations.add(operation(10000, "write", "namespace/job/chk-3/unbound", 0));
        var status = new StorageFaultControl.Status(StorageFaultControl.State.HEALTHY, "", 0, 10000, 0, 0, true, false);
        var command = new StorageFaultControl.Command(INSTANCE, "HEAL", 1, 2, 0, status, "ack", "", true);
        var evidence = new StorageFaultControl.InstanceEvidence(INSTANCE, null, status, true, true,
                "checkpoints/attempt/journal.jsonl", "a".repeat(64), 4_000_000, 10000,
                operations, List.of(command), List.of());
        var file = new StorageFaultControl.FileEvidence("/flink/checkpoints/" + PATH, 1, true, "b".repeat(64), 1);
        var snapshot = new StorageFaultControl.Snapshot("attempt", "storage", "namespace", "file:///flink/checkpoints/namespace",
                "c".repeat(64), List.of(evidence), List.of(file), true, true, List.of());
        var mapper = new ObjectMapper();
        var rendered = StateRecoveryEvidenceRenderer.render(snapshot, mapper);
        var instance = rendered.at("/instances/0");
        assertEquals("bound-subset-v1", instance.path("operationsRepresentation").textValue());
        assertEquals(10000, instance.path("operationCount").intValue());
        assertEquals(10000, instance.path("lastSequence").intValue());
        assertEquals(evidence.journalPath(), instance.path("journalPath").textValue());
        assertEquals(evidence.journalSha256(), instance.path("journalSha256").textValue());
        assertEquals(evidence.journalBytes(), instance.path("journalBytes").longValue());
        assertEquals(9, instance.path("operations").size());
        for (int i = 0; i < 9; i++) assertEquals(9991 + i, instance.path("operations").get(i).path("sequence").longValue());
        long count = 0;
        for (var row : instance.path("operationCounts")) count += row.path("count").longValue();
        assertEquals(10000, count);
        assertEquals("ack", instance.at("/commands/0/stdout").textValue());
        assertEquals(10000, evidence.operations().size(), "The validator's in-memory model must remain complete");
        assertTrue(rendered.toString().getBytes(StandardCharsets.UTF_8).length < 32 * 1024);
        assertTrue(StateRecoveryEvidenceRenderer.render(java.util.Collections.nCopies(7, snapshot), mapper)
                .toString().getBytes(StandardCharsets.UTF_8).length < 224 * 1024);
    }

    private static StorageFaultControl.Operation operation(long sequence, String name, String path, int errno) {
        return new StorageFaultControl.Operation(INSTANCE, sequence, sequence, sequence, 0,
                StorageFaultControl.State.HEALTHY, "", name, path, "", errno == 0 ? 1 : -1,
                errno, 1, errno == 0 ? 1 : 0, null, "");
    }
}
