package org.savonitar.flink.stability.core.execution;

import org.savonitar.flink.stability.core.flink.FlinkScenarioControl;
import org.savonitar.flink.stability.runtime.api.RawEvidenceRef;
import org.savonitar.flink.stability.runtime.api.RawEvidenceStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Synthetic response bodies, with the real store's pending/commit/discard semantics. */
final class RecordingRawEvidenceCaptures {
    final RawEvidenceStore store;
    final List<Call> calls = new ArrayList<>();
    final List<Scope> scopes = new ArrayList<>();
    private Scope active;
    private List<RawEvidenceRef> latest = List.of();

    RecordingRawEvidenceCaptures(Path directory) throws IOException {
        store = new RawEvidenceStore(directory);
    }

    FlinkScenarioControl.RawEvidenceCapture open() throws IOException {
        assertNull(active, "Orchestration must close each capture before opening another");
        active = new Scope(store.batch());
        scopes.add(active);
        return active;
    }

    List<RawEvidenceRef> record(String operation, String context) throws IOException {
        int number = calls.size() + 1;
        String body = "{\"call\":" + number + ",\"operation\":\"" + operation + "\",\"context\":\"" + context + "\"}";
        latest = active == null ? List.of() : List.of(active.batch.retain(body.getBytes(StandardCharsets.UTF_8),
                "synthetic-response", "call-" + number, "GET", "/" + operation, 200, number, number));
        var call = new Call(operation, context, body, latest, active);
        calls.add(call);
        if (active != null) active.calls.add(call);
        return latest;
    }

    List<RawEvidenceRef> latest() { return latest; }

    void assertClosed() {
        assertNull(active, "The last capture must close on success and failure");
        assertTrue(scopes.stream().allMatch(scope -> scope.closed), "Every capture must close");
    }

    void assertRetained(Call call) throws IOException {
        assertNotNull(call.scope(), "Selected operation has no capture: " + call.operation());
        assertTrue(call.scope().retained, "Selected capture was never retained: " + call.operation());
        assertEquals(1, call.references().size());
        var reference = call.references().getFirst();
        assertTrue(store.since(0).contains(reference), "Selected reference was never committed");
        assertEquals(call.body(), Files.readString(Path.of(reference.path())));
    }

    void assertDiscarded(Call call) {
        assertTrue(call.scope() == null || !call.scope().retained, "Polling candidate was retained");
        // A later selected batch may reuse a discarded candidate's planned path/sequence.
        assertTrue(call.references().stream().noneMatch(store.since(0)::contains), "Discarded bytes were committed");
    }

    record Call(String operation, String context, String body, List<RawEvidenceRef> references, Scope scope) {}

    final class Scope implements FlinkScenarioControl.RawEvidenceCapture {
        final RawEvidenceStore.Batch batch;
        final List<Call> calls = new ArrayList<>();
        boolean retained, closed;

        Scope(RawEvidenceStore.Batch batch) { this.batch = batch; }

        @Override public void retain() throws IOException {
            assertFalse(closed, "Cannot retain after close");
            batch.commit();
            retained = true;
        }

        @Override public void close() {
            assertFalse(closed, "Capture closed twice");
            batch.close();
            closed = true;
            active = null;
        }
    }
}
