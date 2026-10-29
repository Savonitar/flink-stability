package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.RawEvidenceRef;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;

class RawEvidenceRendererTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static RawEvidenceRef raw(String kind) {
        return new RawEvidenceRef("/generated/attempt/raw-storage/000001.json", "a".repeat(64), 42,
                kind, 1, 100, 200, "call-1", "GET", "/owned", 200);
    }

    @Test void physicalFenceRendererRetainsSourceReferenceAndLegacyMissingOptionals() {
        var state = new FlinkHaControl.ProcessState("owned", false, false, Optional.of(137L),
                Optional.of(false), Optional.of("finished"), Optional.of(raw("docker-inspect-id-state-source-projection")));
        var result = JSON.createObjectNode(); FlinkHaEvidenceRenderer.state(result, state);
        assertEquals(137, result.path("exitCode").longValue());
        assertEquals("docker-inspect-id-state-source-projection", result.at("/rawEvidence/kind").textValue());
        assertEquals(42, result.at("/rawEvidence/bytes").longValue());
        assertEquals("a".repeat(64), result.at("/rawEvidence/sha256").textValue());
        var legacy = JSON.createObjectNode();
        FlinkHaEvidenceRenderer.state(legacy, new FlinkHaControl.ProcessState("owned", true, false));
        assertFalse(legacy.has("rawEvidence")); assertFalse(legacy.has("exitCode"));
    }

    @Test void jobObservationRecordRendererKeepsGroupAndTimingWithoutStringifyingTheRecord() {
        var job = new FlinkJobObservation(12, FlinkJobState.FINISHED, 3, 0, Optional.empty(),
                List.of(), List.of(), List.of(raw("flink-rest-decoded-body")));
        var result = StateRecoveryEvidenceRenderer.render(job, JSON);
        assertEquals("FINISHED", result.path("state").textValue());
        assertTrue(result.path("rawEvidence").isArray());
        assertEquals(100, result.at("/rawEvidence/0/startedNanos").longValue());
        assertEquals(200, result.at("/rawEvidence/0/completedNanos").longValue());
        assertEquals("call-1", result.at("/rawEvidence/0/group").textValue());
    }
}
