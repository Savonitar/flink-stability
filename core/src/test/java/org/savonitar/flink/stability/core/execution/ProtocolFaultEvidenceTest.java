package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan.NetworkFaultAction.*;

class ProtocolFaultEvidenceTest {
    @Test void delayAndSyntheticErrorsRequireOnTimeCompleteEffects() {
        var delayed = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, true, "request-delayed", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of(), Map.of(), null, true, 3000, 3_000_000_000L);
        assertTrue(fault(DELAY, List.of(delayed)).triggered());
        assertFalse(fault(DELAY, List.of()).triggered());
        var shortDelay = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, true, "request-delayed", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of(), Map.of(), null, true, 3000, 1L);
        assertFalse(fault(DELAY, List.of(shortDelay)).triggered());
        var late = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, false, "response-substituted", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of(), Map.of(), (short) 7, false, 0, 0);
        assertFalse(fault(ERROR_RESPONSE, List.of(late)).triggered());
        var mixed = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, true, "response-dropped", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of("output/0", (short) 0, "output/1", (short) 7), Map.of(), null, true, 0, 0);
        assertFalse(fault(DROP_RESPONSE, List.of(mixed)).triggered());
    }
    @Test void appendProofMustBeCompleteOnTimeAndSuccessful() throws Exception {
        var json = new ObjectMapper();
        var event = json.readTree("""
                {"event":"response-error-after-append","api":"produce","apiVersion":12,
                 "claim":1,"occurrence":1,"timeMillis":1,"beforeDeadline":true,
                 "transactionalId":"eos-1","producerId":42,"producerEpoch":2,
                 "forwardedToBroker":true,"originalErrorCodes":{"output/0":0},
                 "originalBaseOffsets":{"output/0":123},"substitutedErrorCode":7}
                """);
        assertTrue(ProxyFaultEvidence.qualifiesAfterAppend(event));
        assertTrue(fault(ERROR_AFTER_APPEND, ProxyFaultEvidence.affected(List.of(event), "response-error-after-append")).triggered());
        for (String field : List.of("beforeDeadline", "originalErrorCodes", "originalBaseOffsets", "forwardedToBroker", "producerId", "substitutedErrorCode")) {
            var missing = event.deepCopy();
            ((com.fasterxml.jackson.databind.node.ObjectNode) missing).remove(field);
            assertFalse(ProxyFaultEvidence.qualifiesAfterAppend(missing), field);
        }
        var mixed = event.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) mixed.get("originalErrorCodes")).put("output/0", 7);
        assertFalse(ProxyFaultEvidence.qualifiesAfterAppend(mixed));
        var invalid = event.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) invalid.get("originalBaseOffsets")).put("output/0", -1);
        assertFalse(ProxyFaultEvidence.qualifiesAfterAppend(invalid));
    }
    @Test void uncorrelatedResponseDoesNotInventAProducerIdentity() throws Exception {
        var line = new ObjectMapper().readTree("{\"event\":\"response-dropped\",\"api\":\"produce\",\"claim\":1}");
        assertThrows(IllegalArgumentException.class, () -> ProxyFaultEvidence.affected(List.of(line), "response-dropped"));
    }
    @Test void recoveryEvidenceRequiresActualWireSelectorsAndKeepsMissingIdNull() throws Exception {
        var json = new ObjectMapper();
        for (String api : List.of("describe-producers", "list-transactions")) {
            var event = json.createObjectNode().put("event", "request-dropped").put("api", api)
                    .put("beforeDeadline", true).put("claim", 1).put("occurrence", 1);
            assertFalse(ProxyFaultEvidence.affected(List.of(event), "request-dropped").getFirst().qualifies(DROP_REQUEST));
            if (api.equals("describe-producers")) event.putObject("topicPartitions").putArray("output").add(0);
            else { event.putArray("producerIdFilters").add(42); event.putArray("stateFilters").add("Ongoing"); }
            var message = ProxyFaultEvidence.affected(List.of(event), "request-dropped").getFirst();
            assertNull(message.transactionalId()); assertTrue(message.qualifies(DROP_REQUEST));
        }
    }
    private PhaseExecutionEvidence.NetworkFault fault(org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan.NetworkFaultAction action, List<PhaseExecutionEvidence.ProtocolMessage> messages) {
        return new PhaseExecutionEvidence.NetworkFault("step", "fault", "proxy", "image", action, 1, Duration.ofSeconds(10), 0, 10, List.of(), List.of(), "produce", messages);
    }
}
