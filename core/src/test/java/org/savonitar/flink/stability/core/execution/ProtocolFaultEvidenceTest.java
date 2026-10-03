package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan.NetworkFaultAction.*;

class ProtocolFaultEvidenceTest {
    @Test void delayAndSyntheticErrorsRequireOnTimeCompleteEffects() {
        var delayed = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, true, "request-delayed", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of(), null, true, 3000, 3_000_000_000L);
        assertTrue(fault(DELAY, List.of(delayed)).triggered());
        assertFalse(fault(DELAY, List.of()).triggered());
        var shortDelay = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, true, "request-delayed", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of(), null, true, 3000, 1L);
        assertFalse(fault(DELAY, List.of(shortDelay)).triggered());
        var late = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, false, "response-substituted", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of(), (short) 7, false, 0, 0);
        assertFalse(fault(ERROR_RESPONSE, List.of(late)).triggered());
        var mixed = new PhaseExecutionEvidence.ProtocolMessage(1, 1, 0, true, "response-dropped", "produce", (short) 12, 1, "eos-1", 42L, (short) 1, null, Map.of("output/0", (short) 0, "output/1", (short) 7), null, true, 0, 0);
        assertFalse(fault(DROP_RESPONSE, List.of(mixed)).triggered());
    }
    @Test void uncorrelatedResponseDoesNotInventAProducerIdentity() throws Exception {
        var line = new ObjectMapper().readTree("{\"event\":\"response-dropped\",\"api\":\"produce\",\"claim\":1}");
        assertThrows(IllegalArgumentException.class, () -> ProxyFaultEvidence.affected(List.of(line), "response-dropped"));
    }
    private PhaseExecutionEvidence.NetworkFault fault(org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan.NetworkFaultAction action, List<PhaseExecutionEvidence.ProtocolMessage> messages) {
        return new PhaseExecutionEvidence.NetworkFault("step", "fault", "proxy", "image", action, 1, Duration.ofSeconds(10), 0, 10, List.of(), List.of(), "produce", messages);
    }
}
