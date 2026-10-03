package org.savonitar.flink.stability.cli;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class PacketFaultEvidenceRendererTest {
    @Test void keepsUnconfirmedRawEvidenceAndSerializesDurations() {
        var request=new PacketFaultControl.Request("taskmanager-1",new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.NAMED,"broker-1",null,-1,null),
                PacketFaultControl.IMAGE,PacketFaultControl.Action.LOSS,25,0,0,Duration.ofSeconds(15),Duration.ofMinutes(2));
        var node=new ObjectMapper().createArrayNode();
        PacketFaultEvidenceRenderer.render(node,List.of(new PhaseExecutionEvidence.PacketFault("step",List.of(),
                PacketFaultControl.unconfirmed(request,"cleanup uncertain"),null,null)));
        assertFalse(node.get(0).path("confirmed").asBoolean());
        assertEquals("PT15S",node.get(0).at("/request/duration").asText());
        assertEquals("cleanup uncertain",node.get(0).path("error").asText());
        assertTrue(node.get(0).has("receipts"));
    }
}
