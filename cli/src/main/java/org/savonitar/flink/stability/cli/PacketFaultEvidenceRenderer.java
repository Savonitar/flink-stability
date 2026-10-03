package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import java.util.List;

/** Keep raw packet identity, command and counter evidence without requiring a Java-time serializer. */
final class PacketFaultEvidenceRenderer {
    static void render(ArrayNode target, List<PhaseExecutionEvidence.PacketFault> faults) {
        var json = new ObjectMapper();
        for (var fault : faults) {
            var node = target.addObject().put("path", fault.path()).put("confirmed", fault.confirmed());
            node.set("loopIterations", json.valueToTree(fault.loopIterations()));
            V1ExecutionResultRenderer.putJobObservation(node, "jobBefore", fault.jobBefore());
            V1ExecutionResultRenderer.putJobObservation(node, "jobAfter", fault.jobAfter());
            var raw = fault.raw(); var request = raw.request();
            var requested = node.putObject("request").put("taskmanager", request.taskManager())
                    .put("image", request.image()).put("mode", request.action().name())
                    .put("lossPercent", request.lossPercent()).put("delayMillis", request.delayMillis())
                    .put("jitterMillis", request.jitterMillis()).put("duration", request.duration().toString())
                    .put("timeout", request.timeout().toString());
            requested.set("broker", json.valueToTree(request.broker()));
            requested.set("additionalBrokers", json.valueToTree(request.additionalBrokers()));
            node.set("binding", json.valueToTree(raw.binding()));
            node.set("additionalBindings", json.valueToTree(raw.additionalBindings()));
            node.put("sidecarId", raw.sidecarId()).put("sidecarImageId", raw.sidecarImageId()).put("device", raw.device())
                    .put("startedAtMillis", raw.startedAtMillis()).put("completedAtMillis", raw.completedAtMillis())
                    .put("heldNanos", raw.heldNanos()).put("healed", raw.healed()).put("sidecarRemoved", raw.sidecarRemoved())
                    .put("error", raw.error());
            node.set("before", json.valueToTree(raw.before())); node.set("after", json.valueToTree(raw.after()));
            node.set("receipts", json.valueToTree(raw.receipts()));
        }
    }
}
