package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.runtime.api.*;
import java.nio.file.Path;
import java.util.*;
import java.math.BigInteger;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.*;

final class PacketFaultPlanCompiler {
    static void validate(Path source, ObjectNode document, List<Diagnostic> issues) {
        BigInteger count = BigInteger.ZERO;
        for (int i = 0; i < document.path("phases").size(); i++)
            count = count.add(steps(source, document.path("phases").get(i).path("steps"),
                    "$/phases/" + i + "/steps", document, issues));
        if (count.compareTo(BigInteger.valueOf(100)) > 0)
            issues.add(issue(source, "runner.phase.packet-fault-count-unsupported", "$/phases", "At most 100 expanded packet faults"));
    }
    private static BigInteger steps(Path source, JsonNode steps, String path, ObjectNode document, List<Diagnostic> issues) {
        BigInteger count = BigInteger.ZERO;
        for (int i = 0; i < steps.size(); i++) {
            var step = steps.get(i); var location = path + "/" + i;
            if (step.has("packet_fault")) {
                count = count.add(BigInteger.ONE);
                try { fault(step.path("packet_fault"), document); }
                catch (IllegalArgumentException failure) {
                    issues.add(issue(source, "runner.phase.packet-fault-invalid", location + "/packet_fault", failure.getMessage()));
                }
            } else if (step.has("loop")) count = count.add(steps(source, step.at("/loop/steps"),
                    location + "/loop/steps", document, issues).multiply(step.at("/loop/times").bigIntegerValue()));
        }
        return count;
    }
    static ExecutableScenarioPlan.PacketFault fault(JsonNode value, ObjectNode document) {
        String tm = value.path("taskmanager").asText();
        int ordinal = Checks.taskManagerOrdinal(tm);
        if (ordinal > document.at("/setup/flink/taskmanagers").asInt())
            throw new IllegalArgumentException("Packet fault must name a declared TaskManager");
        var target = value.path("target");
        boolean all = "all-brokers".equals(target.path("type").asText());
        var first = all ? named(1) : BrokerFaultPlanCompiler.target(target, document);
        var action = PacketFaultControl.Action.valueOf(value.path("mode").asText().toUpperCase(Locale.ROOT));
        return new ExecutableScenarioPlan.PacketFault(new PacketFaultControl.Request(tm, first,
                PacketFaultControl.IMAGE, action, value.path("loss_percent").asInt(), value.path("delay_ms").asInt(),
                value.path("jitter_ms").asInt(), parseDuration(value.path("duration").asText()),
                parseDuration(value.path("timeout").asText()), all ? List.of(named(2), named(3)) : List.of()));
    }
    private static KafkaBrokerControl.Target named(int ordinal) {
        return new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.NAMED, "broker-" + ordinal, null, -1, null);
    }
}
