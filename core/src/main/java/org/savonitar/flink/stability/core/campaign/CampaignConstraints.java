package org.savonitar.flink.stability.core.campaign;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

/** An explicit replacement phase and a bounded menu of complete, healed fault units. */
record CampaignConstraints(String phase, int minFaults, int maxFaults, int minGapMs, int maxGapMs,
                           List<ArrayNode> faults) {
    static CampaignConstraints parse(ObjectNode document) {
        CampaignDocuments.fields(document, Set.of("phase", "min_faults", "max_faults", "min_gap_ms", "max_gap_ms", "faults"), Set.of());
        String phase = document.path("phase").asText();
        if (!phase.matches("[a-z][a-z0-9]*(?:-[a-z0-9]+)*")) throw new IllegalArgumentException("Invalid replacement phase");
        int min = integer(document, "min_faults", 1, 32), max = integer(document, "max_faults", min, 32);
        int gapMin = integer(document, "min_gap_ms", 0, 120_000), gapMax = integer(document, "max_gap_ms", gapMin, 120_000);
        JsonNode nodes = document.path("faults");
        if (!nodes.isArray() || nodes.isEmpty() || nodes.size() > 32) throw new IllegalArgumentException("Expected 1..32 fault templates");
        List<ArrayNode> faults = StreamSupport.stream(nodes.spliterator(), false).map(node -> {
            if (!(node instanceof ArrayNode steps)) throw new IllegalArgumentException("A fault template must be a step array");
            validateUnit(steps); return steps.deepCopy();
        }).toList();
        return new CampaignConstraints(phase, min, max, gapMin, gapMax, faults);
    }

    private static void validateUnit(ArrayNode steps) {
        if (steps.size() == 1 && steps.get(0).isObject() && steps.get(0).size() == 1) {
            JsonNode step = steps.get(0);
            if (step.has("leader_fault") || step.has("broker_fault")) return;
            if (step.has("network_fault") && "end-txn".equals(step.at("/network_fault/match/api").asText())
                    && Set.of("drop-request", "drop-response").contains(step.at("/network_fault/fault/type").asText())) return;
        }
        if (steps.size() == 3 && steps.get(0).has("kill") && steps.get(1).has("wait") && steps.get(2).has("restart")
                && steps.get(0).size() == 1 && steps.get(1).size() == 1 && steps.get(2).size() == 1
                && steps.get(0).at("/kill/target/name").isTextual()
                && steps.get(0).at("/kill/target/name").equals(steps.get(2).at("/restart/name"))
                && ("broker".equals(steps.get(0).at("/kill/target/role").asText()) ? "kafka" : "taskmanager")
                    .equals(steps.get(2).at("/restart/component").asText())
                && Set.of("taskmanager", "broker").contains(steps.get(0).at("/kill/target/role").asText())) return;
        throw new IllegalArgumentException("Fault template must heal itself: leader_fault, broker_fault, EndTxn loss, or kill/wait/restart of the same target");
    }

    private static int integer(ObjectNode node, String key, int min, int max) {
        JsonNode value = node.path(key);
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < min || value.intValue() > max)
            throw new IllegalArgumentException(key + " must be in " + min + ".." + max);
        return value.intValue();
    }
}
