package org.savonitar.flink.stability.core.campaign;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** One reduction per candidate; never runs an oracle or claims a minimal reproduction. */
public final class CampaignShrinker {
    private static final Pattern DURATION = Pattern.compile("([1-9][0-9]*)(ms|s|m|h)");
    public record Candidate(ObjectNode recipe, CampaignGenerator.Generated generated) {}
    public record Rejected(int index, String operation, String reason) {}
    public record Result(List<Candidate> candidates, List<Rejected> rejected) {}

    public Result shrink(ObjectNode recipe) {
        CampaignGenerator generator = new CampaignGenerator();
        int count = generator.generate(recipe).faultCount();
        List<Candidate> candidates = new ArrayList<>(); List<Rejected> rejected = new ArrayList<>();
        for (int i = 0; i < count; i++) for (String operation : List.of("remove", "shorten")) {
            ObjectNode edited = recipe.deepCopy();
            ((ArrayNode) edited.get("edits")).addObject().put("operation", operation).put("index", i);
            try { candidates.add(new Candidate(edited, generator.generate(edited))); }
            catch (IllegalArgumentException exception) {
                rejected.add(new Rejected(i, operation, exception.getMessage()));
            }
        }
        return new Result(List.copyOf(candidates), List.copyOf(rejected));
    }

    static ArrayNode shorten(ArrayNode unit) {
        ArrayNode result = unit.deepCopy();
        for (JsonNode step : result) {
            if (step.get("wait") instanceof ObjectNode wait) halve(wait, "duration");
            for (String key : List.of("leader_fault", "broker_fault")) if (step.get(key) instanceof ObjectNode fault) {
                halve(fault, "duration");
                if (fault.get("token_fault") instanceof ObjectNode token) halve(token, "delay");
            }
        }
        return result;
    }

    private static void halve(ObjectNode node, String field) {
        if (!node.has(field)) return;
        var matcher = DURATION.matcher(node.path(field).asText());
        if (!matcher.matches()) throw new IllegalArgumentException("Invalid fault duration");
        long multiplier = switch (matcher.group(2)) { case "s" -> 1000; case "m" -> 60_000; case "h" -> 3_600_000; default -> 1; };
        long millis = Math.multiplyExact(Long.parseLong(matcher.group(1)), multiplier);
        if (millis > 1) node.put(field, Math.max(1, millis / 2) + "ms");
    }
}
