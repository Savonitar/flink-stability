package org.savonitar.flink.stability.core.campaign;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/** Pure seeded scheduling. No provisioning, clock, environment, artifact IO or network. */
public final class CampaignGenerator {
    public static final String VERSION = "fault-schedule-v1";
    private final SpecificationLoader loader = new SpecificationLoader();
    private final ScenarioPlanResolver resolver = new ScenarioPlanResolver();

    public ObjectNode recipe(ResolvedScenarioPlan base, ObjectNode constraints, long seed) {
        if (base.scenario().isExperiment()) throw new IllegalArgumentException("Campaign v1 requires a single resolved scenario");
        ObjectNode result = CampaignDocuments.object().put("generator_version", VERSION).put("seed", seed);
        result.set("base", base.scenario().side(ScenarioSide.SINGLE).document());
        ObjectNode expected = base.selectedExpectation().specification().document();
        expected.remove("cases"); expected.set("default", base.selectedExpectation().expectation());
        result.set("expected", expected);
        result.set("constraints", constraints.deepCopy()); result.putArray("edits");
        return result;
    }

    public Generated generate(ObjectNode recipe) {
        CampaignDocuments.fields(recipe, Set.of("generator_version", "seed", "base", "expected", "constraints", "edits"), Set.of());
        if (!VERSION.equals(recipe.path("generator_version").asText())) throw new IllegalArgumentException("Unsupported campaign generator version");
        if (!recipe.path("seed").isIntegralNumber() || !recipe.path("seed").canConvertToLong()) throw new IllegalArgumentException("Seed must be a signed 64-bit integer");
        for (String key : List.of("base", "expected", "constraints"))
            if (!(recipe.get(key) instanceof ObjectNode)) throw new IllegalArgumentException("Expected object: " + key);
        if (!(recipe.get("edits") instanceof ArrayNode edits) || edits.size() > 128) throw new IllegalArgumentException("Expected at most 128 shrink edits");
        CampaignConstraints constraints = CampaignConstraints.parse((ObjectNode) recipe.get("constraints"));
        ObjectNode base = ((ObjectNode) recipe.get("base")).deepCopy();
        if (base.has("parameters") || base.has("experiment")) throw new IllegalArgumentException("Campaign base must be fully materialized");
        int phase = phaseIndex(base, constraints.phase());
        long seed = recipe.path("seed").longValue();
        // java.util.Random's specified 48-bit algorithm is part of VERSION, as is this draw order.
        Random random = new Random(seed);
        int count = constraints.minFaults() + random.nextInt(constraints.maxFaults() - constraints.minFaults() + 1);
        List<ArrayNode> units = new ArrayList<>();
        List<Integer> gaps = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            units.add(constraints.faults().get(random.nextInt(constraints.faults().size())).deepCopy());
            gaps.add(constraints.minGapMs() + random.nextInt(constraints.maxGapMs() - constraints.minGapMs() + 1));
        }
        // Validate every offered template, including templates not selected by this seed.
        for (ArrayNode unit : constraints.faults()) materialize(recipe, phase, List.of(unit), List.of(0));
        for (JsonNode edit : edits) {
            if (!(edit instanceof ObjectNode object)) throw new IllegalArgumentException("Expected shrink edit object");
            CampaignDocuments.fields(object, Set.of("operation", "index"), Set.of());
            JsonNode index = object.path("index");
            if (!index.isIntegralNumber() || !index.canConvertToInt() || index.intValue() < 0 || index.intValue() >= units.size())
                throw new IllegalArgumentException("Shrink index outside current schedule");
            int i = index.intValue();
            switch (object.path("operation").asText()) {
                case "remove" -> { units.remove(i); gaps.remove(i); }
                case "shorten" -> {
                    ArrayNode shortened = CampaignShrinker.shorten(units.get(i));
                    if (shortened.equals(units.get(i))) throw new IllegalArgumentException("Fault cannot be shortened");
                    units.set(i, shortened);
                }
                default -> throw new IllegalArgumentException("Unknown shrink operation");
            }
        }
        return materialize(recipe, phase, units, gaps);
    }

    private Generated materialize(ObjectNode recipe, int phaseIndex, List<ArrayNode> units, List<Integer> gaps) {
        ObjectNode scenario = ((ObjectNode) recipe.get("base")).deepCopy();
        String name = "campaign-" + Long.toUnsignedString(recipe.path("seed").longValue()) + "-" + CampaignDocuments.digest(recipe).substring(0, 16);
        ObjectNode meta = (ObjectNode) scenario.get("meta"); meta.put("name", name);
        meta.putObject("campaign").put("seed", recipe.path("seed").longValue())
                .put("generator_version", VERSION).put("recipe_sha256", CampaignDocuments.digest(recipe));
        ArrayNode phases = (ArrayNode) scenario.get("phases");
        if (units.isEmpty()) {
            phases.remove(phaseIndex);
            if (phases.isEmpty()) scenario.remove("phases");
        } else {
            ArrayNode steps = ((ObjectNode) phases.get(phaseIndex)).putArray("steps");
            for (int i = 0; i < units.size(); i++) {
                if (gaps.get(i) > 0) steps.addObject().putObject("wait").put("duration", gaps.get(i) + "ms");
                units.get(i).forEach(step -> steps.add(step.deepCopy()));
            }
        }
        ObjectNode expected = ((ObjectNode) recipe.get("expected")).deepCopy();
        ((ObjectNode) expected.get("meta")).put("name", name + ".expected").put("scenario", name);
        Path source = Path.of(name + ".yaml");
        var bundle = new ScenarioBundle(loader.validateResolvedScenario(source, scenario),
                loader.validateExpectedResultDocument(Path.of(name + ".expected.yaml"), expected));
        ResolvedScenarioPlan resolved = resolver.resolve(bundle, new ResolutionRequest(Map.of(), Map.of()));
        new ExecutableScenarioPlanCompiler().compile(resolved);
        return new Generated(name, CampaignDocuments.yaml(scenario), CampaignDocuments.yaml(expected), resolved, units.size());
    }

    private static int phaseIndex(ObjectNode base, String phase) {
        JsonNode phases = base.path("phases"); int found = -1;
        for (int i = 0; i < phases.size(); i++) if (phase.equals(phases.get(i).path("name").asText())) {
            if (found != -1) throw new IllegalArgumentException("Ambiguous replacement phase"); found = i;
        }
        if (found == -1) throw new IllegalArgumentException("Replacement phase not found: " + phase);
        return found;
    }

    public record Generated(String name, byte[] scenarioYaml, byte[] expectedYaml, ResolvedScenarioPlan resolved, int faultCount) {
        public ObjectNode manifest(ObjectNode recipe) {
            ObjectNode result = CampaignDocuments.object(); result.set("recipe", recipe.deepCopy());
            result.put("recipe_sha256", CampaignDocuments.digest(recipe));
            result.put("scenario_sha256", Digests.sha256(scenarioYaml)); result.put("expected_sha256", Digests.sha256(expectedYaml));
            return result;
        }
    }
}
