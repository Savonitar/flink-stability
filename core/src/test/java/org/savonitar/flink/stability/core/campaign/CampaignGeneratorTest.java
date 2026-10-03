package org.savonitar.flink.stability.core.campaign;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ResolvedScenarioPlan;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioSide;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CampaignGeneratorTest {
    private final CampaignGenerator generator = new CampaignGenerator();

    @Test void generatesEveryExistingFaultFamilyWithoutChangingWorkloadOrExpectations() {
        for (String name : List.of("distributed-eos", "brokers/broker-eos-kill", "brokers/broker-leader-kill",
                "brokers/broker-leader-pause", "brokers/broker-coordinator-pause", "ha/ha-eos-kill", "ha/ha-eos-pause",
                "ha/ha-eos-isolate", "ha/ha-token-delay", "ha/ha-token-failure", "ha/ha-token-linkage",
                "ha/ha-token-submitted-delay", "ha/ha-token-submitted-failure", "commit-request-lost", "commit-response-lost")) {
            var base = base(name);
            ObjectNode recipe = generator.recipe(base, constraints(base, 1), 123);
            var generated = generator.generate(recipe);
            var document = generated.resolved().scenario().side(ScenarioSide.SINGLE).document();
            var original = base.scenario().side(ScenarioSide.SINGLE).document();
            for (String key : List.of("setup", "workload", "subject", "terminal_validations", "runs", "health_retry_limit"))
                assertEquals(original.get(key), document.get(key), name + " " + key);
            assertEquals(original.at("/phases/0"), document.at("/phases/0"), name);
            assertEquals(base.selectedExpectation().expectation(), generated.resolved().selectedExpectation().expectation(), name);
            assertEquals(123, document.at("/meta/campaign/seed").longValue());
            assertEquals(CampaignGenerator.VERSION, document.at("/meta/campaign/generator_version").asText());
        }
    }

    @Test void sameInputsReplayByteExactlyAndSeedChangesSchedule() throws Exception {
        var base = base("brokers/broker-leader-pause");
        ObjectNode limits = constraints(base, 3).put("min_gap_ms", 1).put("max_gap_ms", 120_000);
        ObjectNode recipe = generator.recipe(base, limits, Long.MIN_VALUE);
        var first = generator.generate(recipe);
        var steps = first.resolved().scenario().side(ScenarioSide.SINGLE).document().at("/phases/1/steps");
        assertEquals("68030ms", steps.at("/0/wait/duration").asText());
        assertEquals("3516ms", steps.at("/2/wait/duration").asText());
        assertEquals("54492ms", steps.at("/4/wait/duration").asText());
        var replay = generator.generate(CampaignDocuments.read(CampaignDocuments.json(recipe)));
        assertArrayEquals(first.scenarioYaml(), replay.scenarioYaml());
        assertArrayEquals(first.expectedYaml(), replay.expectedYaml());
        var different = generator.generate(generator.recipe(base, limits, Long.MAX_VALUE));
        assertNotEquals(first.resolved().scenario().side(ScenarioSide.SINGLE).document().get("phases"),
                different.resolved().scenario().side(ScenarioSide.SINGLE).document().get("phases"));
        assertEquals(recipe, CampaignDocuments.read(CampaignDocuments.json(recipe)));
    }

    @Test void shrinkPreservesPairedHealingAndProducesStrictlySmallerSchedules() {
        var base = base("distributed-eos");
        ObjectNode recipe = generator.recipe(base, constraints(base, 2), 42);
        byte[] untouched = CampaignDocuments.json(recipe);
        var shrunk = new CampaignShrinker().shrink(recipe);
        assertEquals(4, shrunk.candidates().size()); assertTrue(shrunk.rejected().isEmpty());
        for (var candidate : shrunk.candidates()) {
            assertTrue(candidate.generated().faultCount() <= 2);
            assertArrayEquals(candidate.generated().scenarioYaml(), generator.generate(candidate.recipe()).scenarioYaml());
            assertEquals(base.selectedExpectation().expectation(), candidate.generated().resolved().selectedExpectation().expectation());
            var phases = candidate.generated().resolved().scenario().side(ScenarioSide.SINGLE).document().path("phases");
            var steps = phases.get(1).path("steps");
            int kills = 0, restarts = 0;
            for (var step : steps) { if (step.has("kill")) kills++; if (step.has("restart")) restarts++; }
            assertEquals(kills, restarts);
        }
        assertArrayEquals(untouched, CampaignDocuments.json(recipe));
        var single = generator.recipe(base, constraints(base, 1), 2);
        var empty = new CampaignShrinker().shrink(single).candidates().getFirst();
        assertEquals(0, empty.generated().faultCount());
        assertEquals(1, empty.generated().resolved().scenario().side(ScenarioSide.SINGLE).document().path("phases").size());
    }

    @Test void endTxnLossCanBeRemovedButDoesNotPretendToShortenAnOccurrenceFault() {
        var base = base("commit-response-lost");
        var result = new CampaignShrinker().shrink(generator.recipe(base, constraints(base, 1), 42));
        assertEquals(1, result.candidates().size()); assertEquals(1, result.rejected().size());
        assertEquals("shorten", result.rejected().getFirst().operation());
        assertEquals(0, result.candidates().getFirst().generated().faultCount());
    }

    @Test void rejectsUnknownConstraintsVersionsBoundsAndUnhealedOrIncompatibleTemplates() {
        var base = base("brokers/broker-leader-pause"); var limits = constraints(base, 1);
        for (String field : List.of("min_faults", "max_faults", "min_gap_ms", "max_gap_ms")) {
            ObjectNode invalid = limits.deepCopy().put(field, 999999);
            assertThrows(IllegalArgumentException.class, () -> generator.generate(generator.recipe(base, invalid, 1)));
        }
        assertThrows(IllegalArgumentException.class, () -> generator.generate(generator.recipe(base, limits.deepCopy().put("typo", true), 1)));
        assertThrows(IllegalArgumentException.class, () -> generator.generate(generator.recipe(base, limits, 1).put("generator_version", "future")));
        assertThrows(IllegalArgumentException.class, () -> generator.generate(generator.recipe(base, limits.deepCopy().put("phase", "absent"), 1)));
        ObjectNode invalid = limits.deepCopy();
        ((ArrayNode) invalid.get("faults")).addArray().addObject().putObject("kill");
        assertThrows(IllegalArgumentException.class, () -> generator.generate(generator.recipe(base, invalid, 1)));
        ObjectNode incompatible = constraints(base("ha/ha-eos-kill"), 1).put("phase", "broker-recovery");
        assertThrows(IllegalArgumentException.class, () -> generator.generate(generator.recipe(base, incompatible, 1)));
    }

    @Test void parserRejectsDuplicateKeysAndTrailingDocuments() {
        assertThrows(Exception.class, () -> CampaignDocuments.read("{\"seed\":1,\"seed\":2}".getBytes()));
        assertThrows(Exception.class, () -> CampaignDocuments.read("{} {}".getBytes()));
    }

    private static ResolvedScenarioPlan base(String name) {
        var loader = new SpecificationLoader();
        Path root = Path.of("../scenarios");
        return new ScenarioPlanResolver().resolve(new ScenarioBundle(loader.loadScenario(root.resolve(name + ".yaml")),
                loader.loadExpectedResult(root.resolve(name + ".expected.yaml"))), new ResolutionRequest(Map.of(), name.contains("submitted") ? Map.of(
                    "flink_image", com.fasterxml.jackson.databind.node.TextNode.valueOf("local/flink:2.4-SNAPSHOT"),
                    "flink_image_id", com.fasterxml.jackson.databind.node.TextNode.valueOf("sha256:" + "a".repeat(64)),
                    "runtime_jar_sha256", com.fasterxml.jackson.databind.node.TextNode.valueOf("b".repeat(64)),
                    "connector_jar", com.fasterxml.jackson.databind.node.TextNode.valueOf("connector.jar"),
                    "connector_sha256", com.fasterxml.jackson.databind.node.TextNode.valueOf("c".repeat(64)),
                    "workload_jar", com.fasterxml.jackson.databind.node.TextNode.valueOf("workload.jar")) : Map.of()));
    }

    private static ObjectNode constraints(ResolvedScenarioPlan base, int count) {
        var phase = base.scenario().side(ScenarioSide.SINGLE).document().path("phases").get(1);
        ObjectNode result = CampaignDocuments.object().put("phase", phase.path("name").asText())
                .put("min_faults", count).put("max_faults", count).put("min_gap_ms", 0).put("max_gap_ms", 0);
        result.putArray("faults").add(phase.get("steps").deepCopy()); return result;
    }
}
