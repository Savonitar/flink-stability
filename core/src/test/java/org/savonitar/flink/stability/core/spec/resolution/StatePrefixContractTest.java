package org.savonitar.flink.stability.core.spec.resolution;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatePrefixContractTest {
    private final SpecificationLoader loader = new SpecificationLoader();

    @Test
    void resolvesIndependentFixtureWithExplicitBehaviorAndNoOracleMode() {
        var document = resolve(node -> {}, node -> {}).scenario().side(ScenarioSide.SINGLE).document();
        assertEquals("keyed-prefix-v1", document.at("/workload/jobs/0/behavior/type").textValue());
        assertEquals(2, document.at("/workload/jobs/0/behavior/key_count").intValue());
        assertEquals("2m", document.at("/terminal_validations/0/timeout").textValue());
        assertTrue(document.at("/terminal_validations/0/mode").isMissingNode());

        var legacy = loader.loadScenario(resource("minimal.yaml"));
        var legacyResolved = new ScenarioParameterResolver(loader).resolve(legacy, ResolutionRequest.none());
        assertFalse(legacyResolved.side(ScenarioSide.SINGLE).document()
                .at("/workload/jobs/0").has("behavior"));
    }

    @Test
    void acceptsBothKeyCountBoundsAndWholeScalarParameter() {
        for (int count : List.of(1, 1024)) {
            resolve(document -> behavior(document).put("key_count", count), node -> {});
        }
        resolve(document -> {
            document.putObject("parameters").putObject("keys")
                    .put("type", "integer").put("default", 8);
            behavior(document).put("key_count", "${keys}");
        }, node -> {});
    }

    @Test
    void rejectsOutOfRangeMissingUnknownOrWronglyTypedBehaviorFields() {
        List<Consumer<ObjectNode>> changes = List.of(
                document -> behavior(document).put("key_count", 0),
                document -> behavior(document).put("key_count", 1025),
                document -> behavior(document).put("key_count", 1.5),
                document -> behavior(document).put("key_count", "8"),
                document -> behavior(document).remove("key_count"),
                document -> behavior(document).put("type", "passthrough"),
                document -> behavior(document).put("unexpected", true),
                document -> terminal(document).put("mode", "exactly-once"),
                document -> terminal(document).put("mode", "at-least-once"),
                document -> terminal(document).remove("job"));
        for (var change : changes) {
            var failure = assertThrows(SpecificationException.class, () -> resolve(change, node -> {}));
            assertEquals(SpecificationException.Stage.DOCUMENT, failure.stage());
        }
    }

    @Test
    void rejectsResolvedKeyCountOutsideBounds() {
        var failure = assertThrows(SpecificationException.class, () -> resolve(document -> {
            document.putObject("parameters").putObject("keys")
                    .put("type", "integer").put("default", 1025);
            behavior(document).put("key_count", "${keys}");
        }, node -> {}));
        assertTrue(failure.diagnostics().stream().anyMatch(issue -> issue.path().contains("key_count")));
    }

    @Test
    void requiresBothDirectionsOfBehaviorOracleBinding() {
        rejects("preflight.validator.behavior-mismatch", document ->
                ((ObjectNode) document.at("/workload/jobs/0")).remove("behavior"));
        rejects("preflight.workload.state-validator-required", document -> {
            terminal(document).put("type", "kafka.id-set");
            terminal(document).remove("job");
        });
        rejects("preflight.reference.job-not-found", document -> terminal(document).put("job", "missing"));
        rejects("preflight.validator.job-sink-mismatch", document -> terminal(document).put("topic", "input"));
        rejects("preflight.input.manifest-not-available", document ->
                ((ObjectNode) document.at("/setup/kafka/clusters/main/topics/0")).remove("input_source"));
        rejects("preflight.input.validator-source-mismatch", document ->
                ((ObjectNode) document.at("/workload/jobs/0/source")).put("topic", "output"));
        rejects("preflight.input.validator-source-mismatch", document -> {
            var extra = ((ObjectNode) document.at("/workload/jobs/0")).deepCopy();
            extra.put("alias", "another-job");
            extra.remove("behavior");
            document.withObject("workload").withArray("jobs").add(extra);
        });
    }

    @Test
    void registersOnlyCompleteStateOracleDefectReasons() {
        for (String suffix : List.of("malformed-records", "unexpected-ids", "duplicate-ids", "missing-ids", "state-mismatch")) {
            resolve(document -> {}, expected -> expected.withObject("default")
                    .put("outcome", "fail").put("oracle", "kafka.state-prefix")
                    .put("reason", "validator.kafka.state-prefix." + suffix));
        }
        assertThrows(SpecificationException.class, () -> resolve(document -> {}, expected -> expected.withObject("default")
                .put("outcome", "fail").put("oracle", "kafka.state-prefix")
                .put("reason", "validator.kafka.state-prefix.match")));
        assertThrows(SpecificationException.class, () -> resolve(document -> {}, expected -> expected.withObject("default")
                .put("outcome", "fail").put("oracle", "kafka.state-prefix")
                .put("reason", "verification.kafka.incomplete-after-timeout")));
    }

    private void rejects(String code, Consumer<ObjectNode> changes) {
        var failure = assertThrows(SpecificationException.class, () -> resolve(changes, node -> {}));
        assertEquals(SpecificationException.Stage.PREFLIGHT, failure.stage());
        assertTrue(failure.diagnostics().stream().anyMatch(issue -> issue.code().equals(code)),
                () -> failure.diagnostics().toString());
    }

    private ResolvedScenarioPlan resolve(Consumer<ObjectNode> changes, Consumer<ObjectNode> expectedChanges) {
        Path source = resource("keyed-prefix.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        changes.accept(document);
        var scenario = loader.validateScenarioDocument(source, document);
        Path expectedSource = resource("keyed-prefix.expected.yaml");
        ObjectNode expected = loader.loadExpectedResult(expectedSource).document();
        expectedChanges.accept(expected);
        return new ScenarioPlanResolver().resolve(new ScenarioBundle(scenario,
                loader.validateExpectedResultDocument(expectedSource, expected)), ResolutionRequest.none());
    }

    private static ObjectNode behavior(ObjectNode document) {
        return (ObjectNode) document.at("/workload/jobs/0/behavior");
    }

    private static ObjectNode terminal(ObjectNode document) {
        return (ObjectNode) document.at("/terminal_validations/0");
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
