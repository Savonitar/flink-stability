package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkKafkaCompatibilityValidatorTest {
    private static final String CONNECTOR_PATH = "$/subject/connectors/kafka/artifact";

    private final SpecificationLoader loader = new SpecificationLoader();
    private final ScenarioParameterResolver resolver = new ScenarioParameterResolver(loader);

    @Test
    void acceptsRegisteredFlinkAndKafkaConnectorPatchesAndImageVariants() {
        ScenarioSpecification scenario = scenario(document -> {
            flink(document).put(
                    "image",
                    "registry.example:5000/runtime/flink:2.2.9-scala_2.12-java17@sha256:abc");
            connector(document).put(
                    "artifact",
                    "maven:org.apache.flink:flink-connector-kafka:5.0.17-2.2");
        });

        ResolvedSide side = resolver.resolve(scenario, ResolutionRequest.none())
                .side(ScenarioSide.SINGLE);

        assertEquals(
                "registry.example:5000/runtime/flink:2.2.9-scala_2.12-java17@sha256:abc",
                side.document().at("/setup/flink/image").textValue());
        assertEquals(
                "maven:org.apache.flink:flink-connector-kafka:5.0.17-2.2",
                side.document().at("/subject/connectors/kafka/artifact").textValue());
    }

    @Test
    void validatesCompatibilityAfterWholeDocumentParameterInterpolation() {
        ScenarioSpecification scenario = scenario(document -> {
            parameter(document, "flink_patch", "integer", IntNode.valueOf(1));
            parameter(document, "connector_patch", "integer", IntNode.valueOf(3));
            flink(document).put("image", "flink:2.2.${flink_patch}-java17");
            connector(document).put(
                    "artifact",
                    "maven:org.apache.flink:flink-connector-kafka:5.0.${connector_patch}-2.2");
        });

        ResolvedSide side = resolver.resolve(scenario, ResolutionRequest.none())
                .side(ScenarioSide.SINGLE);

        assertEquals("flink:2.2.1-java17",
                side.document().at("/setup/flink/image").textValue());
        assertEquals(
                "maven:org.apache.flink:flink-connector-kafka:5.0.3-2.2",
                side.document().at("/subject/connectors/kafka/artifact").textValue());
    }

    @Test
    void reportsAnIncompatibleExperimentConnectorOnlyAgainstItsSide() {
        String supported = "maven:org.apache.flink:flink-connector-kafka:5.0.0-2.2";
        String incompatible = "maven:org.apache.flink:flink-connector-kafka:3.4.0-1.20";
        ScenarioSpecification scenario = scenario(document -> {
            parameter(document, "connector_artifact", "string", TextNode.valueOf(supported));
            connector(document).put("artifact", "${connector_artifact}");
            experiment(
                    document,
                    "connector_artifact",
                    TextNode.valueOf(supported),
                    TextNode.valueOf(incompatible));
        });

        ScenarioResolutionException exception = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertIssue(
                exception,
                FlinkKafkaCompatibilityValidator.KAFKA_CONNECTOR_LINE_UNSUPPORTED,
                ResolutionScope.CANDIDATE,
                CONNECTOR_PATH);
        assertFalse(exception.issues().stream()
                .anyMatch(issue -> issue.scope() == ResolutionScope.BASELINE));
    }

    @Test
    void treatsALocalPrimaryAsAnAuthorAssertionButStillRequiresFlink22() {
        ScenarioSpecification supported = scenario(document -> useLocalConnector(document));

        assertEquals(
                "./connector.jar",
                resolver.resolve(supported, ResolutionRequest.none())
                        .side(ScenarioSide.SINGLE)
                        .document()
                        .at("/subject/connectors/kafka/artifact")
                        .textValue());

        ScenarioSpecification incompatible = scenario(document -> {
            useLocalConnector(document);
            flink(document).put("image", "flink:1.20.3");
        });
        ScenarioResolutionException exception = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(incompatible, ResolutionRequest.none()));

        assertIssue(
                exception,
                FlinkKafkaCompatibilityValidator.FLINK_LINE_UNSUPPORTED,
                ResolutionScope.SINGLE,
                "$/setup/flink/image");
        assertEquals(1, exception.issues().size());
    }

    @Test
    void rejectsFlinkReferencesWhoseVersionLineCannotBeEstablished() {
        for (String image : new String[] {
                "flink:latest",
                "flink@sha256:abc"
        }) {
            ScenarioSpecification scenario = scenario(document ->
                    flink(document).put("image", image));

            ScenarioResolutionException exception = assertThrows(
                    ScenarioResolutionException.class,
                    () -> resolver.resolve(scenario, ResolutionRequest.none()),
                    image);

            assertIssue(
                    exception,
                    FlinkKafkaCompatibilityValidator.FLINK_LINE_UNSUPPORTED,
                    ResolutionScope.SINGLE,
                    "$/setup/flink/image");
            assertEquals(1, exception.issues().size(), image);
            assertTrue(exception.issues().getFirst().message()
                    .contains("Cannot establish a supported Flink line"));
        }
    }

    @Test
    void rejectsKnownKafkaConnectorLinesWithOneStableDiagnostic() {
        for (String version : new String[] {
                "3.4.0-1.20",
                "5.0.1-2.1",
                "5.1.0-2.2",
                "5.0.0"
        }) {
            ScenarioSpecification scenario = scenario(document -> connector(document).put(
                    "artifact",
                    "maven:org.apache.flink:flink-connector-kafka:" + version));

            ScenarioResolutionException exception = assertThrows(
                    ScenarioResolutionException.class,
                    () -> resolver.resolve(scenario, ResolutionRequest.none()),
                    version);

            assertIssue(
                    exception,
                    FlinkKafkaCompatibilityValidator.KAFKA_CONNECTOR_LINE_UNSUPPORTED,
                    ResolutionScope.SINGLE,
                    CONNECTOR_PATH);
            assertEquals(1, exception.issues().size(), version);
            assertTrue(exception.issues().getFirst().message().contains("5.0.<patch>-2.2"));
        }
    }

    @Test
    void rejectsCanonicalMavenPrimariesOutsideTheRegisteredCoordinate() {
        for (String reference : new String[] {
                "maven:com.example:flink-connector-kafka:5.0.0-2.2",
                "maven:org.apache.flink:renamed-kafka-connector:5.0.0-2.2"
        }) {
            ScenarioSpecification scenario = scenario(document ->
                    connector(document).put("artifact", reference));

            ScenarioResolutionException exception = assertThrows(
                    ScenarioResolutionException.class,
                    () -> resolver.resolve(scenario, ResolutionRequest.none()),
                    reference);

            assertIssue(
                    exception,
                    FlinkKafkaCompatibilityValidator.CONNECTOR_MAVEN_COORDINATE_UNREGISTERED,
                    ResolutionScope.SINGLE,
                    CONNECTOR_PATH);
            assertEquals(1, exception.issues().size(), reference);
        }
    }

    @Test
    void validatesNestedFlinkRestartImagesAlongsideSupportedKafkaRestartImages() {
        ScenarioSpecification scenario = scenario(document -> {
            ArrayNode phases = document.putArray("phases");
            ObjectNode phase = phases.addObject();
            phase.put("name", "upgrade");
            ObjectNode loop = phase.putArray("steps").addObject().putObject("loop");
            loop.put("times", 1);
            ArrayNode nested = loop.putArray("steps");
            ObjectNode kafkaRestart = nested.addObject().putObject("restart");
            kafkaRestart.put("component", "kafka");
            kafkaRestart.put("image", "apache/kafka:4.0.0");
            ObjectNode flinkRestart = nested.addObject().putObject("restart");
            flinkRestart.put("component", "flink");
            flinkRestart.put("image", "flink:2.1.4");
        });

        ScenarioResolutionException exception = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertIssue(
                exception,
                FlinkKafkaCompatibilityValidator.FLINK_LINE_UNSUPPORTED,
                ResolutionScope.SINGLE,
                "$/phases/0/steps/0/loop/steps/1/restart/image");
        assertEquals(1, exception.issues().size());
    }

    @Test
    void acceptsKafka40PatchTagsOptionalDigestsAndNestedRestartImages() {
        String digest = "a".repeat(64);
        ScenarioSpecification scenario = scenario(document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                    .put("image", "apache/kafka:4.0.17@sha256:" + digest);
            ArrayNode phases = document.putArray("phases");
            ObjectNode phase = phases.addObject();
            phase.put("name", "kafka-upgrade");
            ObjectNode restart = phase.putArray("steps").addObject().putObject("restart");
            restart.put("component", "kafka");
            restart.put("image", "apache/kafka:4.0.18");
        });

        ResolvedSide side = resolver.resolve(scenario, ResolutionRequest.none())
                .side(ScenarioSide.SINGLE);

        assertEquals(
                "apache/kafka:4.0.17@sha256:" + digest,
                side.document().at("/setup/kafka/clusters/main/image").textValue());
    }

    @Test
    void rejectsUnsupportedSetupAndNestedKafkaRestartImagesBeforeProvisioning() {
        ScenarioSpecification scenario = scenario(document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main"))
                    .put("image", "apache/kafka:3.9.0");
            ArrayNode phases = document.putArray("phases");
            ObjectNode phase = phases.addObject();
            phase.put("name", "kafka-upgrade");
            ObjectNode loop = phase.putArray("steps").addObject().putObject("loop");
            loop.put("times", 1);
            ObjectNode restart = loop.putArray("steps").addObject().putObject("restart");
            restart.put("component", "kafka");
            restart.put("image", "apache/kafka:4.1.0");
        });

        ScenarioResolutionException exception = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertEquals(2, exception.issues().size());
        assertIssue(
                exception,
                FlinkKafkaCompatibilityValidator.KAFKA_IMAGE_VERSION_UNSUPPORTED,
                ResolutionScope.SINGLE,
                "$/setup/kafka/clusters/main/image");
        assertIssue(
                exception,
                FlinkKafkaCompatibilityValidator.KAFKA_IMAGE_VERSION_UNSUPPORTED,
                ResolutionScope.SINGLE,
                "$/phases/0/steps/0/loop/steps/0/restart/image");
    }

    @Test
    void validatesNestedJobManagerAndTaskManagerRestartImagesInTraversalOrder() {
        ScenarioSpecification scenario = scenario(document -> {
            ArrayNode phases = document.putArray("phases");
            ObjectNode phase = phases.addObject();
            phase.put("name", "component-images");
            ObjectNode loop = phase.putArray("steps").addObject().putObject("loop");
            loop.put("times", 1);
            ArrayNode nested = loop.putArray("steps");
            ObjectNode jobManagerRestart = nested.addObject().putObject("restart");
            jobManagerRestart.put("component", "jobmanager");
            jobManagerRestart.put("image", "flink:1.20.3");
            ObjectNode taskManagerRestart = nested.addObject().putObject("restart");
            taskManagerRestart.put("component", "taskmanager");
            taskManagerRestart.put("image", "flink:2.1.4");
        });

        ScenarioResolutionException exception = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertEquals(2, exception.issues().size());
        assertEquals(
                "$/phases/0/steps/0/loop/steps/0/restart/image",
                exception.issues().get(0).path());
        assertEquals(
                "$/phases/0/steps/0/loop/steps/1/restart/image",
                exception.issues().get(1).path());
        assertTrue(exception.issues().stream().allMatch(issue -> issue.code().equals(
                FlinkKafkaCompatibilityValidator.FLINK_LINE_UNSUPPORTED)));
    }

    @Test
    void reportsTheFlinkRootCauseWithoutDerivativeConnectorNoise() {
        ScenarioSpecification scenario = scenario(document -> {
            flink(document).put("image", "flink:1.20.3");
            connector(document).put(
                    "artifact",
                    "maven:org.apache.flink:flink-connector-kafka:3.4.0-1.20");
        });

        ScenarioResolutionException exception = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(scenario, ResolutionRequest.none()));

        assertIssue(
                exception,
                FlinkKafkaCompatibilityValidator.FLINK_LINE_UNSUPPORTED,
                ResolutionScope.SINGLE,
                "$/setup/flink/image");
        assertFalse(exception.issues().stream().anyMatch(issue ->
                issue.code().equals(
                        FlinkKafkaCompatibilityValidator.KAFKA_CONNECTOR_LINE_UNSUPPORTED)));
        assertEquals(1, exception.issues().size());
    }

    @Test
    void leavesStructuralAndArtifactSyntaxRootCausesToTheirOwningValidators() {
        ScenarioSpecification structurallyInvalid = scenario(document -> {
            parameter(document, "image_value", "integer", IntNode.valueOf(2));
            flink(document).put("image", "${image_value}");
        });

        ScenarioResolutionException schemaException = assertThrows(
                ScenarioResolutionException.class,
                () -> resolver.resolve(structurallyInvalid, ResolutionRequest.none()));
        assertTrue(schemaException.issues().stream().anyMatch(issue ->
                issue.code().startsWith("schema.")
                        && issue.path().equals("$/setup/flink/image")));
        assertFalse(schemaException.issues().stream().anyMatch(issue ->
                issue.code().equals(FlinkKafkaCompatibilityValidator.FLINK_LINE_UNSUPPORTED)));

        ScenarioSpecification malformedMaven = scenario(document ->
                connector(document).put("artifact", "maven:missing-parts"));
        ResolvedScenario resolved = resolver.resolve(malformedMaven, ResolutionRequest.none());
        assertEquals("maven:missing-parts", resolved.side(ScenarioSide.SINGLE)
                .document().at("/subject/connectors/kafka/artifact").textValue());
    }

    private ScenarioSpecification scenario(Consumer<ObjectNode> changes) {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        changes.accept(document);
        return loader.validateScenarioDocument(source, document);
    }

    private static ObjectNode parameter(
            ObjectNode document,
            String name,
            String type,
            JsonNode defaultValue) {
        ObjectNode parameters = document.has("parameters")
                ? (ObjectNode) document.get("parameters")
                : document.putObject("parameters");
        ObjectNode declaration = parameters.putObject(name);
        declaration.put("type", type);
        declaration.set("default", defaultValue);
        return declaration;
    }

    private static void experiment(
            ObjectNode document,
            String parameter,
            JsonNode baseline,
            JsonNode candidate) {
        ObjectNode experiment = document.putObject("experiment");
        experiment.put("claim", "The connector artifact is the only changed input.");
        experiment.putArray("varies").add(parameter);
        experiment.putObject("baseline").set(parameter, baseline);
        experiment.putObject("candidate").set(parameter, candidate);
    }

    private static void useLocalConnector(ObjectNode document) {
        ObjectNode connector = connector(document);
        connector.put("artifact", "./connector.jar");
        connector.putArray("runtime_dependencies");
    }

    private static ObjectNode flink(ObjectNode document) {
        return (ObjectNode) document.at("/setup/flink");
    }

    private static ObjectNode connector(ObjectNode document) {
        return (ObjectNode) document.at("/subject/connectors/kafka");
    }

    private static void assertIssue(
            ScenarioResolutionException exception,
            String code,
            ResolutionScope scope,
            String path) {
        assertTrue(exception.issues().stream().anyMatch(issue ->
                        issue.code().equals(code)
                                && issue.scope() == scope
                                && issue.path().equals(path)),
                () -> "Expected " + code + " [" + scope + "] at " + path
                        + " but got " + exception.issues());
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
