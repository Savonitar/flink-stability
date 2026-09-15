package org.savonitar.flink.stability.core.spec.document;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ScenarioBundleTest {
    private final SpecificationLoader loader = new SpecificationLoader();

    @Test
    void rejectsScenarioWhoseNameDoesNotMatchItsFilename() {
        ScenarioSpecification scenario = new ScenarioSpecification(
                resource("minimal.yaml").resolveSibling("renamed.yaml"),
                loader.loadScenario(resource("minimal.yaml")).document());

        assertThrows(IllegalArgumentException.class,
                () -> new ScenarioBundle(scenario, loader.loadExpectedResult(resource("minimal.expected.yaml"))));
    }

    @Test
    void rejectsExpectedResultThatIsNotTheExactSibling() {
        ExpectedResultSpecification expected = new ExpectedResultSpecification(
                resource("minimal.expected.yaml").resolveSibling("elsewhere/minimal.expected.yaml"),
                loader.loadExpectedResult(resource("minimal.expected.yaml")).document());

        assertThrows(IllegalArgumentException.class,
                () -> new ScenarioBundle(loader.loadScenario(resource("minimal.yaml")), expected));
    }

    @Test
    void rejectsExpectedResultThatTargetsAnotherScenario() {
        ExpectedResultSpecification expected = expected(document ->
                document.withObject("meta").put("scenario", "another"));

        assertThrows(IllegalArgumentException.class,
                () -> new ScenarioBundle(loader.loadScenario(resource("minimal.yaml")), expected));
    }

    @Test
    void rejectsExpectedResultWithWrongMetadataName() {
        ExpectedResultSpecification expected = expected(document ->
                document.withObject("meta").put("name", "another.expected"));

        assertThrows(IllegalArgumentException.class,
                () -> new ScenarioBundle(loader.loadScenario(resource("minimal.yaml")), expected));
    }

    private ExpectedResultSpecification expected(java.util.function.Consumer<ObjectNode> change) {
        Path source = resource("minimal.expected.yaml");
        ObjectNode document = loader.loadExpectedResult(source).document();
        change.accept(document);
        return loader.validateExpectedResultDocument(source, document);
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
