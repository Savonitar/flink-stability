package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalog;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import org.savonitar.flink.stability.core.spec.document.SuiteEntryIdentity;
import org.savonitar.flink.stability.core.spec.document.SuiteSpecification;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Eagerly resolves every suite entry before any provisioning is permitted. */
public final class SuitePlanResolver {
    private static final String SUITE_BINDING_PREFIX = "$/suite-bindings";

    private final ScenarioPlanResolver scenarioPlanResolver;

    public SuitePlanResolver() {
        this(new ScenarioPlanResolver());
    }

    SuitePlanResolver(ScenarioPlanResolver scenarioPlanResolver) {
        this.scenarioPlanResolver = Objects.requireNonNull(
                scenarioPlanResolver, "scenarioPlanResolver");
    }

    public ResolvedSuitePlan resolve(
            SpecificationCatalog catalog,
            String suiteName) {
        return resolve(catalog, suiteName, Map.of());
    }

    /**
     * Applies submit-time overrides strictly to every entry. If any referenced scenario does
     * not declare a key, normal scenario resolution rejects the whole suite.
     */
    public ResolvedSuitePlan resolve(
            SpecificationCatalog catalog,
            String suiteName,
            Map<String, ? extends JsonNode> submitOverrides) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(suiteName, "suiteName");
        Objects.requireNonNull(submitOverrides, "submitOverrides");
        Map<String, JsonNode> submitSnapshot = immutableNodeMap(submitOverrides);
        SuiteSpecification suite = catalog.suite(suiteName).orElseThrow(() ->
                new IllegalArgumentException(
                        "Catalog contains no suite '" + suiteName + "'; available suites: "
                                + catalog.suites().keySet().stream().sorted().toList()));

        ArrayNode nodes = (ArrayNode) suite.document().get("scenarios");
        List<ResolvedSuiteEntry> entries = new ArrayList<>();
        List<Diagnostic> issues = new ArrayList<>();
        for (int index = 0; index < nodes.size(); index++) {
            ObjectNode node = (ObjectNode) nodes.get(index);
            SuiteEntryIdentity identity = identity(suite, node, index);
            ScenarioBundle bundle = catalog.scenario(identity.scenarioName()).orElse(null);
            if (bundle == null) {
                issues.add(new Diagnostic(
                        suite.source(),
                        "suite.scenario-not-found",
                        identity.entryPointer() + "/scenario",
                        "Suite references undiscovered scenario '"
                                + identity.scenarioName() + "'").inSuiteEntry(identity));
                continue;
            }

            try {
                ResolvedScenarioPlan scenarioPlan = scenarioPlanResolver.resolve(
                        bundle,
                        new ResolutionRequest(entryParameters(node), submitSnapshot));
                BigInteger suiteRuns = node.has("runs")
                        ? node.path("runs").bigIntegerValue()
                        : null;
                entries.add(new ResolvedSuiteEntry(
                        identity, scenarioPlan, suiteRuns));
            } catch (SpecificationException exception) {
                issues.addAll(attributeToEntry(identity, exception));
            }
        }

        if (!issues.isEmpty()) {
            throw new SpecificationException(Stage.SUITE_PLANNING, issues);
        }
        return new ResolvedSuitePlan(suite, entries);
    }

    private static SuiteEntryIdentity identity(
            SuiteSpecification suite,
            ObjectNode entry,
            int index) {
        String scenarioName = entry.path("scenario").textValue();
        String entryId = entry.has("as") ? entry.path("as").textValue() : scenarioName;
        return new SuiteEntryIdentity(
                suite.source(),
                suite.name(),
                index,
                "$/scenarios/" + index,
                entryId,
                scenarioName);
    }

    private static Map<String, JsonNode> entryParameters(ObjectNode entry) {
        Map<String, JsonNode> parameters = new LinkedHashMap<>();
        if (entry.get("parameters") instanceof ObjectNode object) {
            object.fields().forEachRemaining(parameter ->
                    parameters.put(parameter.getKey(), parameter.getValue()));
        }
        return parameters;
    }

    private static Map<String, JsonNode> immutableNodeMap(
            Map<String, ? extends JsonNode> values) {
        Map<String, JsonNode> copy = new LinkedHashMap<>();
        values.forEach((name, value) -> copy.put(
                Objects.requireNonNull(name, "submit override name"),
                Objects.requireNonNull(value, "submit override value").deepCopy()));
        return Collections.unmodifiableMap(copy);
    }

    /** Attributes one entry's resolution, expectation, or preflight failure to that entry. */
    private static List<Diagnostic> attributeToEntry(
            SuiteEntryIdentity identity,
            SpecificationException failure) {
        return switch (failure.stage()) {
            case RESOLUTION -> failure.diagnostics().stream()
                    .map(diagnostic -> rebaseSuiteBinding(identity, diagnostic))
                    .toList();
            case EXPECTATION, PREFLIGHT -> failure.diagnostics().stream()
                    .map(diagnostic -> diagnostic.inSuiteEntry(identity))
                    .toList();
            default -> throw failure;
        };
    }

    private static Diagnostic rebaseSuiteBinding(
            SuiteEntryIdentity identity,
            Diagnostic diagnostic) {
        String path = diagnostic.path();
        if (!path.equals(SUITE_BINDING_PREFIX)
                && !path.startsWith(SUITE_BINDING_PREFIX + "/")) {
            return diagnostic.inSuiteEntry(identity);
        }
        return new Diagnostic(
                identity.suiteSource(),
                diagnostic.scope(),
                Optional.of(identity),
                diagnostic.code(),
                identity.entryPointer() + "/parameters"
                        + path.substring(SUITE_BINDING_PREFIX.length()),
                diagnostic.message());
    }
}
