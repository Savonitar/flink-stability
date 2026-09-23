package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.ScenarioBundle;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalog;
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
        List<SuitePlanningIssue> issues = new ArrayList<>();
        for (int index = 0; index < nodes.size(); index++) {
            ObjectNode node = (ObjectNode) nodes.get(index);
            SuiteEntryIdentity identity = identity(suite, node, index);
            ScenarioBundle bundle = catalog.scenario(identity.scenarioName()).orElse(null);
            if (bundle == null) {
                issues.add(new SuitePlanningIssue(
                        identity,
                        suite.source(),
                        ResolutionScope.COMMON,
                        "suite.scenario-not-found",
                        identity.entryPointer() + "/scenario",
                        "Suite references undiscovered scenario '"
                                + identity.scenarioName() + "'"));
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
            } catch (ScenarioResolutionException exception) {
                exception.issues().forEach(issue -> issues.add(
                        mapResolutionIssue(identity, issue)));
            } catch (ExpectedResultSelectionException exception) {
                exception.issues().forEach(issue -> issues.add(new SuitePlanningIssue(
                        identity,
                        issue.source(),
                        ResolutionScope.COMMON,
                        issue.code(),
                        issue.path(),
                        issue.message())));
            } catch (ScenarioPreflightException exception) {
                exception.issues().forEach(issue -> issues.add(new SuitePlanningIssue(
                        identity,
                        issue.source(),
                        issue.scope(),
                        issue.code(),
                        issue.path(),
                        issue.message())));
            }
        }

        if (!issues.isEmpty()) {
            throw new SuitePlanningException(issues);
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

    private static SuitePlanningIssue mapResolutionIssue(
            SuiteEntryIdentity identity,
            ResolutionIssue issue) {
        PathAndPointer location = rebaseSuiteBinding(identity, issue.source(), issue.path());
        return new SuitePlanningIssue(
                identity,
                location.source(),
                issue.scope(),
                issue.code(),
                location.path(),
                issue.message());
    }

    private static PathAndPointer rebaseSuiteBinding(
            SuiteEntryIdentity identity,
            java.nio.file.Path source,
            String path) {
        if (!path.equals(SUITE_BINDING_PREFIX)
                && !path.startsWith(SUITE_BINDING_PREFIX + "/")) {
            return new PathAndPointer(source, path);
        }
        return new PathAndPointer(
                identity.suiteSource(),
                identity.entryPointer() + "/parameters"
                        + path.substring(SUITE_BINDING_PREFIX.length()));
    }

    private record PathAndPointer(java.nio.file.Path source, String path) {}
}
