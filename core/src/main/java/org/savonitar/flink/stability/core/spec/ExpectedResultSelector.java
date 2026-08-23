package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Internal selector used only after current-scenario topology preflight. */
final class ExpectedResultSelector {
    private final ScenarioParameterResolver parameterResolver;

    ExpectedResultSelector() {
        this(new ScenarioParameterResolver());
    }

    ExpectedResultSelector(ScenarioParameterResolver parameterResolver) {
        this.parameterResolver = Objects.requireNonNull(parameterResolver, "parameterResolver");
    }

    ResolvedScenarioPlan select(ScenarioBundle bundle, ResolvedScenario resolved) {
        Objects.requireNonNull(bundle, "bundle");
        Objects.requireNonNull(resolved, "resolved");
        if (!sameScenario(bundle.scenario(), resolved.template())) {
            throw new IllegalArgumentException("Resolved scenario does not belong to the supplied bundle");
        }

        ExpectedResultSpecification specification = bundle.expectedResult();
        Path source = specification.source();
        ObjectNode document = specification.document();
        ObjectNode defaultExpectation = (ObjectNode) document.get("default");
        ArrayNode cases = document.get("cases") instanceof ArrayNode array
                ? array
                : document.arrayNode();
        List<ExpectationIssue> issues = new ArrayList<>();
        boolean experiment = resolved.isExperiment();
        validateShape(source, "$/default", defaultExpectation, experiment, issues);

        Map<String, ScenarioParameterContract.Definition> definitions =
                ScenarioParameterContract.definitions(resolved.template().document());
        Set<String> varies = resolved.experiment()
                .map(ResolvedExperiment::varies)
                .orElse(Set.of());
        List<Boolean> locallyValid = new ArrayList<>();
        List<ObjectNode> conditions = new ArrayList<>();
        List<ObjectNode> replacements = new ArrayList<>();

        for (int index = 0; index < cases.size(); index++) {
            ObjectNode caseNode = (ObjectNode) cases.get(index);
            ObjectNode when = (ObjectNode) caseNode.get("when");
            ObjectNode replacement = caseNode.deepCopy();
            replacement.remove("when");
            conditions.add(when);
            replacements.add(replacement);
            validateShape(source, "$/cases/" + index, replacement, experiment, issues);
            boolean valid = validateCaseLiterals(
                    source, index, when, definitions, varies, issues);
            locallyValid.add(valid);
            if (replacement.equals(defaultExpectation)) {
                issues.add(issue(source, "expectation.case-redundant", "$/cases/" + index,
                        "Case replacement is identical to default"));
            }
        }

        if (issues.isEmpty()) {
            validateOverlaps(source, conditions, issues);
        }
        if (issues.isEmpty()) {
            for (int index = 0; index < cases.size(); index++) {
                if (locallyValid.get(index)) {
                    validateCaseByResolution(source, index, conditions.get(index), resolved, issues);
                }
            }
        }

        if (!issues.isEmpty()) {
            throw new ExpectedResultSelectionException(issues);
        }

        List<Integer> matches = new ArrayList<>();
        for (int index = 0; index < conditions.size(); index++) {
            if (matches(conditions.get(index), resolved.commonEffectiveParameters())) {
                matches.add(index);
            }
        }
        if (matches.size() > 1) {
            throw new ExpectedResultSelectionException(List.of(issue(
                    source,
                    "expectation.multiple-matches",
                    "$/cases",
                    "Resolved parameters match cases " + matches)));
        }

        SelectedExpectation selected;
        if (matches.isEmpty()) {
            selected = new SelectedExpectation(
                    specification,
                    ExpectationSelectionKind.DEFAULT,
                    null,
                    "$/default",
                    Map.of(),
                    defaultExpectation);
        } else {
            int index = matches.getFirst();
            selected = new SelectedExpectation(
                    specification,
                    ExpectationSelectionKind.CASE,
                    index,
                    "$/cases/" + index,
                    nodeMap(conditions.get(index)),
                    replacements.get(index));
        }
        return new ResolvedScenarioPlan(resolved, selected);
    }

    private void validateCaseByResolution(
            Path source,
            int index,
            ObjectNode conditions,
            ResolvedScenario resolved,
            List<ExpectationIssue> issues) {
        Map<String, JsonNode> probeBindings = new LinkedHashMap<>();
        resolved.commonEffectiveParameters().forEach((name, parameter) ->
                probeBindings.put(name, parameter.value()));
        conditions.fields().forEachRemaining(entry ->
                probeBindings.put(entry.getKey(), entry.getValue().deepCopy()));
        try {
            ResolvedScenario probe = parameterResolver.resolveExpectationProbe(
                    resolved.template(), new ResolutionRequest(probeBindings, Map.of()));
            List<PreflightIssue> preflightIssues = new ArrayList<>();
            for (ResolvedSide side : probe.sides()) {
                preflightIssues.addAll(ScenarioPreflightValidator.validateResolvedSide(
                        probe.template().source(), scopeOf(side.side()), side.document()));
            }
            if (!preflightIssues.isEmpty()) {
                String details = preflightIssues.stream()
                        .map(issue -> issue.scope() + ":" + issue.code() + "@" + issue.path()
                                + " - " + issue.message())
                        .sorted()
                        .distinct()
                        .collect(Collectors.joining(", "));
                issues.add(issue(source, "expectation.case-value-invalid", "$/cases/" + index + "/when",
                        "Case values cannot produce a valid scenario: " + details));
            }
        } catch (ScenarioResolutionException exception) {
            String details = exception.issues().stream()
                    .map(issue -> issue.scope() + ":" + issue.code() + "@" + issue.path()
                            + " - " + issue.message())
                    .sorted()
                    .distinct()
                    .collect(Collectors.joining(", "));
            issues.add(issue(source, "expectation.case-value-invalid", "$/cases/" + index + "/when",
                    "Case values cannot produce a valid scenario: " + details));
        }
    }

    private static ResolutionScope scopeOf(ScenarioSide side) {
        return switch (side) {
            case SINGLE -> ResolutionScope.SINGLE;
            case BASELINE -> ResolutionScope.BASELINE;
            case CANDIDATE -> ResolutionScope.CANDIDATE;
        };
    }

    private static boolean validateCaseLiterals(
            Path source,
            int index,
            ObjectNode conditions,
            Map<String, ScenarioParameterContract.Definition> definitions,
            Set<String> varies,
            List<ExpectationIssue> issues) {
        boolean valid = true;
        var fields = conditions.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String path = "$/cases/" + index + "/when/" + escapePointer(entry.getKey());
            ScenarioParameterContract.Definition definition = definitions.get(entry.getKey());
            if (definition == null) {
                issues.add(issue(source, "expectation.case-unknown-parameter", path,
                        "Case references undeclared parameter '" + entry.getKey() + "'"));
                valid = false;
                continue;
            }
            if (varies.contains(entry.getKey())) {
                issues.add(issue(source, "expectation.case-varied-parameter", path,
                        "Case cannot select experiment.varies parameter '" + entry.getKey() + "'"));
                valid = false;
                continue;
            }
            List<ScenarioParameterContract.ValueProblem> problems =
                    ScenarioParameterContract.validate(definition, entry.getValue());
            if (!problems.isEmpty()) {
                valid = false;
                problems.forEach(problem -> issues.add(issue(
                        source,
                        "expectation.case-value-invalid",
                        path,
                        problem.message())));
            }
        }
        return valid;
    }

    private static void validateShape(
            Path source,
            String path,
            ObjectNode expectation,
            boolean experiment,
            List<ExpectationIssue> issues) {
        boolean experimentShape = expectation.has("baseline") && expectation.has("candidate");
        if (experiment != experimentShape) {
            issues.add(issue(source, "expectation.shape-mismatch", path,
                    experiment
                            ? "Experiment requires baseline and candidate expectations"
                            : "Plain scenario requires one outcome expectation"));
        }
    }

    private static void validateOverlaps(
            Path source, List<ObjectNode> conditions, List<ExpectationIssue> issues) {
        for (int later = 0; later < conditions.size(); later++) {
            List<Integer> overlapping = new ArrayList<>();
            for (int earlier = 0; earlier < later; earlier++) {
                if (overlap(conditions.get(earlier), conditions.get(later))) {
                    overlapping.add(earlier);
                }
            }
            if (!overlapping.isEmpty()) {
                issues.add(issue(source, "expectation.case-overlap", "$/cases/" + later + "/when",
                        "Case overlaps earlier cases " + overlapping));
            }
        }
    }

    private static boolean overlap(ObjectNode left, ObjectNode right) {
        var fields = left.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            JsonNode other = right.get(entry.getKey());
            if (other != null && !typedEquals(entry.getValue(), other)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matches(
            ObjectNode conditions, Map<String, EffectiveParameter> effectiveParameters) {
        var fields = conditions.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            EffectiveParameter effective = effectiveParameters.get(entry.getKey());
            if (effective == null || !typedEquals(effective.internalValue(), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static boolean typedEquals(JsonNode left, JsonNode right) {
        if (left.isIntegralNumber() && right.isIntegralNumber()) {
            return left.bigIntegerValue().equals(right.bigIntegerValue());
        }
        return left.equals(right);
    }

    private static Map<String, JsonNode> nodeMap(ObjectNode object) {
        Map<String, JsonNode> values = new LinkedHashMap<>();
        object.fields().forEachRemaining(entry -> values.put(entry.getKey(), entry.getValue()));
        return values;
    }

    private static boolean sameScenario(
            ScenarioSpecification left, ScenarioSpecification right) {
        return left.source().equals(right.source())
                && left.name().equals(right.name())
                && left.document().equals(right.document());
    }

    private static ExpectationIssue issue(Path source, String code, String path, String message) {
        return new ExpectationIssue(source, code, path, message);
    }

    private static String escapePointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }
}
