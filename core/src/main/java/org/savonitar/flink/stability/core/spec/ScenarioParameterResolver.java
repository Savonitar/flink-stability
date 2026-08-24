package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.core.spec.ScenarioParameterContract.Definition;

/** Resolves typed v1 parameters, interpolation, and schema-annotated defaults. */
public final class ScenarioParameterResolver {
    private static final Pattern TEMPLATE = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");

    private final SpecificationLoader specificationLoader;
    private final ScenarioCapabilityValidator capabilityValidator;
    private final FlinkKafkaCompatibilityValidator compatibilityValidator;
    private final ExperimentDriftValidator driftValidator;

    public ScenarioParameterResolver() {
        this(new SpecificationLoader());
    }

    ScenarioParameterResolver(SpecificationLoader specificationLoader) {
        this.specificationLoader = specificationLoader;
        this.capabilityValidator = new ScenarioCapabilityValidator();
        this.compatibilityValidator = new FlinkKafkaCompatibilityValidator();
        this.driftValidator = new ExperimentDriftValidator();
    }

    public ResolvedScenario resolve(ScenarioSpecification scenario, ResolutionRequest request) {
        return resolve(scenario, request, true);
    }

    ResolvedScenario resolveExpectationProbe(
            ScenarioSpecification scenario,
            ResolutionRequest request) {
        return resolve(scenario, request, false);
    }

    private ResolvedScenario resolve(
            ScenarioSpecification scenario,
            ResolutionRequest request,
            boolean validateInvocationPolicyBindings) {
        Objects.requireNonNull(scenario, "scenario");
        Objects.requireNonNull(request, "request");
        Path source = scenario.source();
        ObjectNode template = scenario.document();
        List<ResolutionIssue> issues = new ArrayList<>();
        Map<String, Definition> definitions = readDefinitions(template, source, issues);
        Set<String> varies = readVaries(template, source, definitions, issues);

        validateBindings(source, ResolutionScope.COMMON, "suite", request.internalSuiteBindings(),
                definitions, varies, true, issues);
        validateBindings(source, ResolutionScope.COMMON, "submit", request.internalSubmitOverrides(),
                definitions, varies, true, issues);

        Map<String, JsonNode> baselineOverrides = readSideOverrides(template, "baseline");
        Map<String, JsonNode> candidateOverrides = readSideOverrides(template, "candidate");
        if (template.has("experiment")) {
            validateBindings(source, ResolutionScope.BASELINE, "experiment baseline", baselineOverrides,
                    definitions, varies, false, issues);
            validateBindings(source, ResolutionScope.CANDIDATE, "experiment candidate", candidateOverrides,
                    definitions, varies, false, issues);
        }

        if (!issues.isEmpty()) {
            throw new ScenarioResolutionException(issues);
        }

        Map<String, EffectiveParameter> preSide = bindCommon(
                definitions, request.internalSuiteBindings(), request.internalSubmitOverrides());
        Map<String, EffectiveParameter> common = template.has("experiment")
                ? withoutKeys(preSide, varies)
                : preSide;
        List<SideMaterialization> materializations = new ArrayList<>();
        ResolvedExperiment resolvedExperiment = null;
        if (template.has("experiment")) {
            Map<String, EffectiveParameter> baseline = bindSide(
                    preSide, baselineOverrides, ParameterSource.EXPERIMENT_BASELINE);
            Map<String, EffectiveParameter> candidate = bindSide(
                    preSide, candidateOverrides, ParameterSource.EXPERIMENT_CANDIDATE);
            validateRequired(source, ResolutionScope.BASELINE, definitions, baseline, issues);
            validateRequired(source, ResolutionScope.CANDIDATE, definitions, candidate, issues);
            if (issues.isEmpty()) {
                resolvedExperiment = resolveExperimentMetadata(source, template, varies, common, issues);
                materializations.add(materialize(source, template, ScenarioSide.BASELINE, baseline, issues));
                materializations.add(materialize(source, template, ScenarioSide.CANDIDATE, candidate, issues));
            }
        } else {
            validateRequired(source, ResolutionScope.SINGLE, definitions, common, issues);
            if (issues.isEmpty()) {
                materializations.add(materialize(source, template, ScenarioSide.SINGLE, common, issues));
            }
        }

        if (!issues.isEmpty()) {
            throw new ScenarioResolutionException(issues);
        }

        if (validateInvocationPolicyBindings) {
            for (SideMaterialization materialization : materializations) {
                validateInvocationPolicyBindings(source, materialization, request, issues);
            }
            if (!issues.isEmpty()) {
                throw new ScenarioResolutionException(issues);
            }
        }

        for (SideMaterialization materialization : materializations) {
            issues.addAll(capabilityValidator.validate(
                    source, scopeOf(materialization.side()), materialization.document()));
        }
        if (!issues.isEmpty()) {
            throw new ScenarioResolutionException(issues);
        }

        for (SideMaterialization materialization : materializations) {
            try {
                specificationLoader.validateResolvedScenario(source, materialization.document());
            } catch (DocumentValidationException exception) {
                ResolutionScope scope = scopeOf(materialization.side());
                exception.issues().forEach(issue -> issues.add(new ResolutionIssue(source, scope,
                        issue.code(), issue.path(), "Resolved document: " + issue.message())));
            }
        }
        if (!issues.isEmpty()) {
            throw new ScenarioResolutionException(issues);
        }

        for (SideMaterialization materialization : materializations) {
            issues.addAll(compatibilityValidator.validate(
                    source, scopeOf(materialization.side()), materialization.document()));
        }
        if (!issues.isEmpty()) {
            throw new ScenarioResolutionException(issues);
        }

        if (materializations.size() == 2) {
            SideMaterialization baseline = materializations.get(0);
            SideMaterialization candidate = materializations.get(1);
            issues.addAll(driftValidator.validate(
                    source,
                    varies,
                    baseline.effectiveParameters(),
                    candidate.effectiveParameters(),
                    baseline.document(),
                    candidate.document(),
                    baseline.provenance(),
                    candidate.provenance()));
        }
        if (!issues.isEmpty()) {
            throw new ScenarioResolutionException(issues);
        }

        List<ResolvedSide> resolvedSides = materializations.stream()
                .map(materialization -> new ResolvedSide(
                        materialization.side(),
                        materialization.document(),
                        materialization.effectiveParameters(),
                        materialization.provenance(),
                        materialization.appliedDefaults()))
                .toList();
        return new ResolvedScenario(scenario, common, resolvedExperiment, resolvedSides);
    }

    private static void validateInvocationPolicyBindings(
            Path source,
            SideMaterialization materialization,
            ResolutionRequest request,
            List<ResolutionIssue> issues) {
        validateInvocationPolicyBinding(
                source, materialization, request, "$/runs", "runs", issues);
        validateInvocationPolicyBinding(
                source,
                materialization,
                request,
                "$/health_retry_limit",
                "health-retry-limit",
                issues);
    }

    private static void validateInvocationPolicyBinding(
            Path source,
            SideMaterialization materialization,
            ResolutionRequest request,
            String documentPath,
            String codeSuffix,
            List<ResolutionIssue> issues) {
        materialization.provenance().getOrDefault(documentPath, Set.of()).forEach(parameter -> {
            if (request.internalSuiteBindings().containsKey(parameter)) {
                addInvocationPolicyBindingIssue(
                        source, parameter, "suite", documentPath, codeSuffix, issues);
            }
            if (request.internalSubmitOverrides().containsKey(parameter)) {
                addInvocationPolicyBindingIssue(
                        source, parameter, "submit", documentPath, codeSuffix, issues);
            }
        });
    }

    private static void addInvocationPolicyBindingIssue(
            Path source,
            String parameter,
            String bindingKind,
            String documentPath,
            String codeSuffix,
            List<ResolutionIssue> issues) {
        String codeSource = "suite".equals(bindingKind)
                ? "suite-binding"
                : "submit-override";
        issues.add(issue(
                source,
                ResolutionScope.COMMON,
                "parameter." + codeSource + "-controls-" + codeSuffix,
                bindingPath(bindingKind, parameter),
                ("suite".equals(bindingKind) ? "Suite binding" : "Submit-time override")
                        + " for parameter '" + parameter + "' cannot control "
                        + documentPath + "; use the committed scenario value"
                        + ("runs".equals(codeSuffix) ? " or suite-entry runs" : "")));
    }

    private Map<String, Definition> readDefinitions(
            ObjectNode template, Path source, List<ResolutionIssue> issues) {
        Map<String, Definition> definitions = ScenarioParameterContract.definitions(template);
        JsonNode parameters = template.get("parameters");
        if (!(parameters instanceof ObjectNode parameterObject)) {
            return definitions;
        }

        findForbiddenDefinitionTemplates(source, parameterObject, "$/parameters", issues);
        definitions.forEach((name, definition) -> {
            String declarationPath = "$/parameters/" + escapePointer(name);
            if (definition.minimum() != null && definition.maximum() != null
                    && definition.minimum().compareTo(definition.maximum()) > 0) {
                issues.add(issue(source, ResolutionScope.COMMON, "parameter.invalid-bounds", declarationPath,
                        "Parameter minimum " + definition.minimum()
                                + " exceeds maximum " + definition.maximum()));
            }
            if (definition.defaultValue() != null) {
                validateValue(source, ResolutionScope.COMMON, definition, definition.defaultValue(),
                        declarationPath + "/default", "scenario default", issues);
            }
        });
        return definitions;
    }

    private Set<String> readVaries(
            ObjectNode template,
            Path source,
            Map<String, Definition> definitions,
            List<ResolutionIssue> issues) {
        Set<String> varies = new LinkedHashSet<>();
        JsonNode variesNode = template.at("/experiment/varies");
        if (variesNode instanceof ArrayNode array) {
            for (int index = 0; index < array.size(); index++) {
                String name = array.get(index).textValue();
                varies.add(name);
                if (!definitions.containsKey(name)) {
                    issues.add(issue(source, ResolutionScope.COMMON, "parameter.unknown-varies",
                            "$/experiment/varies/" + index,
                            "Experiment varies undeclared parameter '" + name + "'"));
                }
            }
        }
        return varies;
    }

    private void validateBindings(
            Path source,
            ResolutionScope scope,
            String bindingKind,
            Map<String, JsonNode> bindings,
            Map<String, Definition> definitions,
            Set<String> varies,
            boolean commonBinding,
            List<ResolutionIssue> issues) {
        bindings.forEach((name, value) -> {
            Definition definition = definitions.get(name);
            String path = bindingPath(bindingKind, name);
            if (definition == null) {
                issues.add(issue(source, scope, unknownBindingCode(bindingKind), path,
                        "Unknown parameter '" + name + "'; declared parameters: "
                                + String.join(", ", definitions.keySet())));
                return;
            }
            if (commonBinding && varies.contains(name)) {
                issues.add(issue(source, scope, variedBindingCode(bindingKind), path,
                        "Parameter '" + name + "' is owned by experiment.varies"));
            }
            if (!commonBinding && !varies.contains(name)) {
                issues.add(issue(source, scope, "parameter.side-key-not-varied", path,
                        "Experiment side may override only experiment.varies; '" + name + "' is not listed"));
            }
            validateValue(source, scope, definition, value, path, bindingKind, issues);
        });
    }

    private void validateValue(
            Path source,
            ResolutionScope scope,
            Definition definition,
            JsonNode value,
            String path,
            String valueSource,
            List<ResolutionIssue> issues) {
        ScenarioParameterContract.validate(definition, value).forEach(problem ->
                issues.add(issue(source, scope, "parameter." + problem.code(), path,
                        valueSource + " for " + problem.message())));
    }

    private Map<String, EffectiveParameter> bindCommon(
            Map<String, Definition> definitions,
            Map<String, JsonNode> suiteBindings,
            Map<String, JsonNode> submitOverrides) {
        Map<String, EffectiveParameter> effective = new LinkedHashMap<>();
        definitions.values().stream()
                .filter(definition -> definition.defaultValue() != null)
                .forEach(definition -> effective.put(definition.name(),
                        new EffectiveParameter(definition.defaultValue(), ParameterSource.SCENARIO_DEFAULT)));
        applyBindings(effective, suiteBindings, ParameterSource.SUITE_BINDING);
        applyBindings(effective, submitOverrides, ParameterSource.SUBMIT_OVERRIDE);
        return effective;
    }

    private Map<String, EffectiveParameter> bindSide(
            Map<String, EffectiveParameter> common,
            Map<String, JsonNode> overrides,
            ParameterSource source) {
        Map<String, EffectiveParameter> side = new LinkedHashMap<>(common);
        applyBindings(side, overrides, source);
        return side;
    }

    private static Map<String, EffectiveParameter> withoutKeys(
            Map<String, EffectiveParameter> parameters, Set<String> excluded) {
        Map<String, EffectiveParameter> filtered = new LinkedHashMap<>();
        parameters.forEach((name, value) -> {
            if (!excluded.contains(name)) {
                filtered.put(name, value);
            }
        });
        return filtered;
    }

    private static void applyBindings(
            Map<String, EffectiveParameter> effective,
            Map<String, JsonNode> bindings,
            ParameterSource source) {
        bindings.forEach((name, value) -> effective.put(name, new EffectiveParameter(value, source)));
    }

    private void validateRequired(
            Path source,
            ResolutionScope scope,
            Map<String, Definition> definitions,
            Map<String, EffectiveParameter> effective,
            List<ResolutionIssue> issues) {
        definitions.values().stream()
                .filter(Definition::required)
                .filter(definition -> !effective.containsKey(definition.name()))
                .forEach(definition -> issues.add(issue(source, scope, "parameter.required-value-missing",
                        "$/parameters/" + escapePointer(definition.name()),
                        "Required parameter '" + definition.name() + "' has no effective value")));
    }

    private SideMaterialization materialize(
            Path source,
            ObjectNode template,
            ScenarioSide side,
            Map<String, EffectiveParameter> effective,
            List<ResolutionIssue> issues) {
        ObjectNode document = template.deepCopy();
        document.remove("parameters");
        document.remove("experiment");
        Map<String, Set<String>> provenance = new LinkedHashMap<>();
        resolveNode(source, scopeOf(side), document, "$", effective, provenance, issues);
        List<AppliedDefault> appliedDefaults = applyDefaults(document);
        return new SideMaterialization(side, document, effective, provenance, appliedDefaults);
    }

    private ResolvedExperiment resolveExperimentMetadata(
            Path source,
            ObjectNode template,
            Set<String> varies,
            Map<String, EffectiveParameter> common,
            List<ResolutionIssue> issues) {
        String claim = template.at("/experiment/claim").textValue();
        if (!containsTemplateSyntax(claim)) {
            return resolvedExperiment(source, claim, varies, issues);
        }

        Matcher matcher = TEMPLATE.matcher(claim);
        Set<String> referenced = new LinkedHashSet<>();
        while (matcher.find()) {
            referenced.add(matcher.group(1));
        }
        Set<String> variedReferences = new LinkedHashSet<>(referenced);
        variedReferences.retainAll(varies);
        if (!variedReferences.isEmpty()) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "parameter.varied-reference-in-experiment-claim", "$/experiment/claim",
                    "Experiment claim may reference only pair-common parameters; varied references: "
                            + String.join(", ", variedReferences)));
            return null;
        }

        JsonNode resolved = resolveText(source, ResolutionScope.COMMON, claim, "$/experiment/claim",
                common, new LinkedHashMap<>(), issues);
        if (!resolved.isTextual()) {
            issues.add(issue(source, ResolutionScope.COMMON, "schema.type", "$/experiment/claim",
                    "Resolved experiment claim must remain a string"));
            return null;
        }
        return resolvedExperiment(source, resolved.textValue(), varies, issues);
    }

    private static ResolvedExperiment resolvedExperiment(
            Path source,
            String claim,
            Set<String> varies,
            List<ResolutionIssue> issues) {
        if (claim.isBlank()) {
            issues.add(issue(source, ResolutionScope.COMMON, "experiment.claim-blank", "$/experiment/claim",
                    "Resolved experiment claim must contain non-whitespace text"));
            return null;
        }
        return new ResolvedExperiment(claim, varies);
    }

    private JsonNode resolveNode(
            Path source,
            ResolutionScope scope,
            JsonNode node,
            String path,
            Map<String, EffectiveParameter> effective,
            Map<String, Set<String>> provenance,
            List<ResolutionIssue> issues) {
        if (node instanceof ObjectNode object) {
            List<String> names = new ArrayList<>();
            object.fieldNames().forEachRemaining(names::add);
            for (String name : names) {
                String fieldPath = path + "/" + escapePointer(name);
                if (containsTemplateSyntax(name)) {
                    issues.add(issue(source, scope, "parameter.reference-in-map-key", fieldPath,
                            "Templates are not allowed in map keys"));
                }
                object.set(name, resolveNode(source, scope, object.get(name), fieldPath,
                        effective, provenance, issues));
            }
            return object;
        }
        if (node instanceof ArrayNode array) {
            for (int index = 0; index < array.size(); index++) {
                array.set(index, resolveNode(source, scope, array.get(index), path + "/" + index,
                        effective, provenance, issues));
            }
            return array;
        }
        if (!node.isTextual() || !containsTemplateSyntax(node.textValue())) {
            return node;
        }
        return resolveText(source, scope, node.textValue(), path, effective, provenance, issues);
    }

    private JsonNode resolveText(
            Path source,
            ResolutionScope scope,
            String text,
            String path,
            Map<String, EffectiveParameter> effective,
            Map<String, Set<String>> provenance,
            List<ResolutionIssue> issues) {
        Matcher matcher = TEMPLATE.matcher(text);
        if (matcher.matches()) {
            String parameter = matcher.group(1);
            EffectiveParameter resolved = effective.get(parameter);
            if (resolved == null) {
                addUnknownReference(source, scope, path, parameter, effective, issues);
                return TextNode.valueOf(text);
            }
            provenance.computeIfAbsent(path, ignored -> new LinkedHashSet<>()).add(parameter);
            return resolved.internalValue().deepCopy();
        }

        StringBuilder resolvedText = new StringBuilder();
        int cursor = 0;
        boolean matched = false;
        matcher.reset();
        while (matcher.find()) {
            matched = true;
            resolvedText.append(text, cursor, matcher.start());
            String parameter = matcher.group(1);
            EffectiveParameter resolved = effective.get(parameter);
            if (resolved == null) {
                addUnknownReference(source, scope, path, parameter, effective, issues);
                resolvedText.append(matcher.group());
            } else {
                provenance.computeIfAbsent(path, ignored -> new LinkedHashSet<>()).add(parameter);
                resolvedText.append(lexicalValue(resolved.internalValue()));
            }
            cursor = matcher.end();
        }
        resolvedText.append(text, cursor, text.length());

        String withoutValidTemplates = TEMPLATE.matcher(text).replaceAll("");
        if (!matched || containsTemplateSyntax(withoutValidTemplates)) {
            issues.add(issue(source, scope, "parameter.invalid-reference-syntax", path,
                    "Invalid parameter reference syntax in '" + text + "'"));
        }
        return TextNode.valueOf(resolvedText.toString());
    }

    private List<AppliedDefault> applyDefaults(ObjectNode document) {
        List<AppliedDefault> defaults = new ArrayList<>();
        putDefault(document, "health_retry_limit", IntNode.valueOf(1), "$/health_retry_limit", defaults);

        JsonNode clusters = document.at("/setup/kafka/clusters");
        if (clusters instanceof ObjectNode clusterObject) {
            clusterObject.fields().forEachRemaining(entry -> {
                if (entry.getValue() instanceof ObjectNode cluster) {
                    putDefault(cluster, "mode", TextNode.valueOf("kraft"),
                            "$/setup/kafka/clusters/" + escapePointer(entry.getKey()) + "/mode", defaults);
                }
            });
        }

        JsonNode flink = document.at("/setup/flink");
        if (flink instanceof ObjectNode flinkObject) {
            putDefault(flinkObject, "jobmanagers", IntNode.valueOf(1), "$/setup/flink/jobmanagers", defaults);
            putDefault(flinkObject, "taskmanagers", IntNode.valueOf(1), "$/setup/flink/taskmanagers", defaults);
        }

        JsonNode jobs = document.at("/workload/jobs");
        if (jobs instanceof ArrayNode jobArray) {
            for (int index = 0; index < jobArray.size(); index++) {
                if (jobArray.get(index) instanceof ObjectNode job) {
                    putDefault(job, "start", TextNode.valueOf("auto"),
                            "$/workload/jobs/" + index + "/start", defaults);
                }
            }
        }
        return defaults;
    }

    private static void putDefault(
            ObjectNode parent,
            String field,
            JsonNode value,
            String path,
            List<AppliedDefault> defaults) {
        if (!parent.has(field)) {
            parent.set(field, value.deepCopy());
            defaults.add(new AppliedDefault(path, value));
        }
    }

    private void findForbiddenDefinitionTemplates(
            Path source,
            JsonNode node,
            String path,
            List<ResolutionIssue> issues) {
        if (node.isTextual() && containsTemplateSyntax(node.textValue())) {
            issues.add(issue(source, ResolutionScope.COMMON, "parameter.recursive-expansion", path,
                    "Parameter declarations may not contain templates"));
            return;
        }
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String fieldPath = path + "/" + escapePointer(entry.getKey());
                if (containsTemplateSyntax(entry.getKey())) {
                    issues.add(issue(source, ResolutionScope.COMMON, "parameter.reference-in-map-key", fieldPath,
                            "Templates are not allowed in map keys"));
                }
                findForbiddenDefinitionTemplates(source, entry.getValue(), fieldPath, issues);
            });
        } else if (node.isArray()) {
            for (int index = 0; index < node.size(); index++) {
                findForbiddenDefinitionTemplates(source, node.get(index), path + "/" + index, issues);
            }
        }
    }

    private static Map<String, JsonNode> readSideOverrides(ObjectNode template, String side) {
        JsonNode node = template.at("/experiment/" + side);
        Map<String, JsonNode> overrides = new LinkedHashMap<>();
        if (node instanceof ObjectNode object) {
            object.fields().forEachRemaining(entry -> overrides.put(entry.getKey(), entry.getValue()));
        }
        return overrides;
    }

    private static boolean containsTemplateSyntax(String value) {
        return value.contains("${");
    }

    private static String lexicalValue(JsonNode value) {
        if (value.isTextual()) {
            return value.textValue();
        }
        if (value.isBoolean()) {
            return Boolean.toString(value.booleanValue());
        }
        if (value.isIntegralNumber()) {
            return value.bigIntegerValue().toString();
        }
        throw new IllegalStateException("Unsupported parameter value " + value);
    }

    private static void addUnknownReference(
            Path source,
            ResolutionScope scope,
            String path,
            String parameter,
            Map<String, EffectiveParameter> effective,
            List<ResolutionIssue> issues) {
        issues.add(issue(source, scope, "parameter.unknown-reference", path,
                "Unknown or unresolved parameter '" + parameter + "'; effective parameters: "
                        + String.join(", ", effective.keySet())));
    }

    private static String bindingPath(String bindingKind, String name) {
        String prefix = switch (bindingKind) {
            case "suite" -> "$/suite-bindings";
            case "submit" -> "$/submit-overrides";
            case "experiment baseline" -> "$/experiment/baseline";
            case "experiment candidate" -> "$/experiment/candidate";
            default -> "$";
        };
        return prefix + "/" + escapePointer(name);
    }

    private static String unknownBindingCode(String bindingKind) {
        return switch (bindingKind) {
            case "suite" -> "parameter.unknown-suite-binding";
            case "submit" -> "parameter.unknown-submit-override";
            default -> "parameter.unknown-side-override";
        };
    }

    private static String variedBindingCode(String bindingKind) {
        return "suite".equals(bindingKind)
                ? "parameter.varied-suite-binding"
                : "parameter.varied-submit-override";
    }

    private static ResolutionScope scopeOf(ScenarioSide side) {
        return switch (side) {
            case SINGLE -> ResolutionScope.SINGLE;
            case BASELINE -> ResolutionScope.BASELINE;
            case CANDIDATE -> ResolutionScope.CANDIDATE;
        };
    }

    private static String escapePointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }

    private static ResolutionIssue issue(
            Path source,
            ResolutionScope scope,
            String code,
            String path,
            String message) {
        return new ResolutionIssue(source, scope, code, path, message);
    }

    private record SideMaterialization(
            ScenarioSide side,
            ObjectNode document,
            Map<String, EffectiveParameter> effectiveParameters,
            Map<String, Set<String>> provenance,
            List<AppliedDefault> appliedDefaults) {}
}
