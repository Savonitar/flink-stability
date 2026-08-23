package org.savonitar.flink.stability.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Defends the differential invariant independently of the resolver's construction logic. */
final class ExperimentDriftValidator {
    List<ResolutionIssue> validate(
            Path source,
            Set<String> varies,
            Map<String, EffectiveParameter> baselineParameters,
            Map<String, EffectiveParameter> candidateParameters,
            ObjectNode baselineDocument,
            ObjectNode candidateDocument,
            Map<String, Set<String>> baselineProvenance,
            Map<String, Set<String>> candidateProvenance) {
        List<ResolutionIssue> issues = new ArrayList<>();
        compareParameters(source, varies, baselineParameters, candidateParameters, issues);
        compareNodes(source, baselineDocument, candidateDocument, "$", varies,
                baselineProvenance, candidateProvenance, issues);
        return List.copyOf(issues);
    }

    private static void compareParameters(
            Path source,
            Set<String> varies,
            Map<String, EffectiveParameter> baseline,
            Map<String, EffectiveParameter> candidate,
            List<ResolutionIssue> issues) {
        Set<String> names = new LinkedHashSet<>();
        names.addAll(baseline.keySet());
        names.addAll(candidate.keySet());
        names.stream().filter(name -> !varies.contains(name)).forEach(name -> {
            if (!Objects.equals(baseline.get(name), candidate.get(name))) {
                issues.add(issue(source, "$/parameters/" + escapePointer(name),
                        "Baseline and candidate effective values differ for non-varied parameter '" + name + "'"));
            }
        });
    }

    private static void compareNodes(
            Path source,
            JsonNode baseline,
            JsonNode candidate,
            String path,
            Set<String> varies,
            Map<String, Set<String>> baselineProvenance,
            Map<String, Set<String>> candidateProvenance,
            List<ResolutionIssue> issues) {
        if (baseline.equals(candidate)) {
            return;
        }
        if (baseline.isObject() && candidate.isObject()) {
            Set<String> fields = new LinkedHashSet<>();
            baseline.fieldNames().forEachRemaining(fields::add);
            candidate.fieldNames().forEachRemaining(fields::add);
            fields.forEach(field -> compareNodes(source, baseline.path(field), candidate.path(field),
                    path + "/" + escapePointer(field), varies,
                    baselineProvenance, candidateProvenance, issues));
            return;
        }
        if (baseline.isArray() && candidate.isArray() && baseline.size() == candidate.size()) {
            for (int index = 0; index < baseline.size(); index++) {
                compareNodes(source, baseline.get(index), candidate.get(index), path + "/" + index, varies,
                        baselineProvenance, candidateProvenance, issues);
            }
            return;
        }

        Set<String> causes = new LinkedHashSet<>();
        causes.addAll(baselineProvenance.getOrDefault(path, Set.of()));
        causes.addAll(candidateProvenance.getOrDefault(path, Set.of()));
        if (causes.stream().noneMatch(varies::contains)) {
            issues.add(issue(source, path, "Baseline and candidate documents differ outside experiment.varies"));
        }
    }

    private static ResolutionIssue issue(Path source, String path, String message) {
        return new ResolutionIssue(source, ResolutionScope.COMMON,
                "experiment.configuration-drift", path, message);
    }

    private static String escapePointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }
}
