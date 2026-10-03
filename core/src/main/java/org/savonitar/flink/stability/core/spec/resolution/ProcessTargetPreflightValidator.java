package org.savonitar.flink.stability.core.spec.resolution;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.*;
import java.nio.file.Path;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.*;
import static org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightValidator.issue;

/** Static process names and broker selectors' declaration dependencies. */
final class ProcessTargetPreflightValidator {
    static final Pattern STATIC_TARGET_NAME = Pattern.compile("^(taskmanager|jobmanager|broker)-([1-9][0-9]*)$");
    static void validate(
            Path source,
            ResolutionScope scope,
            Map<String, BigInteger> clusters, BigInteger taskmanagers, BigInteger jobmanagers,
            ObjectNode target,
            String path,
            List<Diagnostic> issues) {
        if ("selector".equals(target.path("kind").textValue())) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "capability.runtime-target-selector.unsupported",
                    path + "/kind",
                    "v1 supports only statically named process targets"));
            return;
        }

        String role = target.path("role").textValue();
        String name = target.path("name").textValue();
        Matcher matcher = STATIC_TARGET_NAME.matcher(name);
        boolean matchesRole = matcher.matches() && matcher.group(1).equals(role);
        BigInteger ordinal = matchesRole
                ? new BigInteger(matcher.group(2))
                : BigInteger.ZERO;
        if (role.equals("broker") && matchesRole) {
            List<String> matchingClusters = clusters.entrySet().stream()
                    .filter(entry -> ordinal.compareTo(entry.getValue()) <= 0)
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
            if (matchingClusters.size() == 1) {
                return;
            }
            if (matchingClusters.size() > 1) {
                issues.add(issue(source, scope,
                        "preflight.target.broker-cluster-ambiguous",
                        path + "/name",
                        "Named broker target '" + name
                                + "' exists in Kafka clusters " + matchingClusters));
                return;
            }
        }

        BigInteger upperBound = switch (role) {
            case "taskmanager" -> taskmanagers;
            case "jobmanager" -> jobmanagers;
            case "broker" -> clusters.values().stream()
                    .max(BigInteger::compareTo)
                    .orElse(BigInteger.ZERO);
            default -> BigInteger.ZERO;
        };
        if (!matchesRole || ordinal.compareTo(upperBound) > 0) {
            issues.add(issue(source, scope,
                    "preflight.target.named-not-found",
                    path + "/name",
                    "No declared " + role + " target named '" + name
                            + "'; available names are " + role + "-1 through "
                            + role + "-" + upperBound));
        }
    }


    static void brokerFaults(Path source, ResolutionScope scope, ObjectNode document, List<Diagnostic> issues) {
        for (int i = 0; i < document.path("phases").size(); i++)
            brokerSteps(source, scope, document.path("phases").get(i).path("steps"), "$/phases/" + i + "/steps", document, issues);
    }
    private static void brokerSteps(Path source, ResolutionScope scope, JsonNode steps, String path,
                                    ObjectNode document, List<Diagnostic> issues) {
        for (int i = 0; i < steps.size(); i++) {
            var step = steps.get(i); var location = path + "/" + i;
            if (step.has("loop")) brokerSteps(source, scope, step.at("/loop/steps"), location + "/loop/steps", document, issues);
            if (!step.has("broker_fault") && !step.has("packet_fault")) continue;
            String key = step.has("packet_fault") ? "packet_fault" : "broker_fault";
            var target = step.path(key).path("target"); String cluster = target.path("cluster").asText();
            var clusters = document.at("/setup/kafka/clusters"); var topology = clusters.path(cluster);
            String error = null;
            if (clusters.size() != 1 || topology.path("brokers").asInt() != 3) error = "Broker faults require the declared three-broker cluster";
            else if ("partition-leader".equals(target.path("type").asText())) {
                boolean found = false;
                for (var topic : topology.path("topics")) if (topic.path("name").asText().equals(target.path("topic").asText()))
                    found = target.path("partition").asInt(-1) >= 0 && target.path("partition").asInt() < topic.path("partitions").asInt();
                if (!found) error = "Selector must name a declared topic partition";
            } else if ("transaction-coordinator".equals(target.path("type").asText())) {
                boolean found = false;
                for (var job : document.at("/workload/jobs")) if (job.path("alias").asText().equals(target.path("job").asText()))
                    found = cluster.equals(job.at("/sink/cluster").asText()) && "EXACTLY_ONCE".equals(job.at("/sink/delivery_guarantee").asText())
                            && !job.at("/sink/transactional_id_prefix").asText().isBlank();
                if (!found) error = "Coordinator selector requires an exactly-once sink in the target cluster";
            }
            if (error != null) issues.add(issue(source, scope, "preflight.broker-fault.target-invalid", location + "/" + key + "/target", error));
        }
    }
}
