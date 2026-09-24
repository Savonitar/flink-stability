package org.savonitar.flink.stability.core.spec.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;

/** Validates harness-owned capability values after parameter interpolation. */
final class ScenarioCapabilityValidator {
    private static final Set<String> KAFKA_MODES = Set.of("kraft");
    private static final Set<String> STATE_BACKENDS = Set.of("hashmap", "rocksdb");
    private static final Set<String> DELIVERY_GUARANTEES =
            Set.of("NONE", "AT_LEAST_ONCE", "EXACTLY_ONCE");
    private static final Set<String> TRANSACTION_ID_NAMING_STRATEGIES =
            Set.of("INCREMENTING", "POOLING");
    private static final Set<String> RESTORE_MODES = Set.of("claim", "no-claim");

    List<Diagnostic> validate(Path source, ResolutionScope scope, ObjectNode document) {
        List<Diagnostic> issues = new ArrayList<>();

        JsonNode clusters = document.at("/setup/kafka/clusters");
        if (clusters instanceof ObjectNode clusterObject) {
            clusterObject.fields().forEachRemaining(entry -> validateTextCapability(
                    source,
                    scope,
                    entry.getValue().get("mode"),
                    "$/setup/kafka/clusters/" + escapePointer(entry.getKey()) + "/mode",
                    "Kafka mode",
                    "capability.kafka-mode.unsupported",
                    KAFKA_MODES,
                    issues));
        }

        JsonNode jobmanagers = document.at("/setup/flink/jobmanagers");
        if (jobmanagers.isIntegralNumber()
                && jobmanagers.bigIntegerValue().compareTo(BigInteger.ONE) > 0) {
            issues.add(issue(source, scope, "capability.multiple-jobmanagers.unsupported",
                    "$/setup/flink/jobmanagers",
                    "v1 supports exactly one JobManager, not " + jobmanagers));
        }

        JsonNode jobs = document.at("/workload/jobs");
        if (jobs instanceof ArrayNode jobArray) {
            for (int index = 0; index < jobArray.size(); index++) {
                JsonNode job = jobArray.get(index);
                String jobPath = "$/workload/jobs/" + index;
                validateTextCapability(source, scope, job.get("state_backend"),
                        jobPath + "/state_backend", "state backend",
                        "capability.state-backend.unsupported", STATE_BACKENDS, issues);
                JsonNode sink = job.get("sink");
                if (sink != null) {
                    validateTextCapability(source, scope, sink.get("delivery_guarantee"),
                            jobPath + "/sink/delivery_guarantee", "delivery guarantee",
                            "capability.delivery-guarantee.unsupported", DELIVERY_GUARANTEES, issues);
                    validateTextCapability(source, scope, sink.get("transaction_id_naming_strategy"),
                            jobPath + "/sink/transaction_id_naming_strategy",
                            "transaction ID naming strategy",
                            "capability.transaction-id-naming-strategy.unsupported",
                            TRANSACTION_ID_NAMING_STRATEGIES, issues);
                }
            }
        }

        validateRestoreModes(source, scope, document.get("phases"), "$/phases", issues);
        return List.copyOf(issues);
    }

    private static void validateRestoreModes(
            Path source,
            ResolutionScope scope,
            JsonNode node,
            String path,
            List<Diagnostic> issues) {
        if (node == null) {
            return;
        }
        if (node instanceof ObjectNode object) {
            JsonNode restore = object.get("restore");
            if (restore instanceof ObjectNode restoreObject) {
                validateTextCapability(source, scope, restoreObject.get("mode"), path + "/restore/mode",
                        "restore mode", "capability.restore-mode.unsupported", RESTORE_MODES, issues);
            }
            object.fields().forEachRemaining(entry -> validateRestoreModes(
                    source, scope, entry.getValue(), path + "/" + escapePointer(entry.getKey()), issues));
        } else if (node instanceof ArrayNode array) {
            for (int index = 0; index < array.size(); index++) {
                validateRestoreModes(source, scope, array.get(index), path + "/" + index, issues);
            }
        }
    }

    private static void validateTextCapability(
            Path source,
            ResolutionScope scope,
            JsonNode value,
            String path,
            String label,
            String code,
            Set<String> supported,
            List<Diagnostic> issues) {
        if (value != null && value.isTextual() && !supported.contains(value.textValue())) {
            issues.add(issue(source, scope, code, path,
                    "Harness does not support " + label + " '" + value.textValue()
                            + "'; supported values: " + String.join(", ", supported.stream().sorted().toList())));
        }
    }

    private static Diagnostic issue(
            Path source,
            ResolutionScope scope,
            String code,
            String path,
            String message) {
        return new Diagnostic(source, scope, code, path, message);
    }

    private static String escapePointer(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }
}
