package org.savonitar.flink.stability.core.execution.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.spec.document.Diagnostic;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.issue;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.parseDuration;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.pointer;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.requireDuration;
import static org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlanCompiler.requirePositiveInt;

/**
 * Checks and maps Kafka proxies and network faults for the first runner (SPEC-004 §11): one
 * Kroxylicious proxy in front of the one cluster, the job sink as its only routed client, and
 * counted drops of EndTxn requests or responses.
 */
final class NetworkFaultCompiler {
    private static final Set<String> ACTIONS = Set.of("drop-request", "drop-response");
    /** A Docker network alias and a port that leaves room for broker ports above it. */
    private static final Pattern LISTEN = Pattern.compile(
            "^([a-z0-9]+(?:-[a-z0-9]+)*):([1-9][0-9]{0,4})$");
    private static final int BROKER_PORTS = 10;
    /** Aliases the runner already gives its own containers. */
    private static final Set<String> RESERVED_HOSTS = Set.of("jobmanager-1", "taskmanager-1");

    private NetworkFaultCompiler() {}

    /** The proxy declarations the runner can provision. */
    static void validateProxies(Path source, ObjectNode document, List<Diagnostic> issues) {
        JsonNode proxies = document.at("/setup/proxies");
        if (!proxies.isObject() || proxies.isEmpty()) {
            return;
        }
        if (proxies.size() != 1) {
            issues.add(issue(source, "runner.kafka.proxy-count-unsupported", "$/setup/proxies",
                    "The first runner provisions at most one Kafka proxy, found "
                            + proxies.size()));
            return;
        }
        Map.Entry<String, JsonNode> entry = proxies.fields().next();
        String path = "$/setup/proxies/" + pointer(entry.getKey());
        ObjectNode proxy = (ObjectNode) entry.getValue();
        if (!proxy.path("bootstrap").has("cluster")) {
            issues.add(issue(source, "runner.kafka.proxy-bootstrap-unsupported",
                    path + "/bootstrap",
                    "The first runner proxies a declared cluster, not an explicit address"));
        }
        Matcher listen = LISTEN.matcher(proxy.path("listen").asText());
        String clusterHost = "kafka-" + proxy.path("cluster").asText();
        if (!listen.matches()
                || Integer.parseInt(listen.group(2)) > 65_535 - BROKER_PORTS
                || RESERVED_HOSTS.contains(listen.group(1))
                || clusterHost.equals(listen.group(1))) {
            issues.add(issue(source, "runner.kafka.proxy-listen-unsupported", path + "/listen",
                    "The proxy must listen on <lower-kebab-host>:<port>, with a host no runner"
                            + " container uses and " + BROKER_PORTS
                            + " free ports above the bootstrap port for brokers"));
        }
    }

    /** One network fault step the runner can inject. */
    static void validateStep(
            Path source,
            ObjectNode networkFault,
            String path,
            boolean inLoop,
            List<Diagnostic> issues) {
        if (inLoop) {
            issues.add(issue(source, "runner.network-fault.loop-unsupported", path,
                    "The first runner does not repeat network faults inside a loop"));
        }
        if (!"end-txn".equals(networkFault.at("/match/api").textValue())) {
            issues.add(issue(source, "runner.network-fault.api-unsupported",
                    path + "/match/api", "The first runner faults only end-txn traffic"));
        }
        if (!ACTIONS.contains(networkFault.at("/fault/type").textValue())) {
            issues.add(issue(source, "runner.network-fault.type-unsupported",
                    path + "/fault/type",
                    "The first runner executes only drop-request and drop-response faults"));
            return;
        }
        requirePositiveInt(source, networkFault.path("occurrences"),
                path + "/occurrences", issues);
        requireDuration(source, networkFault.path("trigger_deadline"),
                path + "/trigger_deadline", issues);
    }

    static Optional<ExecutableScenarioPlan.KafkaProxy> proxy(ObjectNode document) {
        JsonNode proxies = document.at("/setup/proxies");
        if (!proxies.isObject() || proxies.isEmpty()) {
            return Optional.empty();
        }
        Map.Entry<String, JsonNode> entry = proxies.fields().next();
        Matcher listen = LISTEN.matcher(entry.getValue().path("listen").asText());
        if (!listen.matches()) {
            throw new IllegalStateException("Proxy listen address escaped capability validation");
        }
        return Optional.of(new ExecutableScenarioPlan.KafkaProxy(
                entry.getKey(), listen.group(1), Integer.parseInt(listen.group(2))));
    }

    static ExecutableScenarioPlan.EndTxnFault step(ObjectNode networkFault) {
        JsonNode match = networkFault.get("match");
        return new ExecutableScenarioPlan.EndTxnFault(
                networkFault.path("proxy").textValue(),
                match.has("result")
                        ? Optional.of(ExecutableScenarioPlan.TransactionResult.valueOf(
                                match.path("result").textValue().toUpperCase(Locale.ROOT)))
                        : Optional.empty(),
                Optional.ofNullable(match.path("transactional_id_prefix").textValue()),
                ExecutableScenarioPlan.NetworkFaultAction.valueOf(
                        networkFault.at("/fault/type").textValue()
                                .toUpperCase(Locale.ROOT).replace('-', '_')),
                networkFault.path("occurrences").intValue(),
                parseDuration(networkFault.path("trigger_deadline").textValue()));
    }
}
