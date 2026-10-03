package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightValidator.ClusterIndex;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightValidator.ProxyIndex;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightValidator.KafkaEndpoint;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightValidator.SideIndex;
import static org.savonitar.flink.stability.core.spec.resolution.ScenarioPreflightValidator.issue;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ExpectedResultSpecification;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;
import org.savonitar.flink.stability.core.spec.document.SpecificationException.Stage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Proxy route, protocol selector and API/error compatibility checks. */
final class NetworkFaultPreflightValidator {
    private NetworkFaultPreflightValidator() {}
    static void validate(
            Path source,
            ResolutionScope scope,
            SideIndex index,
            ObjectNode networkFault,
            String path,
            List<Diagnostic> issues) {
        String proxyAlias = networkFault.path("proxy").textValue();
        ProxyIndex proxy = index.proxies().get(proxyAlias);
        if (proxy == null) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "preflight.reference.proxy-not-found",
                    path + "/proxy",
                    "Network fault references undeclared proxy '" + proxyAlias + "'"));
            return;
        }

        ObjectNode target = (ObjectNode) networkFault.get("target");
        String targetCluster = target.path("cluster").textValue();
        ClusterIndex cluster = index.clusters().get(targetCluster);
        if (cluster == null) {
            issues.add(issue(source, scope,
                    "preflight.reference.kafka-cluster-not-found",
                    path + "/target/cluster",
                    "Network fault targets undeclared Kafka cluster '"
                            + targetCluster + "'"));
            return;
        }
        if (!index.clusters().containsKey(proxy.cluster())) {
            return;
        }
        if (!targetCluster.equals(proxy.cluster())) {
            issues.add(issue(source, scope,
                    "preflight.network.proxy-target-cluster-mismatch",
                    path + "/target/cluster",
                    "Network fault targets cluster '" + targetCluster + "' but proxy '"
                            + proxyAlias + "' is attached to cluster '" + proxy.cluster() + "'"));
            return;
        }
        if (target.has("broker")
                && !networkBrokerExists(cluster, target.path("broker").textValue())) {
            issues.add(issue(source, scope,
                    "preflight.network.broker-not-found",
                    path + "/target/broker",
                    "Kafka cluster '" + targetCluster + "' has no broker target named '"
                            + target.path("broker").textValue() + "'; available names are "
                            + "broker-1 through broker-" + cluster.brokers()));
            return;
        }

        ObjectNode match = (ObjectNode) networkFault.get("match");
        String api = match.path("api").textValue();
        KafkaFaultApiRegistry.Profile profile = KafkaFaultApiRegistry.profile(api);
        if (profile == null) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "capability.network-api.unsupported",
                    path + "/match/api",
                    "Harness does not support Kafka fault API '" + api + "'"));
            return;
        }

        String topic = optionalText(match, "topic");
        String transactionalIdPrefix = optionalText(match, "transactional_id_prefix");
        boolean selectorValid = true;
        if (topic != null
                && profile.topicBinding() == KafkaFaultApiRegistry.TopicBinding.FORBIDDEN) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "preflight.network.topic-selector-unsupported",
                    path + "/match/topic",
                    "Kafka API '" + api + "' has no safely matchable topic field in v1"));
            selectorValid = false;
        } else if (topic != null && !cluster.topics().containsKey(topic)) {
            issues.add(issue(source, scope,
                    "preflight.reference.kafka-topic-not-found",
                    path + "/match/topic",
                    "Network fault references undeclared topic '" + topic
                            + "' in Kafka cluster '" + targetCluster + "'"));
            selectorValid = false;
        }
        if (transactionalIdPrefix != null && !profile.transactional() && !"produce".equals(api)) {
            issues.add(issue(source, ResolutionScope.COMMON,
                    "preflight.network.transactional-prefix-unsupported",
                    path + "/match/transactional_id_prefix",
                    "Kafka API '" + api
                            + "' cannot be selected by transactional ID prefix"));
            selectorValid = false;
        }

        ObjectNode fault = (ObjectNode) networkFault.get("fault");
        if ("error-response".equals(fault.path("type").textValue())) {
            String error = fault.path("error").textValue();
            if (!profile.allowedErrors().contains(error)) {
                issues.add(issue(source, ResolutionScope.COMMON,
                        "preflight.network.error-response-unsupported",
                        path + "/fault/error",
                        "Kafka API '" + api + "' cannot safely return error '" + error
                                + "' in v1; allowed errors: "
                                + profile.allowedErrors().stream().sorted().toList()));
                selectorValid = false;
            }
        }
        if (!selectorValid) {
            return;
        }

        List<KafkaEndpoint> logicalCandidates = index.endpoints().stream()
                .filter(endpoint -> matchesNetworkSelector(
                        profile,
                        endpoint,
                        targetCluster,
                        topic,
                        transactionalIdPrefix))
                .toList();
        List<KafkaEndpoint> routedCandidates = logicalCandidates.stream()
                .filter(endpoint -> proxyAlias.equals(endpoint.proxy()))
                .toList();
        if (!routedCandidates.isEmpty()) {
            return;
        }

        if (!logicalCandidates.isEmpty()) {
            List<String> routes = logicalCandidates.stream()
                    .map(NetworkFaultPreflightValidator::describeRoute)
                    .sorted()
                    .toList();
            issues.add(issue(source, scope,
                    "preflight.network.endpoint-bypasses-proxy",
                    path + "/match",
                    "Matching Kafka endpoints bypass proxy '" + proxyAlias
                            + "' or use another proxy: " + routes));
            return;
        }

        boolean proxyHasRoutedEndpoint = index.endpoints().stream().anyMatch(endpoint ->
                endpoint.cluster().equals(targetCluster)
                        && proxyAlias.equals(endpoint.proxy()));
        if (proxyHasRoutedEndpoint) {
            issues.add(issue(source, scope,
                    "preflight.network.endpoint-not-found",
                    path + "/match",
                    "No routed endpoint matches Kafka API '" + api + "'"
                            + selectorSuffix(topic, transactionalIdPrefix)));
        } else {
            issues.add(issue(source, scope,
                    "preflight.network.proxy-has-no-routed-endpoint",
                    path + "/proxy",
                    "No source, sink, or input-source endpoint on cluster '"
                            + targetCluster + "' routes through proxy '" + proxyAlias + "'"));
        }
    }

    private static boolean matchesNetworkSelector(
            KafkaFaultApiRegistry.Profile profile,
            KafkaEndpoint endpoint,
            String targetCluster,
            String topic,
            String transactionalIdPrefix) {
        if (!profile.endpointKinds().contains(endpoint.kind())
                || !endpoint.cluster().equals(targetCluster)) {
            return false;
        }
        if (profile.transactional()
                && (!"EXACTLY_ONCE".equals(endpoint.deliveryGuarantee())
                        || endpoint.kind() != KafkaFaultApiRegistry.EndpointKind.SINK)) {
            return false;
        }
        if (transactionalIdPrefix != null
                && !transactionalIdPrefix.equals(endpoint.transactionalIdPrefix())) {
            return false;
        }
        return switch (profile.topicBinding()) {
            case ENDPOINT -> topic == null || topic.equals(endpoint.topic());
            case SOURCE -> endpoint.sourceCluster().equals(targetCluster)
                    && (topic == null || topic.equals(endpoint.sourceTopic()));
            case FORBIDDEN -> true;
        };
    }

    private static boolean networkBrokerExists(ClusterIndex cluster, String name) {
        Matcher matcher = ProcessTargetPreflightValidator.STATIC_TARGET_NAME.matcher(name);
        return matcher.matches()
                && matcher.group(1).equals("broker")
                && new BigInteger(matcher.group(2)).compareTo(cluster.brokers()) <= 0;
    }

    static String canonicalAddress(String address) {
        int separator = address.lastIndexOf(':');
        String host = address.substring(0, separator).toLowerCase(Locale.ROOT);
        while (host.endsWith(".")) {
            host = host.substring(0, host.length() - 1);
        }
        return host + address.substring(separator);
    }

    private static String describeRoute(KafkaEndpoint endpoint) {
        return endpoint.path() + " ("
                + (endpoint.proxy() == null ? "direct" : "proxy=" + endpoint.proxy())
                + ")";
    }

    private static String selectorSuffix(String topic, String transactionalIdPrefix) {
        List<String> selectors = new ArrayList<>();
        if (topic != null) {
            selectors.add("topic=" + topic);
        }
        if (transactionalIdPrefix != null) {
            selectors.add("transactional_id_prefix=" + transactionalIdPrefix);
        }
        return selectors.isEmpty() ? "" : " with " + String.join(", ", selectors);
    }

    private static String optionalText(ObjectNode object, String field) {
        JsonNode value = object.get(field);
        return value == null ? null : value.textValue();
    }
}
