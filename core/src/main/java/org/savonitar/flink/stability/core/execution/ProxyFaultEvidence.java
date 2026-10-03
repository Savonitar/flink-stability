package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Maps correlated proxy events without inventing producer fields absent in the request. */
final class ProxyFaultEvidence {
    private ProxyFaultEvidence() {}
    static boolean qualifiesAfterAppend(JsonNode event) {
        try {
            return affected(List.of(event), "response-error-after-append").stream().anyMatch(message ->
                    message.qualifies(org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan.NetworkFaultAction.ERROR_AFTER_APPEND));
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }
    static List<PhaseExecutionEvidence.ProtocolMessage> affected(List<JsonNode> lines, String event) {
        List<PhaseExecutionEvidence.ProtocolMessage> result = new ArrayList<>();
        for (JsonNode line : lines) {
            if (!event.equals(line.path("event").asText()) || !line.has("api")) continue;
            JsonNode request = "response-dropped".equals(event) ? lines.stream()
                    .filter(candidate -> "request-forwarded".equals(candidate.path("event").asText())
                            && candidate.path("claim").intValue() == line.path("claim").intValue())
                    .findFirst().orElseThrow(() -> new IllegalArgumentException("Missing request identity for dropped response")) : line;
            Map<String, Short> codes = new LinkedHashMap<>();
            line.path("originalErrorCodes").fields().forEachRemaining(entry -> codes.put(entry.getKey(), entry.getValue().shortValue()));
            Map<String, Long> offsets = new LinkedHashMap<>();
            line.path("originalBaseOffsets").fields().forEachRemaining(entry -> {
                if (entry.getValue().isIntegralNumber() && entry.getValue().canConvertToLong())
                    offsets.put(entry.getKey(), entry.getValue().longValue());
            });
            result.add(new PhaseExecutionEvidence.ProtocolMessage(line.path("occurrence").intValue(), line.path("claim").intValue(),
                    line.path("timeMillis").longValue(), line.path("beforeDeadline").asBoolean(false), event, request.path("api").asText(),
                    request.path("apiVersion").shortValue(), request.path("correlationId").intValue(), request.path("transactionalId").asText(),
                    request.path("producerId").isNumber() ? request.path("producerId").longValue() : null,
                    request.path("producerEpoch").isNumber() ? request.path("producerEpoch").shortValue() : null,
                    request.path("committed").isBoolean() ? request.path("committed").booleanValue() : null, codes, offsets,
                    line.path("substitutedErrorCode").isNumber() ? line.path("substitutedErrorCode").shortValue() : null,
                    line.path("forwardedToBroker").asBoolean(), line.path("requestedDelayMillis").longValue(), line.path("actualDelayNanos").longValue()));
        }
        return List.copyOf(result);
    }
}
