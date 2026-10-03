package org.savonitar.flink.stability.faultproxy;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A successful correlated append acknowledgement, followed by an uncertain client outcome. */
final class ProduceResponseFault {
    private ProduceResponseFault() {}

    static Set<String> partitions(ApiMessage request) {
        if (!(request instanceof ProduceRequestData produce) || produce.acks() != -1) return Set.of();
        Set<String> partitions = new LinkedHashSet<>();
        for (var topic : produce.topicData()) {
            for (var partition : topic.partitionData()) {
                if (!partitions.add(topic.name() + "/" + partition.index())) return Set.of();
            }
        }
        return Set.copyOf(partitions);
    }

    static Map<String, Long> appendOffsets(ApiMessage response, Set<String> expected) {
        if (!(response instanceof ProduceResponseData produce) || expected.isEmpty()) return Map.of();
        Map<String, Long> offsets = new LinkedHashMap<>();
        for (var topic : produce.responses()) {
            for (var partition : topic.partitionResponses()) {
                if (partition.errorCode() != Errors.NONE.code() || partition.baseOffset() < 0
                        || offsets.putIfAbsent(topic.name() + "/" + partition.index(), partition.baseOffset()) != null)
                    return Map.of();
            }
        }
        return offsets.keySet().equals(expected) ? Map.copyOf(offsets) : Map.of();
    }

    static ProduceResponseData timeout(ProduceResponseData original) {
        ProduceResponseData response = original.duplicate();
        response.responses().forEach(topic -> topic.partitionResponses().forEach(partition ->
                partition.setErrorCode(Errors.REQUEST_TIMED_OUT.code()).setBaseOffset(-1)
                        .setLogAppendTimeMs(-1).setLogStartOffset(-1).setRecordErrors(List.of())
                        .setErrorMessage("Injected timeout after successful append acknowledgement")));
        return response;
    }
}
