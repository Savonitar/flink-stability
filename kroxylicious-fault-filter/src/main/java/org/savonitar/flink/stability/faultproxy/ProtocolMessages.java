package org.savonitar.flink.stability.faultproxy;

import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.Records;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Wire fields only; ambiguous/batched transactional identities are never guessed. */
final class ProtocolMessages {
    private ProtocolMessages() {}
    static Optional<FaultRuleBook.RequestIdentity> identity(short version, ApiMessage request) {
        FaultRuleBook.RequestIdentity identity = switch (request) {
            case EndTxnRequestData value -> new FaultRuleBook.RequestIdentity("end-txn", value.transactionalId(), value.producerId(), value.producerEpoch(), value.committed(), Set.of());
            case InitProducerIdRequestData value -> new FaultRuleBook.RequestIdentity("init-producer-id", value.transactionalId(),
                    version >= 3 ? value.producerId() : null, version >= 3 ? value.producerEpoch() : null, null, Set.of());
            case AddOffsetsToTxnRequestData value -> new FaultRuleBook.RequestIdentity("add-offsets-to-txn", value.transactionalId(), value.producerId(), value.producerEpoch(), null, Set.of());
            case TxnOffsetCommitRequestData value -> new FaultRuleBook.RequestIdentity("txn-offset-commit", value.transactionalId(), value.producerId(), value.producerEpoch(), null,
                    value.topics().stream().map(TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic::name).collect(java.util.stream.Collectors.toSet()));
            case FindCoordinatorRequestData value -> value.keyType() != 1 || (version >= 4 && value.coordinatorKeys().size() != 1) ? null
                    : new FaultRuleBook.RequestIdentity("find-coordinator", version >= 4 ? value.coordinatorKeys().getFirst() : value.key(), null, null, null, Set.of());
            case AddPartitionsToTxnRequestData value -> partitions(version, value);
            case ProduceRequestData value -> produce(version, value);
            case DescribeProducersRequestData value -> version != 0 || value.topics().isEmpty()
                    || value.topics().stream().anyMatch(t -> t.name().isBlank() || t.partitionIndexes().isEmpty()
                        || t.partitionIndexes().stream().anyMatch(p -> p < 0)) ? null
                    : new FaultRuleBook.RequestIdentity("describe-producers", null, null, null, null,
                        value.topics().stream().map(DescribeProducersRequestData.TopicRequest::name).collect(java.util.stream.Collectors.toSet()));
            case ListTransactionsRequestData value -> version < 0 || version > 1 || !value.stateFilters().equals(java.util.List.of("Ongoing"))
                    || value.producerIdFilters().isEmpty() || value.producerIdFilters().stream().anyMatch(id -> id < 0) ? null
                    : new FaultRuleBook.RequestIdentity("list-transactions", null, null, null, null, Set.of());
            default -> null;
        };
        return identity == null || (!Set.of("describe-producers", "list-transactions").contains(identity.api())
                && (identity.transactionalId() == null || identity.transactionalId().isBlank())) ? Optional.empty() : Optional.of(identity);
    }

    private static FaultRuleBook.RequestIdentity partitions(short version, AddPartitionsToTxnRequestData value) {
        if (version <= 3) return new FaultRuleBook.RequestIdentity("add-partitions-to-txn", value.v3AndBelowTransactionalId(),
                value.v3AndBelowProducerId(), value.v3AndBelowProducerEpoch(), null,
                value.v3AndBelowTopics().stream().map(AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic::name).collect(java.util.stream.Collectors.toSet()));
        if (value.transactions().size() != 1) return null;
        var transaction = value.transactions().iterator().next();
        return new FaultRuleBook.RequestIdentity("add-partitions-to-txn", transaction.transactionalId(), transaction.producerId(), transaction.producerEpoch(), null,
                transaction.topics().stream().map(AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic::name).collect(java.util.stream.Collectors.toSet()));
    }

    private static FaultRuleBook.RequestIdentity produce(short version, ProduceRequestData value) {
        // v13 uses topic UUIDs; resolving names would require a separate observed metadata binding.
        if (version > 12 || value.acks() == 0 || value.transactionalId() == null) return null;
        Long producerId = null; Short epoch = null; Set<String> topics = new LinkedHashSet<>();
        for (var topic : value.topicData()) {
            topics.add(topic.name());
            for (var partition : topic.partitionData()) {
                if (!(partition.records() instanceof Records records)) return null;
                for (var batch : records.batches()) {
                    if (!batch.isTransactional() || batch.producerId() < 0) return null;
                    if (producerId != null && (producerId != batch.producerId() || epoch != batch.producerEpoch())) return null;
                    producerId = batch.producerId(); epoch = batch.producerEpoch();
                }
            }
        }
        return producerId == null ? null : new FaultRuleBook.RequestIdentity("produce", value.transactionalId(), producerId, epoch, null, topics);
    }

    static Map<String, Short> errors(short version, ApiMessage response) {
        Map<String, Short> errors = new LinkedHashMap<>();
        switch (response) {
            case DescribeProducersResponseData value -> value.topics().forEach(topic -> topic.partitions().forEach(partition ->
                    errors.put(topic.name() + "/" + partition.partitionIndex(), partition.errorCode())));
            case ListTransactionsResponseData value -> {
                errors.put("response", value.errorCode());
                value.unknownStateFilters().forEach(state -> errors.put("unknown-state/" + state,
                        org.apache.kafka.common.protocol.Errors.INVALID_REQUEST.code()));
            }
            case EndTxnResponseData value -> errors.put("response", value.errorCode());
            case InitProducerIdResponseData value -> errors.put("response", value.errorCode());
            case AddOffsetsToTxnResponseData value -> errors.put("response", value.errorCode());
            case ProduceResponseData value -> value.responses().forEach(topic -> topic.partitionResponses().forEach(partition ->
                    errors.put(topic.name() + "/" + partition.index(), partition.errorCode())));
            case TxnOffsetCommitResponseData value -> value.topics().forEach(topic -> topic.partitions().forEach(partition ->
                    errors.put(topic.name() + "/" + partition.partitionIndex(), partition.errorCode())));
            case FindCoordinatorResponseData value -> {
                if (version < 4) errors.put("response", value.errorCode());
                else value.coordinators().forEach(coordinator -> errors.put(coordinator.key(), coordinator.errorCode()));
            }
            case AddPartitionsToTxnResponseData value -> {
                if (version >= 4) {
                    errors.put("response", value.errorCode());
                    value.resultsByTransaction().forEach(transaction -> partitionErrors(errors, transaction.transactionalId() + "/", transaction.topicResults()));
                } else partitionErrors(errors, "", value.resultsByTopicV3AndBelow());
            }
            default -> { }
        }
        return Map.copyOf(errors);
    }

    private static void partitionErrors(Map<String, Short> result, String prefix,
            AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection topics) {
        topics.forEach(topic -> topic.resultsByPartition().forEach(partition ->
                result.put(prefix + topic.name() + "/" + partition.partitionIndex(), partition.partitionErrorCode())));
    }
}
