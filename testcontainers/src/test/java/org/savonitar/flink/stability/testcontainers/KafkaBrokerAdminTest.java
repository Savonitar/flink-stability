package org.savonitar.flink.stability.testcontainers;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.common.*;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaBrokerAdminTest {
    @Test void pausedAdminRequestConsumesHoldEvenWhenAnotherBrokerAlreadyReportsNewLeader() throws Exception {
        var live = new Fake(); live.leader = 3;
        var pausedRequest = new KafkaFutureImpl<TopicDescription>();
        Admin routedToPaused = (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[]{Admin.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "describeTopics" -> result(DescribeTopicsResult.class, null, Map.of("output", pausedRequest));
                    case "close" -> null;
                    default -> throw new AssertionError(method.getName());
                });
        var partitions = List.of(new KafkaLogCapture.Partition("output", 2));
        try (var healthy = new KafkaBrokerAdmin(live.admin(), live::topics); var old = new KafkaBrokerAdmin(routedToPaused, (names, deadline) -> routedToPaused.describeTopics(names)
                .allTopicNames().get(deadline.remaining().toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS))) {
            assertEquals(3, healthy.leaders(partitions, deadline()).getFirst().leader());
            var hold = MonotonicDeadline.start(Duration.ofMillis(100), System::nanoTime);
            assertThrows(java.util.concurrent.TimeoutException.class, () -> old.leaders(partitions, hold));
            assertTrue(hold.remaining().isZero());
            assertFalse(pausedRequest.isDone(), "Caller timeout neither completes nor cancels the stuck RPC");
        }
    }

    @Test void coordinatorUsesObservedNondefaultPartitionCountAndLatestLeaderRatherThanDescriptionCoordinator() throws Exception {
        var fake = new Fake();
        try (var admin = new KafkaBrokerAdmin(fake.admin(), fake::topics)) {
            var target = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR, null, null, -1, "sink-");
            var first = admin.select(target, deadline());
            assertEquals("sink-a", first.transactionalId()); assertEquals(7, first.partitionCount());
            assertEquals(KafkaBrokerControl.transactionStatePartition("sink-a",7), first.partition());
            assertEquals("broker-2", first.broker()); assertEquals(77L, first.producerId()); assertEquals(4, first.producerEpoch());
            fake.leader = 3; assertEquals("broker-3", admin.select(target, deadline()).broker());
            assertTrue(fake.filteredOngoing);
        }
    }
    @Test void noOpenTransactionClosedTransactionOrUnownedLeaderNeverGuesses() throws Exception {
        for (int failure = 0; failure < 3; failure++) {
            var fake = new Fake(); if (failure==0) fake.empty=true; if (failure==1) fake.state=TransactionState.COMPLETE_COMMIT; if(failure==2)fake.leader=9;
            try (var admin = new KafkaBrokerAdmin(fake.admin(), fake::topics)) {
                assertThrows(IllegalStateException.class, () -> admin.select(new KafkaBrokerControl.Target(
                        KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR,null,null,-1,"sink-"),deadline()));
            }
        }
    }
    @Test void partitionTargetReadsMetadataAtEachSelection() throws Exception {
        var fake = new Fake();
        try(var admin = new KafkaBrokerAdmin(fake.admin(), fake::topics)) {
            var target = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.PARTITION_LEADER,null,"output",2,null);
            assertEquals(2,admin.select(target,deadline()).leader());fake.leader=1;
            assertEquals(1,admin.select(target,deadline()).leader());
        }
    }
    @Test void readsSessionTimeoutFromBrokerMetadataAndExactTransaction() throws Exception {
        var fake = new Fake();
        try (var admin = new KafkaBrokerAdmin(fake.admin(), fake::topics)) {
            assertEquals(17000, admin.sessionTimeoutMillis(2, deadline()));
            var state = admin.transaction("sink-a", deadline());
            assertEquals("ONGOING", state.state()); assertEquals(77, state.producerId()); assertEquals(4, state.producerEpoch());
        }
    }
    @Test void faultClientsBoundNetworkRequestsWithoutShorteningTheOperationTimeout() {
        var config = KafkaBrokerAdmin.properties("localhost:9092", Duration.ofSeconds(120));
        assertEquals(120_000, config.get("default.api.timeout.ms"));
        assertEquals(1_000, config.get("request.timeout.ms"));
        assertEquals(50, KafkaBrokerAdmin.properties("localhost:9092", Duration.ofMillis(50)).get("request.timeout.ms"));
    }
    @Test void stalledCoordinatorObservationLeavesBudgetForTheCommitObserverToRetry() throws Exception {
        var pending = new KafkaFutureImpl<TransactionDescription>();
        Admin fake = (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[]{Admin.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "describeTransactions" -> {
                        assertTrue(((DescribeTransactionsOptions) args[1]).timeoutMs() <= 100);
                        yield result(DescribeTransactionsResult.class, Map.of(CoordinatorKey.byTransactionalId("sink-a"), pending));
                    }
                    case "close" -> null;
                    default -> throw new AssertionError(method.getName());
                });
        try (var admin = new KafkaBrokerAdmin(fake, (names, deadline) -> { throw new AssertionError(); })) {
            var hold = MonotonicDeadline.start(Duration.ofMillis(200), System::nanoTime);
            assertThrows(java.util.concurrent.TimeoutException.class, () -> admin.transaction("sink-a", hold));
            assertFalse(hold.remaining().isZero());
            pending.complete(new TransactionDescription(3, TransactionState.ONGOING, 77, 4, 7200000,
                    OptionalLong.of(1), Set.of(new TopicPartition("output",0))));
            assertEquals(3, admin.transaction("sink-a", hold).coordinatorId());
        }
    }
    static MonotonicDeadline deadline(){return MonotonicDeadline.start(Duration.ofSeconds(1),System::nanoTime);}
    static Object result(Class<?> type, Object... args) throws Exception {
        var ctor = type.getDeclaredConstructors()[0]; ctor.setAccessible(true); return ctor.newInstance(args);
    }
    static class Fake {
        int leader=2; boolean empty, filteredOngoing; TransactionState state=TransactionState.ONGOING;
        Map<String, TopicDescription> topics(List<String> names, MonotonicDeadline deadline) throws Exception {
            return admin().describeTopics(names).allTopicNames().get(deadline.remaining().toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
        }
        Admin admin() {
            return (Admin)Proxy.newProxyInstance(Admin.class.getClassLoader(),new Class<?>[]{Admin.class},(proxy,method,args)->{
                return switch(method.getName()) {
                    case "describeConfigs" -> {
                        var resource = new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.BROKER, "2");
                        assertEquals(List.of(resource), args[0]);
                        yield result(DescribeConfigsResult.class, Map.of(resource, KafkaFuture.completedFuture(new Config(List.of(new ConfigEntry("broker.session.timeout.ms", "17000"))))));
                    }
                    case "listTransactions" -> {
                        filteredOngoing=((ListTransactionsOptions)args[0]).filteredStates().equals(Set.of(TransactionState.ONGOING));
                        var future=new KafkaFutureImpl<Collection<TransactionListing>>();
                        future.complete(empty?List.of():List.of(new TransactionListing("other",1,TransactionState.ONGOING),
                                new TransactionListing("sink-z",78,TransactionState.ONGOING),new TransactionListing("sink-a",77,TransactionState.ONGOING)));
                        yield result(ListTransactionsResult.class,KafkaFuture.completedFuture(Map.of(1,future)));
                    }
                    case "describeTransactions" -> result(DescribeTransactionsResult.class,Map.of(CoordinatorKey.byTransactionalId("sink-a"),
                            KafkaFuture.completedFuture(new TransactionDescription(1,state,77,4,7200000,OptionalLong.of(1),Set.of(new TopicPartition("output",0))))));
                    case "describeTopics" -> {
                        Map<String,KafkaFuture<TopicDescription>> topics=new HashMap<>();
                        for(String topic:List.of("output","__transaction_state")) {
                            var nodes=List.of(new Node(1,"one",9092),new Node(2,"two",9092),new Node(3,"three",9092));
                            var partitions=java.util.stream.IntStream.range(0,7).mapToObj(id->new TopicPartitionInfo(id,
                                    new Node(leader,"host",9092),nodes,nodes)).toList();
                            topics.put(topic,KafkaFuture.completedFuture(new TopicDescription(topic,false,partitions)));
                        }
                        yield result(DescribeTopicsResult.class,null,topics);
                    }
                    case "close" -> null;
                    default -> throw new AssertionError(method.getName());
                };
            });
        }
    }
}
