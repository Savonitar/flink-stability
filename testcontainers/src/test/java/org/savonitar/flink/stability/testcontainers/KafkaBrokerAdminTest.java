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
    @Test void coordinatorUsesObservedNondefaultPartitionCountAndLatestLeaderRatherThanDescriptionCoordinator() throws Exception {
        var fake = new Fake();
        try (var admin = new KafkaBrokerAdmin(fake.admin())) {
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
            try (var admin = new KafkaBrokerAdmin(fake.admin())) {
                assertThrows(IllegalStateException.class, () -> admin.select(new KafkaBrokerControl.Target(
                        KafkaBrokerControl.TargetKind.TRANSACTION_COORDINATOR,null,null,-1,"sink-"),deadline()));
            }
        }
    }
    @Test void partitionTargetReadsMetadataAtEachSelection() throws Exception {
        var fake = new Fake();
        try(var admin = new KafkaBrokerAdmin(fake.admin())) {
            var target = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.PARTITION_LEADER,null,"output",2,null);
            assertEquals(2,admin.select(target,deadline()).leader());fake.leader=1;
            assertEquals(1,admin.select(target,deadline()).leader());
        }
    }
    static MonotonicDeadline deadline(){return MonotonicDeadline.start(Duration.ofSeconds(1),System::nanoTime);}
    static Object result(Class<?> type, Object... args) throws Exception {
        var ctor = type.getDeclaredConstructors()[0]; ctor.setAccessible(true); return ctor.newInstance(args);
    }
    static class Fake {
        int leader=2; boolean empty, filteredOngoing; TransactionState state=TransactionState.ONGOING;
        Admin admin() {
            return (Admin)Proxy.newProxyInstance(Admin.class.getClassLoader(),new Class<?>[]{Admin.class},(proxy,method,args)->{
                return switch(method.getName()) {
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
