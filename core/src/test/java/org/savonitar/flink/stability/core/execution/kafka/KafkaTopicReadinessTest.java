package org.savonitar.flink.stability.core.execution.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class KafkaTopicReadinessTest {
    private static final KafkaInputOperations.TopicDefinition TOPIC =
            new KafkaInputOperations.TopicDefinition("input", 1, (short) 1,
                    KafkaInputOperations.TopicCleanupPolicy.DELETE, Duration.ofDays(7));
    private static final List<KafkaInputOperations.TopicDefinition> TOPICS = List.of(TOPIC);

    @Test
    void waitsForLaggingBrokerAndRetriesAdminObservationsWithoutRecreatingTopics() throws Exception {
        Fixture fixture = new Fixture();
        fixture.descriptionFailures = 1;
        fixture.configurationFailures = 1;
        var brokerAttempts = new java.util.LinkedHashMap<String, Integer>();
        var operations = fixture.operations((endpoint, topics, deadline) -> {
            fixture.events.add(endpoint);
            int attempt = brokerAttempts.merge(endpoint, 1, Integer::sum);
            if (endpoint.equals("broker-2:9092") && attempt == 1) {
                throw new UnknownTopicOrPartitionException("metadata has not propagated");
            }
        });

        operations.createTopics(TOPICS, fixture.deadline(Duration.ofSeconds(1)));

        assertEquals(Map.of("broker-1:9092", 1, "broker-2:9092", 2, "broker-3:9092", 1), brokerAttempts);
        assertEquals(List.of("create", "broker-1:9092", "broker-2:9092", "pause",
                "broker-2:9092", "broker-3:9092", "describe", "pause", "describe",
                "configs", "pause", "configs"), fixture.events);
        assertEquals(1, fixture.creations);
        assertEquals(Duration.ofMillis(150).toNanos(), fixture.clock.get());
    }

    @Test
    void deadlineStopsLaggingBrokerAndRetainsItsFailureAndEndpoint() {
        Fixture fixture = new Fixture();
        var cause = new UnknownTopicOrPartitionException("still lagging");
        AtomicInteger observations = new AtomicInteger();
        var operations = fixture.operations((endpoint, topics, deadline) -> {
            observations.incrementAndGet();
            throw cause;
        });

        Exception failure = assertThrows(Exception.class, () -> operations.createTopics(
                TOPICS, fixture.deadline(Duration.ofMillis(75))));

        assertTrue(failure.getMessage().contains("broker-1:9092"));
        assertSame(cause, failure.getCause());
        assertEquals(2, observations.get());
        assertEquals(1, fixture.creations);
        assertFalse(fixture.events.contains("describe"));
        assertEquals(Duration.ofMillis(75).toNanos(), fixture.clock.get());
    }

    @Test
    void permanentBrokerFailureIsNotRetried() {
        Fixture fixture = new Fixture();
        var cause = new TopicAuthorizationException("denied");
        var operations = fixture.operations((endpoint, topics, deadline) -> { throw cause; });

        Exception failure = assertThrows(Exception.class, () -> operations.createTopics(
                TOPICS, fixture.deadline(Duration.ofSeconds(1))));

        assertTrue(failure.getMessage().contains("broker-1:9092"));
        assertSame(cause, failure.getCause());
        assertEquals(List.of("create"), fixture.events);
    }

    @Test
    void permanentFailureAfterATransientIsNotReplacedByTheExpiredDeadline() {
        Fixture fixture = new Fixture();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger readsBeforeAdmission = new AtomicInteger();
        CountDownLatch admitted = new CountDownLatch(1);
        var permanent = new TopicAuthorizationException("authorization changed");
        var operations = fixture.operations((endpoint, topics, deadline) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new UnknownTopicOrPartitionException("metadata lag");
            }
            assertTrue(admitted.await(1, TimeUnit.SECONDS));
            fixture.clock.set(Duration.ofSeconds(2).toNanos());
            throw permanent;
        }, duration -> {
            fixture.clock.addAndGet(duration.toNanos());
            // The retry checks its budget, then the boundary checks before and after enqueueing.
            // Advance time only after those admission checks so the result reaches the caller.
            readsBeforeAdmission.set(3);
        });
        var deadline = new KafkaInputPreparationDeadline(Duration.ofSeconds(1), () -> {
            long now = fixture.clock.get();
            if (readsBeforeAdmission.get() > 0 && readsBeforeAdmission.decrementAndGet() == 0) {
                admitted.countDown();
            }
            return now;
        });

        Exception failure = assertThrows(Exception.class, () -> operations.createTopics(TOPICS, deadline));

        assertSame(permanent, failure.getCause());
        assertTrue(failure.getMessage().contains("broker-1:9092"));
        assertEquals(Duration.ZERO, deadline.remaining());
        assertEquals(2, attempts.get());
        assertEquals(1, fixture.creations);
    }

    @Test
    void creationFailureIsNeverRetriedAndNamesTheOperation() {
        Fixture fixture = new Fixture();
        fixture.creationFailure = new UnknownTopicOrPartitionException("creation failed");

        Exception failure = assertThrows(Exception.class, () -> fixture.operations(noOpObserver())
                .createTopics(TOPICS, fixture.deadline(Duration.ofSeconds(1))));

        assertTrue(failure.getMessage().contains("creating Kafka topics"));
        assertSame(fixture.creationFailure, failure.getCause());
        assertEquals(List.of("create"), fixture.events);
        assertEquals(1, fixture.creations);
    }

    @Test
    void declarationMismatchNamesDescribeAndDoesNotRetry() {
        Fixture fixture = new Fixture();
        fixture.description = new TopicDescription("input", false, List.of());

        Exception failure = assertThrows(Exception.class, () -> fixture.operations(noOpObserver())
                .createTopics(TOPICS, fixture.deadline(Duration.ofSeconds(1))));

        assertTrue(failure.getMessage().contains("describing created Kafka topics"));
        assertTrue(failure.getCause().getMessage().contains("does not match"));
        assertEquals(List.of("create", "describe"), fixture.events);
    }

    @Test
    void configurationMismatchNamesConfigsAndDoesNotRetry() {
        Fixture fixture = new Fixture();
        fixture.configuration = new Config(List.of());

        Exception failure = assertThrows(Exception.class, () -> fixture.operations(noOpObserver())
                .createTopics(TOPICS, fixture.deadline(Duration.ofSeconds(1))));

        assertTrue(failure.getMessage().contains("describing Kafka topic configuration"));
        assertTrue(failure.getCause().getMessage().contains("does not match"));
        assertEquals(List.of("create", "describe", "configs"), fixture.events);
    }

    @Test
    void interruptionStopsImmediatelyWithTheBrokerContext() {
        Fixture fixture = new Fixture();
        var operations = fixture.operations((endpoint, topics, deadline) -> {
            throw new UnknownTopicOrPartitionException("lagging");
        }, duration -> { throw new InterruptedException("stop requested"); });

        InterruptedException failure = assertThrows(InterruptedException.class,
                () -> operations.createTopics(TOPICS, fixture.deadline(Duration.ofSeconds(1))));

        assertTrue(failure.getMessage().contains("broker-1:9092"));
        assertEquals(List.of("create"), fixture.events);
    }

    @Test
    void blockingObservationIsCancelledWithinTheExistingDeadline() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch interrupted = new CountDownLatch(1);
        var operations = fixture.operations((endpoint, topics, deadline) -> {
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException failure) {
                interrupted.countDown();
                throw failure;
            }
        });

        Exception failure = assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThrows(Exception.class, () -> operations.createTopics(TOPICS,
                        new KafkaInputPreparationDeadline(Duration.ofMillis(200), System::nanoTime))));

        assertTrue(failure.getMessage().contains("broker-1:9092"));
        assertTrue(interrupted.await(1, TimeUnit.SECONDS));
        assertEquals(1, fixture.creations);
    }

    @Test
    void offsetFailuresDistinguishLatestFromEarliestAndKeepTheCause() throws Exception {
        for (int failureCall : List.of(1, 2)) {
            Fixture fixture = new Fixture();
            fixture.offsetFailureCall = failureCall;
            Exception failure = assertThrows(Exception.class, () -> fixture.operations(noOpObserver())
                    .readOffsets("input", 1, fixture.deadline(Duration.ofSeconds(1))));
            assertTrue(failure.getMessage().contains(failureCall == 1 ? "latest offsets" : "earliest offsets"));
            assertSame(fixture.offsetFailure, failure.getCause());
            assertEquals(failureCall, fixture.offsetCalls);
        }
    }

    @Test
    void directMetadataRequiresTopicLeaderAndMatchingDeclaration() {
        KafkaBrokerTopicReadiness.validate(metadata(Errors.NONE, 1, 1), TOPICS);
        assertThrows(UnknownTopicOrPartitionException.class,
                () -> KafkaBrokerTopicReadiness.validate(metadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, 1, 1), TOPICS));
        assertThrows(LeaderNotAvailableException.class,
                () -> KafkaBrokerTopicReadiness.validate(metadata(Errors.NONE, -1, 1), TOPICS));
        assertThrows(IllegalStateException.class,
                () -> KafkaBrokerTopicReadiness.validate(metadata(Errors.NONE, 1, 2), TOPICS));
        assertThrows(TopicAuthorizationException.class,
                () -> KafkaBrokerTopicReadiness.validate(metadata(Errors.TOPIC_AUTHORIZATION_FAILED, 1, 1), TOPICS));
        var absent = new MetadataResponse(new MetadataResponseData(), KafkaBrokerTopicReadiness.METADATA_VERSION);
        assertThrows(UnknownTopicOrPartitionException.class,
                () -> KafkaBrokerTopicReadiness.validate(absent, TOPICS));
    }

    @Test
    void directProbeUsesTheRequestedEndpointAndDisablesAutomaticTopicCreation() throws Exception {
        try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            var executor = Executors.newSingleThreadExecutor();
            var served = executor.submit(() -> {
                try (var socket = server.accept()) {
                    socket.setSoTimeout(2_000);
                    var input = new DataInputStream(socket.getInputStream());
                    int length = input.readInt();
                    assertTrue(length > 0 && length < 4_096);
                    var requestBytes = ByteBuffer.wrap(input.readNBytes(length));
                    var header = RequestHeader.parse(requestBytes);
                    assertEquals(KafkaBrokerTopicReadiness.METADATA_VERSION, header.apiVersion());
                    var request = MetadataRequest.parse(requestBytes, header.apiVersion());
                    assertEquals(List.of("input"), request.topics());
                    assertFalse(request.allowAutoTopicCreation());
                    ByteBuffer response = MessageUtil.toByteBuffer(
                            metadata(Errors.NONE, 1, 1).data(), header.apiVersion());
                    var output = new DataOutputStream(socket.getOutputStream());
                    // Metadata v4 uses response header v0: only the correlation id.
                    output.writeInt(4 + response.remaining());
                    output.writeInt(header.correlationId());
                    output.write(Utils.toArray(response));
                    output.flush();
                }
                return null;
            });
            try {
                KafkaBrokerTopicReadiness.observe(
                        Utils.formatAddress(server.getInetAddress().getHostAddress(), server.getLocalPort()),
                        TOPICS, new KafkaInputPreparationDeadline(Duration.ofSeconds(2), System::nanoTime));
                served.get(2, TimeUnit.SECONDS);
            } finally {
                server.close();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void stalledEndpointCannotHideTheNewLeaderOnALiveEndpoint() throws Exception {
        try (var paused = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var live = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var workers = Executors.newFixedThreadPool(2)) {
            paused.setSoTimeout(2_000);
            var release = new CountDownLatch(1);
            var stuck = workers.submit(() -> {
                try (var socket = paused.accept()) {
                    // The broker accepts TCP and the Metadata request, but never answers.
                    var input = new DataInputStream(socket.getInputStream());
                    input.readNBytes(input.readInt());
                    release.await(5, TimeUnit.SECONDS);
                }
                return null;
            });
            var healthy = workers.submit(() -> {
                var response = metadata(Errors.NONE, 3, 1);
                response.data().topics().iterator().next().partitions().getFirst()
                        .setReplicaNodes(List.of(1, 2, 3)).setIsrNodes(List.of(2, 3));
                serve(live, response); return null;
            });
            try {
                var deadline = org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(Duration.ofSeconds(4), System::nanoTime);
                var result = KafkaBrokerTopicReadiness.describeTopics(endpoint(paused) + "," + endpoint(live), List.of("input"), deadline);
                assertEquals(3, result.get("input").partitions().getFirst().leader().id());
                assertEquals(List.of(1, 2, 3), result.get("input").partitions().getFirst().replicas().stream().map(Node::id).toList());
                assertEquals(List.of(2, 3), result.get("input").partitions().getFirst().isr().stream().map(Node::id).toList());
                assertFalse(deadline.remaining().isZero());
                assertFalse(stuck.isDone(), "Live evidence arrives while the paused request remains unanswered");
                healthy.get(1, TimeUnit.SECONDS);
            } finally { release.countDown(); }
        }
    }

    @Test
    void unavailableEndpointsExpireAndPermanentErrorsAreNotHidden() throws Exception {
        try (var missing = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            String endpoint = endpoint(missing); missing.close();
            var deadline = org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(Duration.ofMillis(100), System::nanoTime);
            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> KafkaBrokerTopicReadiness.describeTopics(endpoint, List.of("input"), deadline));
        }
        try (var denied = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var worker = Executors.newSingleThreadExecutor()) {
            var served = worker.submit(() -> { serve(denied, metadata(Errors.TOPIC_AUTHORIZATION_FAILED, 3, 1)); return null; });
            assertThrows(TopicAuthorizationException.class, () -> KafkaBrokerTopicReadiness.describeTopics(
                    endpoint(denied), List.of("input"), org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(Duration.ofSeconds(2), System::nanoTime)));
            served.get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void transientTopicVisibilityFallsBackAndInternalMetadataRemainsUsable() throws Exception {
        try (var lagging = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var live = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> { serve(lagging, metadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, 1, 1)); return null; });
            var second = workers.submit(() -> {
                var response = metadata(Errors.NONE, 3, 1);
                response.data().topics().iterator().next().setName("__transaction_state").setIsInternal(true);
                serve(live, response); return null;
            });
            var result = KafkaBrokerTopicReadiness.describeTopics(endpoint(lagging) + "," + endpoint(live),
                    List.of("__transaction_state"), org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(Duration.ofSeconds(2), System::nanoTime));
            assertTrue(result.get("__transaction_state").isInternal());
            assertEquals(3, result.get("__transaction_state").partitions().getFirst().leader().id());
            first.get(1, TimeUnit.SECONDS); second.get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void expiredOrInterruptedObservationDoesNotSendAnotherRequest() {
        var now = new AtomicLong();
        var expired = org.savonitar.flink.stability.runtime.api.MonotonicDeadline.start(Duration.ofSeconds(1), now::get);
        now.set(TimeUnit.SECONDS.toNanos(1));
        assertThrows(java.util.concurrent.TimeoutException.class,
                () -> KafkaBrokerTopicReadiness.describeTopics("127.0.0.1:1", List.of("input"), expired));
        Thread.currentThread().interrupt();
        try { assertThrows(InterruptedException.class,
                () -> KafkaBrokerTopicReadiness.describeTopics("127.0.0.1:1", List.of("input"), expired)); }
        finally { Thread.interrupted(); }
    }

    private static String endpoint(ServerSocket socket) {
        return Utils.formatAddress(socket.getInetAddress().getHostAddress(), socket.getLocalPort());
    }

    private static void serve(ServerSocket server, MetadataResponse metadata) throws Exception {
        server.setSoTimeout(3_000);
        try (var socket = server.accept()) {
            socket.setSoTimeout(2_000);
            var input = new DataInputStream(socket.getInputStream());
            var bytes = ByteBuffer.wrap(input.readNBytes(input.readInt()));
            var header = RequestHeader.parse(bytes);
            var request = MetadataRequest.parse(bytes, header.apiVersion());
            assertFalse(request.allowAutoTopicCreation());
            var response = MessageUtil.toByteBuffer(metadata.data(), header.apiVersion());
            var output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(4 + response.remaining()); output.writeInt(header.correlationId());
            output.write(Utils.toArray(response)); output.flush();
        }
    }

    private static MetadataResponse metadata(Errors error, int leader, int partitions) {
        var topic = new MetadataResponseData.MetadataResponseTopic().setName("input")
                .setErrorCode(error.code()).setPartitions(java.util.stream.IntStream.range(0, partitions)
                        .mapToObj(index -> new MetadataResponseData.MetadataResponsePartition()
                                .setPartitionIndex(index).setLeaderId(leader)
                                .setReplicaNodes(List.of(1)).setIsrNodes(List.of(1))).toList());
        var data = new MetadataResponseData().setTopics(
                new MetadataResponseData.MetadataResponseTopicCollection(List.of(topic).iterator()));
        return new MetadataResponse(data, KafkaBrokerTopicReadiness.METADATA_VERSION);
    }

    private static KafkaBrokerTopicReadiness.Observer noOpObserver() {
        return (endpoint, topics, deadline) -> {};
    }

    private static <T> KafkaFuture<T> future(T value, Throwable failure) {
        var result = new KafkaFutureImpl<T>();
        if (failure == null) result.complete(value); else result.completeExceptionally(failure);
        return result;
    }

    private static final class Fixture {
        final AtomicLong clock = new AtomicLong();
        final List<String> events = new ArrayList<>();
        int creations;
        int descriptionFailures;
        int configurationFailures;
        int offsetCalls;
        int offsetFailureCall;
        Throwable creationFailure;
        final Throwable offsetFailure = new TopicAuthorizationException("offset denied");
        final Node node = new Node(1, "broker-1", 9092);
        TopicDescription description = new TopicDescription("input", false,
                List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))));
        Config configuration = new Config(KafkaClientInputOperations.topicConfiguration(TOPIC)
                .entrySet().stream().map(entry -> new ConfigEntry(entry.getKey(), entry.getValue())).toList());
        final Admin admin = (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(),
                new Class<?>[] {Admin.class}, (proxy, method, arguments) -> {
                    switch (method.getName()) {
                        case "createTopics":
                            events.add("create");
                            creations++;
                            return new CreateTopicsResult(Map.of("input",
                                    KafkaTopicReadinessTest.<CreateTopicsResult.TopicMetadataAndConfig>future(
                                            null, creationFailure))) {};
                        case "describeTopics":
                            events.add("describe");
                            return new DescribeTopicsResult(null, Map.of("input", future(description,
                                    descriptionFailures-- > 0
                                            ? new UnknownTopicOrPartitionException("describe lag") : null))) {};
                        case "describeConfigs":
                            events.add("configs");
                            return new DescribeConfigsResult(Map.of(
                                    new ConfigResource(ConfigResource.Type.TOPIC, "input"), future(configuration,
                                            configurationFailures-- > 0
                                                    ? new UnknownTopicOrPartitionException("config lag") : null))) {};
                        case "listOffsets":
                            offsetCalls++;
                            return new ListOffsetsResult(Map.of(new TopicPartition("input", 0),
                                    future(new ListOffsetsResult.ListOffsetsResultInfo(0, 0, Optional.empty()),
                                            offsetCalls == offsetFailureCall ? offsetFailure : null)));
                        default:
                            throw new AssertionError("Unexpected Admin call: " + method.getName());
                    }
                });

        KafkaInputPreparationDeadline deadline(Duration timeout) {
            return new KafkaInputPreparationDeadline(timeout, clock::get);
        }

        KafkaClientInputOperations operations(KafkaBrokerTopicReadiness.Observer observer) {
            return operations(observer, duration -> {
                events.add("pause");
                clock.addAndGet(duration.toNanos());
            });
        }

        KafkaClientInputOperations operations(KafkaBrokerTopicReadiness.Observer observer,
                KafkaClientInputOperations.RetryPause pause) {
            return new KafkaClientInputOperations("broker-1:9092,broker-2:9092,broker-3:9092",
                    Duration.ofSeconds(1), admin,
                    properties -> { throw new AssertionError("Producer must not be opened during readiness"); },
                    properties -> { throw new AssertionError("Consumer must not be opened during readiness"); },
                    observer, pause);
        }
    }
}
