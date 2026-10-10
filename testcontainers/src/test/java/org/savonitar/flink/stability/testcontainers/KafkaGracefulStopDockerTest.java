package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.exception.NotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.savonitar.flink.stability.testcontainers.KafkaLifecycleInspection.record;

/** Opt-in production launch/STOP regression; the external supervisor owns hard limits and cleanup. */
class KafkaGracefulStopDockerTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration LIMIT = Duration.ofMinutes(2);
    private static final String LABEL = "org.savonitar.kafka-lifecycle";

    /** Runs only this already compiled test, without a build or additional test selection. */
    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--verify-evidence")) {
            Path output = Path.of(args[1]).toAbsolutePath();
            Files.createDirectory(output);
            verifyEvidenceSerialization(output);
            return;
        }
        if (args.length != 0) throw new IllegalArgumentException("Expected no arguments or --verify-evidence <new-directory>");
        if (!Boolean.getBoolean("flink.kafka.lifecycle")) throw new IllegalStateException("Live opt-in required");
        new KafkaGracefulStopDockerTest().productionKafkaIsPidOneAndTerminatesAfterStopBeforeCleanup();
    }

    @Test void evidenceSerializationWorksWithoutDocker(@TempDir Path output) throws IOException {
        verifyEvidenceSerialization(output);
    }

    @Test void evidenceLogPreservesBytesDeliveredDirectlyToTheSinkAndHandlesEndFrames() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var logs = new EvidenceLog(bytes)) {
            logs.accept(new OutputFrame(OutputFrame.OutputType.STDOUT, new byte[]{0, -1, 10}));
            logs.accept(new OutputFrame(OutputFrame.OutputType.STDERR, new byte[]{13, 42}));
            assertNull(OutputFrame.END.getBytes());
            // The pinned callback sends END once for each registered stream.
            try (var callback = new EvidenceCallback(logs)) {
                callback.onComplete();
            }
            logs.awaitCompletion(MonotonicDeadline.start(Duration.ofNanos(1), System::nanoTime));
        }
        assertArrayEquals(new byte[]{0, -1, 10, 13, 42}, bytes.toByteArray());
    }

    @Test void pinnedCallbackNormalizesUtf8AndAnsiBeforeDeliveringTextToTheSink() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var logs = new EvidenceLog(bytes); var callback = new EvidenceCallback(logs)) {
            byte[] prefix = "\u001b[31mcaf".getBytes(StandardCharsets.UTF_8);
            callback.onNext(new com.github.dockerjava.api.model.Frame(com.github.dockerjava.api.model.StreamType.STDOUT, prefix));
            callback.onNext(new com.github.dockerjava.api.model.Frame(com.github.dockerjava.api.model.StreamType.STDOUT,
                    new byte[]{(byte) 0xc3}));
            callback.onNext(new com.github.dockerjava.api.model.Frame(com.github.dockerjava.api.model.StreamType.STDOUT,
                    new byte[]{(byte) 0xa9, 27, '[', '0', 'm', '\n'}));
            callback.onNext(new com.github.dockerjava.api.model.Frame(com.github.dockerjava.api.model.StreamType.STDERR,
                    new byte[]{(byte) 0xff, '\n'}));
            callback.onComplete();
            logs.awaitCompletion(logDeadline()); logs.assertHealthy();
        }
        assertArrayEquals("caf\u00e9\n\ufffd\n".getBytes(StandardCharsets.UTF_8), bytes.toByteArray(),
                "The pinned line adapter strips ANSI color and decodes/re-encodes UTF-8; this is not raw transport capture");
    }

    @Test void evidenceLogRejectsMalformedOrdinaryFrames() {
        for (var type : List.of(OutputFrame.OutputType.STDOUT, OutputFrame.OutputType.STDERR)) {
            var logs = new EvidenceLog(new ByteArrayOutputStream());
            logs.accept(new OutputFrame(type, null));
            assertThrows(IOException.class, logs::assertHealthy);
            assertThrows(IOException.class, logs::close);
        }
    }

    @Test void evidenceLogRetainsAsynchronousWriteFailureThroughEndAndClose() throws Exception {
        IOException failure = new IOException("write failed");
        var logs = new EvidenceLog(new OutputStream() {
            @Override public void write(int value) throws IOException { throw failure; }
        });
        Thread callback = new Thread(() -> {
            logs.accept(new OutputFrame(OutputFrame.OutputType.STDOUT, new byte[]{1}));
            logs.accept(OutputFrame.END); logs.accept(OutputFrame.END);
        });
        callback.start(); callback.join();
        assertSame(failure, assertThrows(IOException.class, logs::assertHealthy).getCause());
        assertThrows(IOException.class, () -> logs.awaitCompletion(MonotonicDeadline.start(LIMIT, System::nanoTime)));
        assertSame(failure, assertThrows(IOException.class, logs::close).getCause());
    }

    @Test void evidenceLogRetainsFlushAndCloseFailures() {
        for (boolean failFlush : List.of(true, false)) {
            IOException failure = new IOException(failFlush ? "flush failed" : "close failed");
            var logs = new EvidenceLog(new ByteArrayOutputStream() {
                @Override public void flush() throws IOException { if (failFlush) throw failure; }
                @Override public void close() throws IOException { if (!failFlush) throw failure; }
            });
            logs.accept(new OutputFrame(OutputFrame.OutputType.STDERR, new byte[]{2}));
            logs.accept(OutputFrame.END); logs.accept(OutputFrame.END);
            assertSame(failure, assertThrows(IOException.class, logs::close).getCause());
        }
    }

    @Test void evidenceLogCannotPassWithOnlyOneEndOrOutputAfterCompletion() throws Exception {
        try (var logs = new EvidenceLog(new ByteArrayOutputStream())) {
            logs.accept(OutputFrame.END);
            assertThrows(IOException.class, () -> logs.awaitCompletion(MonotonicDeadline.start(Duration.ofNanos(1), System::nanoTime)));
            logs.accept(OutputFrame.END);
            assertThrows(IOException.class, () -> logs.awaitCompletion(MonotonicDeadline.start(Duration.ofNanos(1), System::nanoTime)),
                    "END frames alone do not prove natural transport completion");
            logs.completedNaturally();
            logs.awaitCompletion(MonotonicDeadline.start(Duration.ofNanos(1), System::nanoTime));
        }
        var logs = new EvidenceLog(new ByteArrayOutputStream());
        logs.accept(OutputFrame.END); logs.accept(OutputFrame.END);
        logs.accept(new OutputFrame(OutputFrame.OutputType.STDOUT, new byte[]{3}));
        assertThrows(IOException.class, logs::close);
    }

    @Test void evidenceLogChecksFailuresRaisedDuringSinkClose() {
        EvidenceLog[] reference = new EvidenceLog[1];
        var logs = new EvidenceLog(new ByteArrayOutputStream() {
            @Override public void close() {
                reference[0].accept(new OutputFrame(OutputFrame.OutputType.STDERR, null));
            }
        });
        reference[0] = logs;
        logs.accept(OutputFrame.END); logs.accept(OutputFrame.END);
        assertThrows(IOException.class, logs::close);
    }

    @Test void actualContinuousRequestRetainsTransportFailureBeforeAndAfterPartialOutput() throws Exception {
        for (boolean partial : List.of(false, true)) {
            var bytes = new ByteArrayOutputStream();
            var logs = new EvidenceLog(bytes);
            var callback = new EvidenceCallback(logs);
            var calls = Collections.synchronizedList(new ArrayList<String>());
            var failure = new IOException("injected transport failure");
            var docker = logClient(calls, connected -> {
                if (partial) connected.onNext(new com.github.dockerjava.api.model.Frame(
                        com.github.dockerjava.api.model.StreamType.STDOUT, new byte[]{42, 10}));
                connected.onError(failure);
            });
            var observed = assertThrows(IOException.class, () -> callback.follow(docker, "owned-broker", logDeadline()));
            assertSame(failure, observed.getCause().getCause());
            assertArrayEquals(partial ? new byte[]{42, 10} : new byte[0], bytes.toByteArray());
            assertEquals(2, logs.endedStreams, "Inherited END delivery must not erase the transport failure");
            assertThrows(IOException.class, logs::assertHealthy);
            assertThrows(IOException.class, () -> logs.awaitCompletion(logDeadline()));
            assertThrows(UncheckedIOException.class, () -> callback.awaitCompletion(1, TimeUnit.SECONDS));
            assertThrows(IOException.class, callback::close);
            // The exact live acceptance helper also rejects despite a successful final-log retrieval.
            String finalLog = "Terminating process due to signal SIGTERM\nTransition from STARTED to SHUTTING_DOWN\n"
                    + "[BrokerServer id=1] shut down completed\nGraceful shutdown completed\n";
            assertThrows(IOException.class, () -> confirmShutdownLogs(finalLog, logs, logDeadline()));
            assertThrows(IOException.class, logs::close);
            assertTrue(calls.contains("stream.close")); assertEquals(1, Collections.frequency(calls, "request.close"));
        }
    }

    @Test void actualContinuousRequestRequiresNaturalCompletionAndOwnsItsResources() throws Exception {
        var calls = Collections.synchronizedList(new ArrayList<String>());
        var bytes = new ByteArrayOutputStream();
        try (var logs = new EvidenceLog(bytes); var callback = new EvidenceCallback(logs)) {
            callback.follow(logClient(calls, connected -> {}), "owned-broker", logDeadline());
            assertEquals(List.of("container=owned-broker", "withFollowStream=true", "withSince=0",
                    "withStdOut=true", "withStdErr=true", "exec"), calls);
            callback.onNext(new com.github.dockerjava.api.model.Frame(
                    com.github.dockerjava.api.model.StreamType.STDERR, new byte[]{42, 10}));
            callback.onComplete();
            logs.awaitCompletion(logDeadline());
            assertTrue(callback.awaitCompletion(1, TimeUnit.SECONDS));
            assertTrue(logs.naturalCompletion); logs.assertHealthy();
        }
        assertArrayEquals(new byte[]{42, 10}, bytes.toByteArray());
        assertEquals(1, Collections.frequency(calls, "stream.close"));
        assertEquals(1, Collections.frequency(calls, "request.close"));
    }

    @Test void deliberateContinuousRequestCloseCannotManufactureNaturalCompletion() throws Exception {
        var logs = new EvidenceLog(new ByteArrayOutputStream());
        var callback = new EvidenceCallback(logs);
        var calls = Collections.synchronizedList(new ArrayList<String>());
        callback.follow(logClient(calls, connected -> {}), "owned-broker", logDeadline());
        assertThrows(IOException.class, callback::close);
        assertEquals(2, logs.endedStreams); assertFalse(logs.naturalCompletion);
        callback.onComplete(); // A late completion notification cannot rescue cancellation.
        assertFalse(logs.naturalCompletion);
        assertThrows(IOException.class, logs::assertHealthy);
        assertThrows(IOException.class, () -> logs.awaitCompletion(logDeadline()));
        assertThrows(UncheckedIOException.class, () -> callback.awaitCompletion(1, TimeUnit.SECONDS));
        assertThrows(IOException.class, logs::close);
        assertTrue(calls.contains("stream.close")); assertTrue(calls.contains("request.close"));
    }

    @Test void completionWaitsRetainConcurrentStreamCloseFailure() throws Exception {
        for (boolean timed : List.of(false, true)) {
            var logs = new EvidenceLog(new ByteArrayOutputStream());
            var callback = new EvidenceCallback(logs);
            var closing = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var closeFailure = new IOException("stream close failed");
            callback.onStart(() -> {
                closing.countDown();
                try { if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("Close fixture timed out"); }
                catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
                throw closeFailure;
            });
            try (var workers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
                var completion = workers.submit(callback::onComplete);
                assertTrue(closing.await(5, TimeUnit.SECONDS));
                var waiting = workers.submit(() -> assertThrows(UncheckedIOException.class, () -> {
                    if (timed) callback.awaitCompletion(5, TimeUnit.SECONDS);
                    else callback.awaitCompletion();
                }));
                release.countDown();
                waiting.get(5, TimeUnit.SECONDS); completion.get(5, TimeUnit.SECONDS);
            } finally { release.countDown(); }
            assertSame(closeFailure, logs.failure.getCause());
            assertFalse(logs.naturalCompletion);
            assertThrows(IOException.class, () -> logs.awaitCompletion(logDeadline()));
            assertThrows(IOException.class, callback::close); assertThrows(IOException.class, logs::close);
        }
    }

    @Test void transportErrorAfterCompletionRemainsVisibleThroughFinalClose() throws Exception {
        var logs = new EvidenceLog(new ByteArrayOutputStream());
        var callback = new EvidenceCallback(logs);
        callback.onComplete(); logs.awaitCompletion(logDeadline());
        var failure = new IOException("late transport failure");
        callback.onError(failure);
        assertSame(failure, assertThrows(IOException.class, logs::assertHealthy).getCause().getCause());
        assertThrows(IOException.class, () -> logs.awaitCompletion(logDeadline()));
        assertThrows(IOException.class, callback::close); assertThrows(IOException.class, logs::close);
    }

    @Test void aLateStartCannotReopenACancelledContinuousRequest() throws Exception {
        var logs = new EvidenceLog(new ByteArrayOutputStream());
        var callback = new EvidenceCallback(logs);
        assertThrows(IOException.class, callback::close);
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        callback.onStart(closes::incrementAndGet);
        assertEquals(1, closes.get());
        assertFalse(callback.awaitStarted(1, TimeUnit.NANOSECONDS));
        assertThrows(IOException.class, callback::close);
        assertThrows(IOException.class, logs::assertHealthy);
        assertFalse(logs.naturalCompletion); assertThrows(IOException.class, logs::close);
    }

    @Test void cleanupKeepsTheOriginalFailureAndAttemptsRemainingResources() throws Exception {
        var primary = new IOException("transport failed");
        var cleanup = new IOException("runtime cleanup failed");
        var close = new IOException("request close failed");
        var attempted = new ArrayList<String>();
        cleanupAfter(primary,
                () -> { attempted.add("runtime"); throw cleanup; },
                () -> { attempted.add("callback"); throw close; });
        assertEquals(List.of("runtime", "callback"), attempted);
        assertArrayEquals(new Throwable[]{cleanup, close}, primary.getSuppressed());
        assertSame(cleanup, assertThrows(IOException.class, () -> cleanupAfter(null, () -> { throw cleanup; })));
    }

    @Test void cleanupPreservesInterruptionAfterAttemptingOwnedResources() throws Exception {
        try {
            Thread.currentThread().interrupt();
            cleanupAfter(new InterruptedException("observation interrupted"),
                    () -> { assertFalse(Thread.currentThread().isInterrupted()); return null; });
            assertTrue(Thread.currentThread().isInterrupted());
        } finally { Thread.interrupted(); }
    }

    @Test void interruptionDuringOneCleanupDoesNotSkipTheNextResource() throws Exception {
        var primary = new IOException("observation failed");
        var attempted = new ArrayList<String>();
        try {
            cleanupAfter(primary,
                    () -> {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("bounded cleanup interrupted", new InterruptedException());
                    },
                    () -> { assertFalse(Thread.currentThread().isInterrupted()); attempted.add("callback"); return null; });
            assertEquals(List.of("callback"), attempted);
            assertEquals(1, primary.getSuppressed().length);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally { Thread.interrupted(); }
    }

    private static MonotonicDeadline logDeadline() { return MonotonicDeadline.start(LIMIT, System::nanoTime); }

    /** Drives the same request method as the live fixture, with no Docker daemon or client factory. */
    private static DockerClient logClient(List<String> calls, Consumer<EvidenceCallback> connected) {
        var request = (LogContainerCmd) Proxy.newProxyInstance(LogContainerCmd.class.getClassLoader(),
                new Class<?>[]{LogContainerCmd.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "withFollowStream", "withSince", "withStdOut", "withStdErr" -> {
                            calls.add(method.getName() + "=" + args[0]); return proxy;
                        }
                        case "exec" -> {
                            calls.add("exec");
                            var callback = assertInstanceOf(EvidenceCallback.class, args[0]);
                            callback.onStart(() -> calls.add("stream.close"));
                            connected.accept(callback); return callback;
                        }
                        case "close" -> { calls.add("request.close"); return null; }
                        default -> throw new AssertionError("Unexpected log request method " + method.getName());
                    }
                });
        return (DockerClient) Proxy.newProxyInstance(DockerClient.class.getClassLoader(), new Class<?>[]{DockerClient.class},
                (proxy, method, args) -> {
                    if (!method.getName().equals("logContainerCmd")) throw new AssertionError(method.getName());
                    calls.add("container=" + args[0]); return request;
                });
    }

    private static void verifyEvidenceSerialization(Path output) throws IOException {
        record(output, "startup-submitted", Map.of("case", "default-apache"));
        var state = JSON.readValue("""
                {"Id":"owned-fixture", "Image":"fixture-image", "State":{
                  "Status":"exited", "Running":false, "Paused":false, "Restarting":false,
                  "OOMKilled":false, "Dead":false, "Pid":0, "ExitCode":143, "Error":"",
                  "StartedAt":"2026-01-01T00:00:00Z", "FinishedAt":"2026-01-01T00:00:01Z"}}
                """, InspectContainerResponse.class);
        record(output, "ready", selectedState(state));
        String image = "sha256:" + "a".repeat(64), id = "b".repeat(64);
        var target = target("custom-apache", "apache/kafka:4.0.0", image);
        var container = new KafkaRuntimeEvidence.Container(target.brokerAlias(1), id, image,
                Map.of("KAFKA_NODE_ID", "1"), List.of("sh", "-c", "exec /tmp/testcontainers_start.sh"),
                KafkaRuntimeTarget.LOG_DIRECTORY, true, true, "#!/bin/bash\nexec /etc/kafka/docker/run \n");
        var evidence = new KafkaRuntimeEvidence(target.clusterAlias(), target.imageReference(), target.imageId(),
                target.launchType(), target.brokerConfig(), List.of(container));
        assertTrue(evidence.confirms(target));
        record(output, "runtime-evidence", selectedRuntimeEvidence(evidence));
        KafkaLifecycleInspection.launch(output, null, null, new String[]{"sh", "-c", "exec starter"}, Map.of());
        var startup = JSON.readTree(output.resolve("startup-submitted.json").toFile());
        assertEquals("default-apache", startup.path("case").asText());
        assertDoesNotThrow(() -> Instant.parse(startup.path("wall").asText()));
        assertTrue(startup.path("monotonicNanos").isIntegralNumber());
        var ready = JSON.readTree(output.resolve("ready.json").toFile());
        assertEquals("owned-fixture", ready.path("containerId").asText());
        assertEquals("fixture-image", ready.path("imageId").asText());
        assertTrue(ready.path("state").path("Running").isBoolean());
        assertFalse(ready.path("state").path("Running").asBoolean());
        assertEquals(143L, ready.path("state").path("ExitCode").asLong());
        var receipt = JSON.readTree(output.resolve("runtime-evidence.json").toFile());
        assertEquals(image, receipt.path("imageId").asText());
        assertEquals("3600000", receipt.path("brokerConfig").path("log.retention.ms").asText());
        assertEquals(id, receipt.path("containers").get(0).path("containerId").asText());
        assertEquals(container.startupScript(), receipt.path("containers").get(0).path("startupScript").asText());
        var launch = JSON.readTree(output.resolve("created-launch.json").toFile());
        assertTrue(launch.has("expectedEntrypoint") && launch.path("expectedEntrypoint").isNull());
        assertTrue(launch.has("entrypoint") && launch.path("entrypoint").isNull());
        try (var files = Files.list(output)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".partial")));
        }
    }

    @EnabledIfSystemProperty(named = "flink.kafka.lifecycle", matches = "true")
    @Test void productionKafkaIsPidOneAndTerminatesAfterStopBeforeCleanup() throws Exception {
        String selected = required("case"), imageName = required("image"), expectedImage = required("imageId");
        String platform = required("platform"), digest = required("digest"), session = required("session");
        String networkId = required("networkId");
        Path output = Path.of(required("output")).toAbsolutePath();
        Files.createDirectory(output);
        record(output, "startup-submitted", Map.of("case", selected));
        var target = target(selected, imageName, expectedImage);
        var network = suppliedNetwork(networkId);
        KafkaRuntimeCluster runtime = target.launchType().equals(KafkaRuntimeTarget.GENERIC_KRAFT)
                ? new GenericKraftKafkaRuntime(network, target) : new ApacheKafkaRuntime(network, target);
        GenericContainer<?> broker = ownedBroker(runtime);
        broker.withStartupAttempts(1).withStartupTimeout(LIMIT).withLabel(LABEL, session)
                .withLabel(LABEL + ".case", selected)
                .withCreateContainerCmdModifier(command -> command.withPlatform(platform));
        try (var logs = new EvidenceLog(Files.newOutputStream(output.resolve("kafka-continuous.log"), StandardOpenOption.CREATE_NEW))) {
            var callback = new EvidenceCallback(logs);
            boolean accepted = false;
            Throwable primaryFailure = null;
            try {
                var docker = DockerClientFactory.instance().client();
                var preflight = MonotonicDeadline.start(Duration.ofSeconds(20), System::nanoTime);
                var image = KafkaLifecycleInspection.observation(output, "image", () -> bounded(preflight, "inspect pinned image", () -> {
                    try (var command = docker.inspectImageCmd(imageName)) { return command.exec(); }
                }), value -> Map.of("reference", imageName, "id", value.getId(), "platform", platform, "digest", digest,
                        "observedPlatform", value.getOs() + "/" + value.getArch(), "repoDigests", value.getRepoDigests()));
                assertEquals(expectedImage, image.getId());
                assertEquals(String.join("/", Arrays.copyOf(platform.split("/"), 2)), image.getOs() + "/" + image.getArch());
                assertTrue(image.getRepoDigests().contains(digest));
                runtime.start();
                logs.assertHealthy();
                String id = broker.getContainerId();
                var observe = MonotonicDeadline.start(LIMIT, System::nanoTime);
                var before = inspectOwned(broker, target, session, selected, networkId, observe);
                callback.follow(docker, id, observe);
                record(output, "ready", selectedState(before));
                assertEquals(expectedImage, before.getImageId());
                assertArrayEquals(broker.getCommandParts(), before.getConfig().getCmd());
                Map<String, String> expectedEnvironment = broker.getEnvMap();
                Map<String, String> observedEnvironment = new TreeMap<>();
                for (String entry : before.getConfig().getEnv()) {
                    int split = entry.indexOf('=');
                    if (split > 0 && expectedEnvironment.containsKey(entry.substring(0, split)))
                        assertNull(observedEnvironment.put(entry.substring(0, split), entry.substring(split + 1)), "Duplicate owned environment key");
                }
                assertEquals(expectedEnvironment, observedEnvironment);
                var genericLaunch = target.launchType().equals(KafkaRuntimeTarget.GENERIC_KRAFT)
                        ? KafkaRuntimeLaunch.genericKraft(target, 1, broker.getHost(), broker.getMappedPort(9092).toString()) : null;
                String[] expectedEntrypoint = genericLaunch == null
                        ? Objects.requireNonNull(image.getConfig(), "Selected image configuration").getEntrypoint()
                        : genericLaunch.command().subList(0, 2).toArray(String[]::new);
                KafkaLifecycleInspection.launch(output, expectedEntrypoint, before.getConfig().getEntrypoint(),
                        before.getConfig().getCmd(), observedEnvironment);
                String propertyPath;
                if (target.launchType().equals(KafkaRuntimeTarget.APACHE_KAFKA)) {
                    assertTrue(broker.getCommandParts()[2].endsWith("; exec /tmp/testcontainers_start.sh"));
                    byte[] starter = KafkaLifecycleInspection.transfer(output, "starter", "actual-starter.sh", "/tmp/testcontainers_start.sh",
                            sink -> bounded(observe, "actual Apache starter", () -> broker.copyFileFromContainer(
                                    "/tmp/testcontainers_start.sh", input -> { input.transferTo(sink); return null; })));
                    assertTrue(new String(starter, StandardCharsets.UTF_8).endsWith("\nexec /etc/kafka/docker/run \n"));
                    propertyPath = "/opt/kafka/config/server.properties";
                } else {
                    assertArrayEquals(genericLaunch.command().subList(2, 3).toArray(String[]::new), before.getConfig().getCmd());
                    assertEquals(genericLaunch.environment(), observedEnvironment);
                    propertyPath = KafkaRuntimeLaunch.CONFIG_FILE;
                }
                byte[] rawProperties = KafkaLifecycleInspection.transfer(output, "broker-config", "broker-config.raw", propertyPath,
                        sink -> bounded(observe, "actual broker configuration", () -> broker.copyFileFromContainer(
                                propertyPath, input -> { input.transferTo(sink); return null; })));
                var actualProperties = KafkaLifecycleInspection.brokerProperties(rawProperties, Set.of("process.roles", "node.id",
                        "offsets.topic.replication.factor", "transaction.max.timeout.ms", "transaction.state.log.replication.factor",
                        "transaction.state.log.min.isr", "group.initial.rebalance.delay.ms", "log.retention.ms"));
                try (var writer = Files.newBufferedWriter(output.resolve("selected-broker.properties"), StandardOpenOption.CREATE_NEW)) {
                    actualProperties.store(writer, "Selected from the observed broker file");
                }
                target.resolvedBrokerConfig().forEach((key, value) -> assertEquals(value, actualProperties.getProperty(key), key));
                assertEquals("broker,controller", actualProperties.getProperty("process.roles"));
                assertEquals("1", actualProperties.getProperty("node.id"));
                runtime.runtimeEvidence().ifPresent(evidence -> {
                    assertTrue(evidence.confirms(target), "Custom runtime receipt must confirm the selected target");
                    try { record(output, "runtime-evidence", selectedRuntimeEvidence(evidence)); }
                    catch (IOException failure) { throw new UncheckedIOException(failure); }
                });
                long hostPid = before.getState().getPidLong();
                var top = KafkaLifecycleInspection.observation(output, "host-top", () -> bounded(observe, "owned host process topology", () -> {
                    try (var command = docker.topContainerCmd(id).withPsArgs("-eo pid,ppid,lstart,args")) { return command.exec(); }
                }), value -> Map.of("titles", value.getTitles(), "processes", value.getProcesses(), "inspectHostPid", hostPid));
                assertTrue(hostPid > 0 && Arrays.stream(top.getProcesses()).anyMatch(row -> row[0].trim().equals(Long.toString(hostPid))
                        && String.join(" ", row).contains("kafka.Kafka")), "Container host PID must be Kafka Java");
                String status = inspectCommand(broker, output, observe, "pid1-status", "/bin/cat", "/proc/1/status");
                String stat = inspectCommand(broker, output, observe, "pid1-stat", "/bin/cat", "/proc/1/stat");
                String command = inspectCommand(broker, output, observe, "pid1-cmdline", "/bin/cat", "/proc/1/cmdline");
                String namespace = inspectCommand(broker, output, observe, "pid1-namespace", "/usr/bin/readlink", "/proc/1/ns/pid");
                Files.writeString(output.resolve("namespace-pid-one.txt"), KafkaLifecycleInspection.processIdentity(status, stat, command, namespace));
                var driver = new DockerKafkaBrokerDriver(broker, target, 1, networkId, () -> true, null, new HashMap<>());
                assertTrue(driver.inspect(observe).running());
                var stopWindow = MonotonicDeadline.start(LIMIT, System::nanoTime);
                logs.assertCapturing();
                record(output, "term-submitted", Map.of("containerId", id));
                driver.mutate(KafkaBrokerControl.Action.STOP, stopWindow);
                record(output, "term-returned", Map.of("containerId", id));
                InspectContainerResponse after;
                do {
                    logs.assertHealthy();
                    after = inspectOwned(broker, target, session, selected, networkId, stopWindow);
                    Files.writeString(output.resolve("states.jsonl"), JSON.writeValueAsString(selectedState(after)) + "\n",
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    if (!Boolean.TRUE.equals(after.getState().getRunning())) break;
                    assertFalse(stopWindow.remaining().isZero(), "Kafka did not terminate after TERM");
                    Thread.sleep(Math.min(200, Math.max(1, stopWindow.remaining().toMillis())));
                } while (true);
                assertEquals(expectedImage, after.getImageId()); assertEquals(0L, after.getState().getPidLong());
                assertFalse(Boolean.TRUE.equals(after.getState().getOOMKilled())); assertNotNull(after.getState().getExitCodeLong());
                String finalLog = bounded(stopWindow, "Kafka shutdown log", broker::getLogs);
                Files.writeString(output.resolve("kafka-final.log"), finalLog);
                assertFalse(driver.inspect(stopWindow).running());
                confirmShutdownLogs(finalLog, logs, stopWindow);
                record(output, "result", Map.of("accepted", true, "containerId", id, "exitCode", after.getState().getExitCodeLong(),
                        "finishedAt", after.getState().getFinishedAt(), "shutdownConfirmedBeforeCleanup", true));
                accepted = true;
            } catch (Exception | Error failure) {
                primaryFailure = failure;
                throw failure;
            } finally {
                // Supervisor records this boundary before granting cleanup; no cleanup death can pass STOP.
                var cleanup = MonotonicDeadline.start(Duration.ofMinutes(1), System::nanoTime);
                boolean observationAccepted = accepted;
                cleanupAfter(primaryFailure,
                        () -> { record(output, "observation-ended", Map.of("accepted", observationAccepted,
                                "containerId", String.valueOf(broker.getContainerId()))); return null; },
                        () -> {
                            while (!Files.exists(output.resolve("cleanup-authorized"))) {
                                assertFalse(cleanup.remaining().isZero(), "Supervisor did not authorize owned cleanup");
                                Thread.sleep(50);
                            }
                            String cleanupId = broker.getContainerId();
                            if (cleanupId != null) {
                                try { inspectOwned(broker, target, session, selected, networkId, cleanup); }
                                catch (NotFoundException alreadyRemovedByFailedStartup) { /* Startup owns its failed container cleanup. */ }
                            }
                            bounded(cleanup, "owned runtime cleanup", () -> { runtime.stop(); return null; });
                            if (cleanupId != null) assertThrows(NotFoundException.class, () -> bounded(cleanup, "owned removal confirmation", () -> {
                                // GenericContainer.stop clears its current ID; retain the positively checked identity.
                                try (var command = broker.getDockerClient().inspectContainerCmd(cleanupId)) { return command.exec(); }
                            }));
                            record(output, "cleanup", Map.of("ownedContainerRemoved", true, "containerId", String.valueOf(cleanupId)));
                            return null;
                        },
                        () -> bounded(cleanup, "continuous log request cleanup", () -> { callback.close(); return null; }));
            }
        }
        // These are success postconditions after natural completion, owned cleanup and all closes.
        // result.json is earlier shutdown evidence; reaching it alone does not establish these postconditions.
        record(output, "log-evidence", Map.of("complete", true, "healthyAfterCleanupAndClose", true,
                "naturalCompletion", true, "transportFailed", false, "cancelled", false));
    }

    private static void confirmShutdownLogs(String finalLog, EvidenceLog logs, MonotonicDeadline deadline) throws Exception {
        assertTrue(finalLog.contains("Terminating process due to signal SIGTERM"), "Kafka must report TERM receipt");
        assertTrue(finalLog.contains("Transition from STARTED to SHUTTING_DOWN"), "Kafka shutdown must start");
        assertTrue(finalLog.contains("[BrokerServer id=1] shut down completed"), "Broker shutdown must complete");
        assertTrue(finalLog.contains("Graceful shutdown completed"), "Raft shutdown must complete");
        logs.awaitCompletion(deadline);
    }

    /** Attempt every cleanup action without replacing the original observation failure. */
    private static void cleanupAfter(Throwable primary, Callable<?>... actions) throws Exception {
        Throwable failure = primary;
        boolean interrupted = Thread.interrupted();
        try {
            for (var action : actions) {
                try { action.call(); }
                catch (Exception | Error secondary) {
                    interrupted |= secondary instanceof InterruptedException;
                    if (failure == null) failure = secondary;
                    else if (failure != secondary) failure.addSuppressed(secondary);
                } finally { interrupted |= Thread.interrupted(); }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
        if (primary == null && failure instanceof Exception exception) throw exception;
        if (primary == null && failure instanceof Error error) throw error;
    }

    /** Owns the follow request and its normalized textual output, not byte-for-byte Docker transport. */
    private static final class EvidenceCallback extends FrameConsumerResultCallback {
        private final EvidenceLog logs;
        private LogContainerCmd request;
        private boolean completingNaturally; // onComplete was entered; this does not prove successful cleanup.
        private boolean requestClosed;
        private boolean closeStarted; // Prevents reopening; close may still fail.

        private EvidenceCallback(EvidenceLog logs) {
            this.logs = logs;
            addConsumer(OutputFrame.OutputType.STDOUT, logs);
            addConsumer(OutputFrame.OutputType.STDERR, logs);
        }

        private void follow(DockerClient docker, String id, MonotonicDeadline deadline) throws Exception {
            request = docker.logContainerCmd(id).withFollowStream(true).withSince(0).withStdOut(true).withStdErr(true);
            bounded(deadline, "start continuous Kafka log request", () -> request.exec(this));
            logs.assertHealthy();
            boolean started = awaitStarted(deadline.remaining().toNanos(), TimeUnit.NANOSECONDS);
            logs.assertHealthy();
            assertTrue(started, "Kafka log request did not start");
            logs.assertCapturing();
        }

        @Override public synchronized void onStart(Closeable stream) {
            if (!closeStarted) { super.onStart(stream); return; }
            logs.failedIfHealthy(new IOException("Kafka log stream started after capture was closed"));
            try { stream.close(); }
            catch (IOException | RuntimeException failure) { logs.failed(new IOException("Late Kafka log stream close failed", failure)); }
        }

        @Override public synchronized void onError(Throwable failure) {
            logs.failed(new IOException("Continuous Kafka log transport failed", failure));
            try { close(); }
            catch (IOException retainedFailure) { /* The test thread observes the already retained failure. */ }
        }

        @Override public synchronized void onComplete() {
            completingNaturally = true;
            try { super.onComplete(); logs.completedNaturally(); }
            catch (IOException | RuntimeException failure) { logs.failedIfHealthy(new IOException("Kafka log completion failed", failure)); }
        }

        // Inherited timed waits can exceed their timeout while acquiring this close monitor.
        // The live fixture uses EvidenceLog's deadline and the external supervisor for execution bounds.
        @Override protected synchronized void throwFirstError() {
            super.throwFirstError();
            try { logs.assertHealthy(); }
            catch (IOException failure) { throw new UncheckedIOException(failure); }
        }

        @Override public synchronized void close() throws IOException {
            closeStarted = true;
            if (!completingNaturally) logs.failed(new IOException("Continuous Kafka log capture cancelled before natural completion"));
            try { super.close(); }
            catch (IOException | RuntimeException failure) { logs.failed(new IOException("Kafka log stream close failed", failure)); }
            try { if (request != null && !requestClosed) { requestClosed = true; request.close(); } }
            catch (RuntimeException failure) { logs.failed(new IOException("Kafka log request close failed", failure)); }
            logs.assertHealthy();
        }
    }

    /** Retains callback failures for the test thread, including failures delivered during cleanup. */
    private static final class EvidenceLog implements Consumer<OutputFrame>, AutoCloseable {
        private final OutputStream sink;
        private IOException failure;
        private int endedStreams;
        private boolean naturalCompletion;
        private boolean closed;

        private EvidenceLog(OutputStream sink) { this.sink = Objects.requireNonNull(sink); }

        @Override public synchronized void accept(OutputFrame frame) {
            try {
                Objects.requireNonNull(frame, "Null output frame");
                if (frame.getType() == OutputFrame.OutputType.END) {
                    if (frame.getBytes() != null || endedStreams == 2) throw new IOException("Malformed or duplicate END frame");
                    endedStreams++;
                } else {
                    if (closed || endedStreams == 2) throw new IOException("Output arrived after evidence completion");
                    if (frame.getType() != OutputFrame.OutputType.STDOUT && frame.getType() != OutputFrame.OutputType.STDERR)
                        throw new IOException("Unexpected output frame type");
                    sink.write(Objects.requireNonNull(frame.getBytes(), "Ordinary output frame has no bytes"));
                    sink.flush();
                }
            } catch (IOException error) { retain(error); }
            catch (RuntimeException error) { retain(new IOException("Malformed output frame or evidence sink failure", error)); }
            finally { notifyAll(); }
        }

        private void retain(IOException error) {
            if (failure == null) failure = error;
            else if (failure != error) failure.addSuppressed(error);
        }

        private synchronized void failed(IOException error) { retain(error); notifyAll(); }

        private synchronized void failedIfHealthy(IOException error) { if (failure == null) retain(error); notifyAll(); }

        private synchronized void completedNaturally() throws IOException {
            assertHealthy();
            if (endedStreams != 2) throw new IOException("Natural Kafka log completion did not end both streams");
            naturalCompletion = true;
            notifyAll();
        }

        private synchronized void assertHealthy() throws IOException {
            if (failure != null) throw new IOException("Continuous Kafka log evidence failed", failure);
        }

        private synchronized void assertCapturing() throws IOException {
            assertHealthy();
            if (naturalCompletion) throw new IOException("Continuous Kafka log capture ended before TERM");
        }

        private synchronized void awaitCompletion(MonotonicDeadline deadline) throws IOException, InterruptedException {
            assertHealthy();
            while (endedStreams != 2 || !naturalCompletion) {
                long remaining = deadline.remaining().toNanos();
                if (remaining == 0) throw new IOException("Kafka log streams did not complete within the shutdown deadline");
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
                assertHealthy();
            }
        }

        @Override public synchronized void close() throws IOException {
            if (!closed) {
                closed = true;
                try { sink.close(); }
                catch (IOException error) { retain(error); }
                catch (RuntimeException error) { retain(new IOException("Evidence sink close failed", error)); }
            }
            assertHealthy();
        }
    }

    static KafkaRuntimeTarget target(String selected, String image, String pin) {
        boolean generic = selected.equals("generic-apache") || selected.equals("generic-confluent");
        if (!Set.of("default-apache", "custom-apache", "generic-apache", "generic-confluent").contains(selected))
            throw new IllegalArgumentException("Unknown lifecycle case " + selected);
        return new KafkaRuntimeTarget("lifecycle", image, KafkaBrokerPolicy.v1SingleBroker(), 1,
                selected.equals("default-apache") ? Optional.empty() : Optional.of(pin),
                generic ? KafkaRuntimeTarget.GENERIC_KRAFT : KafkaRuntimeTarget.APACHE_KAFKA,
                Set.of("custom-apache", "generic-apache").contains(selected) ? Map.of("log.retention.ms", "3600000") : Map.of(),
                selected.equals("generic-confluent") ? KafkaRuntimeTarget.CONFLUENT_PLATFORM_LAYOUT : KafkaRuntimeTarget.APACHE_LAYOUT);
    }
    private static GenericContainer<?> ownedBroker(KafkaRuntimeCluster runtime) throws Exception {
        boolean generic = runtime instanceof GenericKraftKafkaRuntime;
        Field field = runtime.getClass().getDeclaredField(generic ? "brokers" : "container");field.setAccessible(true);
        return (GenericContainer<?>) (generic ? ((List<?>) field.get(runtime)).getFirst() : field.get(runtime));
    }
    private static Network suppliedNetwork(String id) {
        return new Network() {
            public String getId() { return id; }
            public void close() { /* Positive-identity removal belongs to the external supervisor. */ }
            public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement base, org.junit.runner.Description description) { return base; }
        };
    }
    private static String required(String name) {
        String value = System.getProperty("flink.kafka.lifecycle." + name);
        assertNotNull(value, "Required explicit live property " + name); return value;
    }
    private static InspectContainerResponse inspectOwned(GenericContainer<?> broker, KafkaRuntimeTarget target,
            String session, String selected, String network, MonotonicDeadline deadline) {
        return bounded(deadline, "owned Kafka state", () -> {
            try (var command = broker.getDockerClient().inspectContainerCmd(Objects.requireNonNull(broker.getContainerId()))) {
                var value = command.exec(); assertEquals(broker.getContainerId(), value.getId());
                assertEquals(session, value.getConfig().getLabels().get(LABEL));
                assertEquals(selected, value.getConfig().getLabels().get(LABEL + ".case"));
                assertTrue(value.getNetworkSettings().getNetworks().values().stream().anyMatch(connection ->
                        network.equals(connection.getNetworkID()) && connection.getAliases().contains(target.networkAlias())));
                return value;
            }
        });
    }
    private static Map<String,Object> selectedState(InspectContainerResponse value) {
        var result = new LinkedHashMap<String,Object>(); result.put("wall", Instant.now().toString()); result.put("monotonicNanos", System.nanoTime());
        result.put("containerId", value.getId()); result.put("imageId", value.getImageId()); result.put("state", value.getState()); return result;
    }
    private static Map<String,Object> selectedRuntimeEvidence(KafkaRuntimeEvidence evidence) {
        return Map.of("imageId", evidence.imageId().orElseThrow(), "launchType", evidence.launchType(),
                "layout", String.valueOf(evidence.layout()), "brokerConfig", evidence.brokerConfig(), "containers", evidence.containers());
    }
    private static String inspectCommand(GenericContainer<?> broker, Path output, MonotonicDeadline deadline,
                                         String name, String... command) throws Exception {
        return KafkaLifecycleInspection.command(output, name, List.of(command),
                () -> bounded(deadline, name, () -> broker.execInContainer(command)));
    }
    private static <T> T bounded(MonotonicDeadline deadline, String name, Callable<T> call) {
        return ContainerDriverCallBoundary.call(ContainerOperationDeadline.shared("Kafka lifecycle regression", deadline), name, call);
    }
}
