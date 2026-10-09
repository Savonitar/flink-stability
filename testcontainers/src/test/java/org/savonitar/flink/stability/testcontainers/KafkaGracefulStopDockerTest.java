package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test void evidenceLogPreservesBytesAndHandlesActualEndFrames() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var logs = new EvidenceLog(bytes)) {
            logs.accept(new OutputFrame(OutputFrame.OutputType.STDOUT, new byte[]{0, -1, 10}));
            logs.accept(new OutputFrame(OutputFrame.OutputType.STDERR, new byte[]{13, 42}));
            assertNull(OutputFrame.END.getBytes());
            // The pinned callback sends END once for each registered stream.
            try (var callback = new FrameConsumerResultCallback()) {
                callback.addConsumer(OutputFrame.OutputType.STDOUT, logs);
                callback.addConsumer(OutputFrame.OutputType.STDERR, logs);
            }
            logs.awaitCompletion(MonotonicDeadline.start(Duration.ofNanos(1), System::nanoTime));
        }
        assertArrayEquals(new byte[]{0, -1, 10, 13, 42}, bytes.toByteArray());
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
            broker.withLogConsumer(logs);
            boolean accepted = false;
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
                record(output, "created-launch", Map.of("entrypoint", before.getConfig().getEntrypoint(),
                        "command", before.getConfig().getCmd(), "ownedEnvironment", observedEnvironment));
                String propertyPath;
                if (target.launchType().equals(KafkaRuntimeTarget.APACHE_KAFKA)) {
                    assertArrayEquals(new String[]{"/__cacert_entrypoint.sh"}, before.getConfig().getEntrypoint());
                    assertTrue(broker.getCommandParts()[2].endsWith("; exec /tmp/testcontainers_start.sh"));
                    byte[] starter = KafkaLifecycleInspection.transfer(output, "starter", "actual-starter.sh", "/tmp/testcontainers_start.sh",
                            sink -> bounded(observe, "actual Apache starter", () -> broker.copyFileFromContainer(
                                    "/tmp/testcontainers_start.sh", input -> { input.transferTo(sink); return null; })));
                    assertTrue(new String(starter, StandardCharsets.UTF_8).endsWith("\nexec /etc/kafka/docker/run \n"));
                    propertyPath = "/opt/kafka/config/server.properties";
                } else {
                    var launch = KafkaRuntimeLaunch.genericKraft(target, 1, broker.getHost(), broker.getMappedPort(9092).toString());
                    assertArrayEquals(launch.command().subList(0, 2).toArray(String[]::new), before.getConfig().getEntrypoint());
                    assertArrayEquals(launch.command().subList(2, 3).toArray(String[]::new), before.getConfig().getCmd());
                    assertEquals(launch.environment(), observedEnvironment);
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
                logs.assertHealthy();
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
                assertTrue(finalLog.contains("Terminating process due to signal SIGTERM"), "Kafka must report TERM receipt");
                assertTrue(finalLog.contains("Transition from STARTED to SHUTTING_DOWN"), "Kafka shutdown must start");
                assertTrue(finalLog.contains("[BrokerServer id=1] shut down completed"), "Broker shutdown must complete");
                assertTrue(finalLog.contains("Graceful shutdown completed"), "Raft shutdown must complete");
                assertFalse(driver.inspect(stopWindow).running());
                logs.awaitCompletion(stopWindow);
                record(output, "result", Map.of("accepted", true, "containerId", id, "exitCode", after.getState().getExitCodeLong(),
                        "finishedAt", after.getState().getFinishedAt(), "shutdownConfirmedBeforeCleanup", true));
                accepted = true;
            } finally {
                record(output, "observation-ended", Map.of("accepted", accepted, "containerId", String.valueOf(broker.getContainerId())));
                // Supervisor records this boundary before granting cleanup; no cleanup death can pass STOP.
                var cleanup = MonotonicDeadline.start(Duration.ofMinutes(1), System::nanoTime);
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
            }
        }
        record(output, "log-evidence", Map.of("complete", true, "healthyAfterCleanupAndClose", true));
    }

    /** Retains callback failures for the test thread, including failures delivered during cleanup. */
    private static final class EvidenceLog implements Consumer<OutputFrame>, AutoCloseable {
        private final OutputStream sink;
        private IOException failure;
        private int endedStreams;
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

        private void retain(IOException error) { if (failure == null) failure = error; }

        private synchronized void assertHealthy() throws IOException {
            if (failure != null) throw new IOException("Continuous Kafka log evidence failed", failure);
        }

        private synchronized void awaitCompletion(MonotonicDeadline deadline) throws IOException, InterruptedException {
            assertHealthy();
            while (endedStreams != 2) {
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
    static void record(Path output, String name, Map<String,?> values) throws IOException {
        var result = new LinkedHashMap<String,Object>(); result.put("wall", Instant.now().toString());result.put("monotonicNanos", System.nanoTime());result.putAll(values);
        Path partial = output.resolve(name + ".json.partial"); JSON.writerWithDefaultPrettyPrinter().writeValue(partial.toFile(), result);
        Files.move(partial, output.resolve(name + ".json"), StandardCopyOption.ATOMIC_MOVE);
    }
    private static <T> T bounded(MonotonicDeadline deadline, String name, Callable<T> call) {
        return ContainerDriverCallBoundary.call(ContainerOperationDeadline.shared("Kafka lifecycle regression", deadline), name, call);
    }
}
