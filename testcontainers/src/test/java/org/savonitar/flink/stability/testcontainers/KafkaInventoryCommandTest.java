package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.ExecStartCmd;
import com.github.dockerjava.api.command.InspectExecCmd;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the real exec boundary with command proxies; no Docker discovery or processes. */
class KafkaInventoryCommandTest {
    private static final String OWNER = "1".repeat(64);
    private static final byte[] PAYLOAD = "retained-output".getBytes(StandardCharsets.US_ASCII);

    @Test
    void createReturningAfterCancellationCannotStartTheContainerExec() throws Exception {
        FakeDocker fake = new FakeDocker();
        AtomicBoolean canceled = new AtomicBoolean();
        var deadline = deadline();
        var timeout = deadline.timedOut("caller abandoned during create", null);
        var closeFailure = new IllegalStateException("create close failed");
        fake.created = () -> canceled.set(true);
        fake.createCloseFailure = closeFailure;

        var thrown = assertThrows(ContainerOperationTimeoutException.class, () -> fake.run(deadline, () -> {
            if (canceled.get()) throw timeout;
        }));

        assertSame(timeout, thrown);
        assertArrayEquals(new Throwable[] {closeFailure}, thrown.getSuppressed());
        assertEquals(List.of("create", "create-exec", "create-close"), fake.calls);
    }

    @Test
    void cancellationWhileConstructingTheStartCommandIsCheckedBeforeExec() throws Exception {
        FakeDocker fake = new FakeDocker();
        AtomicBoolean canceled = new AtomicBoolean();
        var deadline = deadline();
        var timeout = deadline.timedOut("caller abandoned before start", null);
        fake.startCreated = () -> canceled.set(true);

        assertSame(timeout, assertThrows(ContainerOperationTimeoutException.class,
                () -> fake.run(deadline, () -> { if (canceled.get()) throw timeout; })));
        assertFalse(fake.calls.contains("start-exec"));
        assertFalse(fake.calls.contains("inspect-exec"));
        assertTrue(fake.calls.contains("start-close"));
    }

    @Test
    void completedCallbackCannotHideStartInspectOrCommandCloseFailure() throws Exception {
        FakeDocker healthy = new FakeDocker();
        var completed = healthy.run(deadline(), () -> {});
        assertTrue(completed.finished());
        assertEquals(0, completed.exitCode());
        assertArrayEquals(PAYLOAD, completed.stdout());

        for (String at : List.of("start", "inspect", "inspect-close", "start-close", "create-close")) {
            FakeDocker fake = new FakeDocker();
            var failure = new IllegalStateException("failed at " + at);
            switch (at) {
                case "start" -> fake.startFailure = failure;
                case "inspect" -> fake.inspectFailure = failure;
                case "inspect-close" -> fake.inspectCloseFailure = failure;
                case "start-close" -> fake.startCloseFailure = failure;
                case "create-close" -> fake.createCloseFailure = failure;
                default -> throw new AssertionError(at);
            }
            var result = fake.run(deadline(), () -> {});
            assertFalse(result.finished(), at);
            assertEquals(-1, result.exitCode(), at);
            assertArrayEquals(PAYLOAD, result.stdout(), at);
            assertTrue(new String(result.stderr(), StandardCharsets.US_ASCII).contains("IllegalStateException"), at);
        }
    }

    @Test
    void fatalGuardRemainsPrimaryWhenCommandCloseAlsoFails() throws Exception {
        FakeDocker fake = new FakeDocker();
        var fatal = new AssertionError("fatal caller guard");
        var closeFailure = new IllegalStateException("create close failed");
        fake.createCloseFailure = closeFailure;
        AtomicInteger guards = new AtomicInteger();

        var thrown = assertThrows(AssertionError.class, () -> fake.run(deadline(), () -> {
            if (guards.incrementAndGet() == 2) throw fatal;
        }));

        assertSame(fatal, thrown);
        assertArrayEquals(new Throwable[] {closeFailure}, thrown.getSuppressed());
        assertFalse(fake.calls.contains("start"));
    }

    private static ContainerOperationDeadline deadline() {
        return ContainerOperationDeadline.start("fake inventory exec", Duration.ofSeconds(1), () -> 0L);
    }

    private static final class FakeDocker {
        final List<String> calls = new ArrayList<>();
        Runnable created = () -> {}, startCreated = () -> {};
        RuntimeException startFailure, inspectFailure, createCloseFailure, startCloseFailure, inspectCloseFailure;
        final DockerClient client;

        FakeDocker() throws Exception {
            // Populate only response data; none of these DTOs owns a Docker connection.
            var createdResponse = new ExecCreateCmdResponse();
            set(createdResponse, "id", "owned-exec");
            var state = new InspectExecResponse();
            set(state, "running", false);
            set(state, "exitCode", 0L);
            ExecCreateCmd create = proxy(ExecCreateCmd.class, (self, method, args) -> {
                if (method.getName().startsWith("with")) return self;
                return switch (method.getName()) {
                    case "exec" -> { calls.add("create-exec"); created.run(); yield createdResponse; }
                    case "close" -> { calls.add("create-close"); fail(createCloseFailure); yield null; }
                    default -> throw new AssertionError("Unexpected create method: " + method.getName());
                };
            });
            ExecStartCmd start = proxy(ExecStartCmd.class, (self, method, args) -> {
                if (method.getName().startsWith("with")) return self;
                return switch (method.getName()) {
                    case "exec" -> {
                        calls.add("start-exec");
                        @SuppressWarnings("unchecked")
                        var callback = (ResultCallback<Frame>) args[0];
                        callback.onNext(new Frame(StreamType.STDOUT, PAYLOAD));
                        callback.onComplete();
                        fail(startFailure);
                        yield callback;
                    }
                    case "close" -> { calls.add("start-close"); fail(startCloseFailure); yield null; }
                    default -> throw new AssertionError("Unexpected start method: " + method.getName());
                };
            });
            InspectExecCmd inspect = proxy(InspectExecCmd.class, (self, method, args) -> switch (method.getName()) {
                case "exec" -> { calls.add("inspect-exec"); fail(inspectFailure); yield state; }
                case "close" -> { calls.add("inspect-close"); fail(inspectCloseFailure); yield null; }
                default -> throw new AssertionError("Unexpected inspect method: " + method.getName());
            });
            client = proxy(DockerClient.class, (self, method, args) -> switch (method.getName()) {
                case "execCreateCmd" -> { assertEquals(OWNER, args[0]); calls.add("create"); yield create; }
                case "execStartCmd" -> {
                    assertEquals("owned-exec", args[0]); calls.add("start"); startCreated.run(); yield start;
                }
                case "inspectExecCmd" -> { assertEquals("owned-exec", args[0]); yield inspect; }
                default -> throw new AssertionError("Unexpected Docker method: " + method.getName());
            });
        }

        KafkaLogInventory.Command run(ContainerOperationDeadline deadline, Runnable guard) {
            return KafkaInventoryCommand.run(client, OWNER, 256, deadline, guard,
                    "/usr/bin/stat", "--format=%f %s %Y %i", "--", "/tmp/kafka-logs/output-0");
        }

        private static void fail(RuntimeException failure) { if (failure != null) throw failure; }
    }

    private static void set(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }
}
