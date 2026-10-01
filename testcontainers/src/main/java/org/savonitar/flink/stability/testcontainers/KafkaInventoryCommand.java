package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Only the adapter's fixed find/stat calls use this bounded, shell-free exec boundary. */
final class KafkaInventoryCommand {
    private KafkaInventoryCommand() {}

    static KafkaLogInventory.Command run(DockerClient client, String ownedContainerId,
            int maximumBytes, ContainerOperationDeadline deadline, Runnable checkActive, String... argv) {
        if (maximumBytes < 1 || maximumBytes > KafkaLogInventory.MAX_INVENTORY_BYTES) {
            throw new IllegalArgumentException("Invalid inventory output bound");
        }
        checkActive.run();
        var callback = new Output(maximumBytes);
        try (callback; var create = client.execCreateCmd(ownedContainerId).withAttachStdout(true)
                .withAttachStderr(true).withTty(false).withCmd(argv)) {
            String execId = create.exec().getId();
            checkActive.run();
            try (var start = client.execStartCmd(execId).withDetach(false).withTty(false)) {
                checkActive.run();
                start.exec(callback);
                checkActive.run();
                try {
                    if (!callback.done.await(deadline.remaining("reading inventory exec").toNanos(), TimeUnit.NANOSECONDS)) {
                        return callback.result(-1, false);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return callback.result(-1, false);
                }
                if (!callback.complete || callback.overflow || callback.failed) return callback.result(-1, false);
                checkActive.run();
                try (var inspect = client.inspectExecCmd(execId)) {
                    var state = inspect.exec();
                    checkActive.run();
                    boolean finished = Boolean.FALSE.equals(state.isRunning()) && state.getExitCodeLong() != null;
                    return callback.result(finished ? state.getExitCodeLong() : -1, finished);
                }
            }
        } catch (ContainerOperationTimeoutException timeout) {
            throw timeout;
        } catch (IOException | RuntimeException failure) {
            // A failed start/inspect/close cannot certify that the container exec terminated.
            callback.noteFailure(failure);
            return callback.result(-1, false);
        }
    }

    static final class Output extends ResultCallback.Adapter<Frame> {
        private final int maximum;
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final ByteArrayOutputStream err = new ByteArrayOutputStream();
        final CountDownLatch done = new CountDownLatch(1);
        volatile boolean overflow, failed, complete;
        Output(int maximum) { this.maximum = maximum; }

        @Override public synchronized void onNext(Frame frame) {
            if (overflow || failed || complete) return;
            if (frame.getStreamType() != StreamType.STDOUT && frame.getStreamType() != StreamType.STDERR) {
                failed = true; done.countDown(); return;
            }
            byte[] payload = frame.getPayload();
            int count = Math.min(payload.length, maximum - out.size() - err.size());
            (frame.getStreamType() == StreamType.STDOUT ? out : err).write(payload, 0, count);
            if (count != payload.length) { overflow = true; done.countDown(); }
        }
        @Override public void onError(Throwable failure) { failed = true; done.countDown(); }
        @Override public void onComplete() { complete = true; done.countDown(); }
        synchronized void noteFailure(Throwable failure) {
            failed = true;
            byte[] diagnostic = failure.getClass().getSimpleName().getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            int count = Math.min(diagnostic.length, maximum - out.size() - err.size());
            err.write(diagnostic, 0, count);
        }
        synchronized KafkaLogInventory.Command result(long exitCode, boolean finished) {
            return new KafkaLogInventory.Command(out.toByteArray(), err.toByteArray(), exitCode,
                    finished && complete && !overflow && !failed);
        }
    }
}
