package org.savonitar.flink.stability.core.execution.kafka;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keeps a potentially blocking Kafka client invocation from holding the attempt thread past its
 * absolute deadline. Calls submitted to one boundary remain strictly sequential.
 */
final class KafkaInputCallBoundary implements AutoCloseable {

    private static final AtomicLong THREAD_ORDINAL = new AtomicLong();

    private final ExecutorService executor;

    KafkaInputCallBoundary(String threadPurpose) {
        Objects.requireNonNull(threadPurpose, "threadPurpose");
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(
                    runnable,
                    "flink-stability-" + threadPurpose + "-"
                            + THREAD_ORDINAL.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    <T> T call(
            String operation,
            KafkaInputPreparationDeadline deadline,
            Callable<T> invocation) throws Exception {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(deadline, "deadline");
        Objects.requireNonNull(invocation, "invocation");
        // Never enqueue a Kafka mutation after the stage budget is already exhausted.
        deadline.requireRemaining(operation);
        Future<T> submitted = executor.submit(invocation);
        try {
            // Submission and worker-start overhead consume the same absolute budget.
            Duration remaining = deadline.requireRemaining(operation);
            return submitted.get(remaining.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException failure) {
            submitted.cancel(true);
            throw failure;
        } catch (TimeoutException failure) {
            submitted.cancel(true);
            throw deadline.exceeded(operation, failure);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Kafka invocation failed", cause);
        } catch (KafkaInputPreparationDeadline
                .KafkaInputPreparationDeadlineExceededException failure) {
            submitted.cancel(true);
            throw failure;
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
