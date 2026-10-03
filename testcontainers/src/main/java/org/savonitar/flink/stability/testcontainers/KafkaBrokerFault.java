package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaBrokerControl;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import java.time.Duration;
import java.util.List;

/** One bounded operation; retain partial observations and always allow the later healing step. */
final class KafkaBrokerFault {
    interface Driver {
        KafkaBrokerControl.Snapshot inspect(MonotonicDeadline deadline);
        void mutate(boolean restart, MonotonicDeadline deadline);
        List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline) throws Exception;
        default void pause(MonotonicDeadline deadline) throws InterruptedException {
            Thread.sleep(Math.min(100, Math.max(1, deadline.remaining().toMillis())));
        }
    }
    static KafkaBrokerControl.Evidence execute(String name, boolean restart,
            List<KafkaLogCapture.Partition> partitions, Duration timeout, Driver driver) {
        return execute(name, restart, partitions, MonotonicDeadline.start(timeout, System::nanoTime), driver);
    }
    static KafkaBrokerControl.Evidence execute(String name, boolean restart,
            List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline, Driver driver) {
        KafkaBrokerControl.Snapshot before = null, after = null;
        List<KafkaBrokerControl.Leadership> leadersBefore = List.of(), leadersAfter = List.of();
        try {
            check(deadline);
            before = driver.inspect(deadline);
            if (before.running() == restart) throw new IllegalStateException("Unexpected broker state before operation");
            leadersBefore = driver.leaders(partitions, deadline);
            check(deadline);
            driver.mutate(restart, deadline);
            do {
                check(deadline);
                after = driver.inspect(deadline);
                leadersAfter = driver.leaders(partitions, deadline);
                var observed = new KafkaBrokerControl.Evidence(name, restart, before, after, leadersBefore, leadersAfter, null);
                if (observed.confirmed()) return observed;
                int broker = before.brokerId();
                if (!restart && !after.running() && leadersBefore.stream().noneMatch(partition -> partition.leader() == broker))
                    throw new IllegalStateException("No declared partition was led by the killed broker");
                driver.pause(deadline);
            } while (true);
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            return new KafkaBrokerControl.Evidence(name, restart, before, after, leadersBefore, leadersAfter,
                    failure.getClass().getSimpleName() + ": " + failure.getMessage());
        }
    }
    private static void check(MonotonicDeadline deadline) {
        if (Thread.currentThread().isInterrupted() || deadline.remaining().isZero())
            throw new IllegalStateException("Broker operation deadline expired or interrupted");
    }
}
