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
        default void mutate(KafkaBrokerControl.Action action, MonotonicDeadline deadline) {
            if (action == KafkaBrokerControl.Action.PAUSE || action == KafkaBrokerControl.Action.RESUME)
                throw new UnsupportedOperationException("Pause unavailable");
            mutate(action.heals(), deadline);
        }
        List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline) throws Exception;
        default long nanoTime() { return System.nanoTime(); }
        default void beforeMutation(List<KafkaBrokerControl.Leadership> leaders) {}
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
        return execute(name, restart ? KafkaBrokerControl.Action.RESTART : KafkaBrokerControl.Action.KILL, partitions, deadline, driver);
    }
    static KafkaBrokerControl.Evidence execute(String name, KafkaBrokerControl.Action action,
            List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline, Driver driver) {
        return execute(name, action, partitions, deadline, driver, null);
    }
    static KafkaBrokerControl.Evidence execute(String name, KafkaBrokerControl.Action action,
            List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline, Driver driver, Duration maximumActive) {
        boolean restart = action.heals();
        KafkaBrokerControl.Snapshot before = null, after = null;
        List<KafkaBrokerControl.Leadership> leadersBefore = List.of(), leadersAfter = List.of();
        try {
            check(deadline);
            before = driver.inspect(deadline);
            boolean ready = switch (action) {
                case KILL, PAUSE -> before.running() && !before.paused();
                case RESTART -> !before.running() && !before.paused();
                case RESUME -> before.running() && before.paused();
            };
            if (!ready) throw new IllegalStateException("Unexpected broker state before operation");
            leadersBefore = driver.leaders(partitions, deadline);
            check(deadline);
            driver.beforeMutation(leadersBefore);
            long mutationStarted = driver.nanoTime();
            driver.mutate(action, deadline);
            if (maximumActive != null) {
                var activeRemaining = maximumActive.minusNanos(Math.max(0, driver.nanoTime() - mutationStarted));
                if (activeRemaining.isNegative() || activeRemaining.isZero()) throw new IllegalStateException("Broker hold expired during mutation");
                deadline = MonotonicDeadline.start(deadline.remaining().compareTo(activeRemaining) < 0
                        ? deadline.remaining() : activeRemaining, driver::nanoTime);
            }
            do {
                check(deadline);
                after = driver.inspect(deadline);
                leadersAfter = driver.leaders(partitions, deadline);
                check(deadline);
                var observed = new KafkaBrokerControl.Evidence(name, restart, before, after, leadersBefore, leadersAfter, null, action, null);
                if (observed.confirmed()) return observed;
                int broker = before.brokerId();
                if (!restart && (!after.running() || after.paused()) && leadersBefore.stream().noneMatch(partition -> partition.leader() == broker))
                    throw new IllegalStateException("No declared partition was led by the killed broker");
                driver.pause(deadline);
            } while (true);
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            return new KafkaBrokerControl.Evidence(name, restart, before, after, leadersBefore, leadersAfter,
                    failure.getClass().getSimpleName() + ": " + failure.getMessage(), action, null);
        }
    }
    private static void check(MonotonicDeadline deadline) {
        if (Thread.currentThread().isInterrupted() || deadline.remaining().isZero())
            throw new IllegalStateException("Broker operation deadline expired or interrupted");
    }
}
