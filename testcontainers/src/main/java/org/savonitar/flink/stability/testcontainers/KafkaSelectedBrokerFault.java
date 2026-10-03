package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.*;

/** Select once, retain the selected identity, and always heal that same broker. */
final class KafkaSelectedBrokerFault {
    interface Driver {
        KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline deadline) throws Exception;
        KafkaBrokerFault.Driver broker(String name);
        default long nanoTime() { return System.nanoTime(); }
    }
    static List<KafkaBrokerControl.Evidence> execute(KafkaBrokerControl.Request request,
            List<KafkaLogCapture.Partition> partitions, Driver driver) {
        var deadline = MonotonicDeadline.start(request.timeout(), driver::nanoTime);
        var result = new ArrayList<KafkaBrokerControl.Evidence>();
        KafkaBrokerControl.Selection selection = null;
        KafkaBrokerFault.Driver broker = null;
        boolean[] mutationAttempted = {false}; long[] heldAt = {0};
        List<KafkaLogCapture.Partition> observed = new ArrayList<>(partitions);
        try {
            selection = driver.select(request.target(), deadline);
            if (selection.topic() != null) {
                var selected = new KafkaLogCapture.Partition(selection.topic(), selection.partition());
                if (!observed.contains(selected)) observed.add(selected);
            }
            if (observed.size() > 129) throw new IllegalStateException("Broker observation partition bound exceeded");
            broker = driver.broker(selection.broker());
            var selected = selection; var physical = broker;
            var guarded = new KafkaBrokerFault.Driver() {
                @Override public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline remaining) { return physical.inspect(remaining); }
                @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> ps, MonotonicDeadline remaining) throws Exception {
                    return physical.leaders(ps, remaining);
                }
                @Override public void beforeMutation(List<KafkaBrokerControl.Leadership> leaders) {
                    if (selected.topic() != null && leaders.stream().noneMatch(value -> value.topic().equals(selected.topic())
                            && value.partition() == selected.partition() && value.leader() == selected.leader()))
                        throw new IllegalStateException("Selected partition leader changed before injection");
                }
                @Override public void mutate(boolean restart, MonotonicDeadline remaining) { throw new UnsupportedOperationException(); }
                @Override public void mutate(KafkaBrokerControl.Action action, MonotonicDeadline remaining) {
                    mutationAttempted[0] = true; heldAt[0] = driver.nanoTime(); physical.mutate(action, remaining);
                }
                @Override public long nanoTime() { return driver.nanoTime(); }
                @Override public void pause(MonotonicDeadline remaining) throws InterruptedException { physical.pause(remaining); }
            };
            result.add(KafkaBrokerFault.execute(selection.broker(), request.action(), observed, deadline, guarded,
                    request.duration()).withSelection(selection));
            while (mutationAttempted[0] && driver.nanoTime() - heldAt[0] < request.duration().toNanos()) {
                if (deadline.remaining().isZero()) throw new IllegalStateException("Broker hold deadline expired");
                broker.pause(deadline);
            }
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            if (result.isEmpty()) result.add(new KafkaBrokerControl.Evidence(
                    selection == null ? "unresolved" : selection.broker(), false, null, null, List.of(), List.of(),
                    failure.toString(), request.action(), selection));
            else result.set(0, result.getFirst().withError(failure.toString()));
        } finally {
            if (mutationAttempted[0] && broker != null) {
                // Safety healing stays bounded even after expiration/interruption; it cannot restore fault confirmation.
                boolean interrupted = Thread.interrupted();
                boolean expired = deadline.remaining().isZero() || interrupted;
                var healing = expired ? MonotonicDeadline.start(Duration.ofSeconds(30), driver::nanoTime) : deadline;
                try {
                    var action = request.action() == KafkaBrokerControl.Action.KILL ? KafkaBrokerControl.Action.RESTART : KafkaBrokerControl.Action.RESUME;
                    var physical = broker; boolean[] healAttempted = {false};
                    var healer = new KafkaBrokerFault.Driver() {
                        @Override public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline remaining) { return physical.inspect(remaining); }
                        @Override public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> ps, MonotonicDeadline remaining) throws Exception {
                            return physical.leaders(ps, remaining);
                        }
                        @Override public void mutate(boolean restart, MonotonicDeadline remaining) { throw new UnsupportedOperationException(); }
                        @Override public void mutate(KafkaBrokerControl.Action mode, MonotonicDeadline remaining) {
                            healAttempted[0] = true; physical.mutate(mode, remaining);
                        }
                        @Override public void pause(MonotonicDeadline remaining) throws InterruptedException { physical.pause(remaining); }
                    };
                    var healed = KafkaBrokerFault.execute(selection.broker(), action, observed, healing, healer).withSelection(selection);
                    if (!healAttempted[0]) {
                        // Missing pre-heal metadata must not suppress the physical safety operation.
                        try {
                            var safety = healing.remaining().isZero()
                                    ? MonotonicDeadline.start(Duration.ofSeconds(30), driver::nanoTime) : healing;
                            physical.mutate(action, safety);
                            healed = healed.withError("Physical safety heal attempted without complete pre-heal observations");
                        } catch (RuntimeException failure) { healed = healed.withError("Safety heal failed: " + failure); }
                    }
                    result.add(healed);
                    if (expired) result.set(0, result.getFirst().withError("Original fault deadline expired/interrupted; safety healing only"));
                } finally { if (interrupted) Thread.currentThread().interrupt(); }
            }
        }
        return List.copyOf(result);
    }
}
