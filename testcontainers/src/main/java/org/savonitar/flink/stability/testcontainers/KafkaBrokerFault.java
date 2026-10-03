package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.KafkaBrokerControl;
import org.savonitar.flink.stability.runtime.api.KafkaCommitWindow;
import org.savonitar.flink.stability.runtime.api.KafkaLogCapture;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;
import java.time.Duration;
import java.util.*;

/** One bounded operation; retain partial observations and always allow the later healing step. */
final class KafkaBrokerFault {
    interface Driver {
        KafkaBrokerControl.Snapshot inspect(MonotonicDeadline deadline);
        void mutate(boolean restart, MonotonicDeadline deadline);
        default void mutate(KafkaBrokerControl.Action action, MonotonicDeadline deadline) {
            if (action != KafkaBrokerControl.Action.KILL && action != KafkaBrokerControl.Action.RESTART)
                throw new UnsupportedOperationException("Physical action unavailable: " + action);
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
                case KILL, PAUSE, STOP -> before.running() && !before.paused();
                case RESTART -> !before.running() && !before.paused();
                case RESUME -> before.running() && before.paused();
                case ROLLING_RESTART -> throw new IllegalArgumentException("Rolling restart is an orchestration");
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
                if (action != KafkaBrokerControl.Action.STOP && !restart && (!after.running() || after.paused()) && leadersBefore.stream().noneMatch(partition -> partition.leader() == broker))
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
    interface ClusterDriver {
        default List<KafkaLogCapture.Partition> transactionPartitions(MonotonicDeadline deadline) throws Exception { throw new UnsupportedOperationException("Transaction-state inventory unavailable"); }
        default void electPreferred(List<KafkaLogCapture.Partition> partitions, MonotonicDeadline deadline) throws Exception { throw new UnsupportedOperationException("Preferred election unavailable"); }
        KafkaBrokerControl.Selection select(KafkaBrokerControl.Target target, MonotonicDeadline deadline) throws Exception;
        KafkaBrokerFault.Driver broker(String name);
        default long clockMillis() { return System.currentTimeMillis(); }
        default long sessionTimeoutMillis(int brokerId, MonotonicDeadline deadline) throws Exception { throw new UnsupportedOperationException("Broker session timeout unavailable"); }
        default KafkaCommitWindow.Transaction transaction(String id, MonotonicDeadline deadline) throws Exception { throw new UnsupportedOperationException("Transaction observation unavailable"); }
        default long nanoTime() { return System.nanoTime(); }
    }
    static List<KafkaBrokerControl.Evidence> execute(KafkaBrokerControl.Request request,
            List<KafkaLogCapture.Partition> partitions, ClusterDriver driver) {
        if (request.action() == KafkaBrokerControl.Action.ROLLING_RESTART) return rolling(request, partitions, driver);
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
            long sessionMillis = request.commitTransactionVersion() == null ? 0 : driver.sessionTimeoutMillis(selection.leader(), deadline);
            if (request.commitTransactionVersion() != null && (sessionMillis <= 0 || request.duration().toMillis() <= sessionMillis))
                throw new IllegalStateException("Commit fault hold must exceed the observed broker session timeout");
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
            if (request.commitTransactionVersion() != null && result.getFirst().confirmed()) {
                var witness = KafkaCommitWindowObserver.observe(selection, request.commitTransactionVersion(), sessionMillis,
                        heldAt[0], request.duration(), deadline, driver, broker);
                result.set(0, result.getFirst().withCommitWindow(witness));
            }
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
    private static List<KafkaBrokerControl.Evidence> rolling(KafkaBrokerControl.Request request,
            List<KafkaLogCapture.Partition> partitions, ClusterDriver driver) {
        var deadline = MonotonicDeadline.start(request.timeout(), driver::nanoTime);
        var result = new ArrayList<KafkaBrokerControl.Evidence>();
        try {
            var observed = new ArrayList<>(partitions);
            for (var partition : driver.transactionPartitions(deadline)) if (!observed.contains(partition)) observed.add(partition);
            if (observed.isEmpty() || observed.size() > 256 || observed.stream().noneMatch(p -> p.topic().equals("__transaction_state")))
                throw new IllegalStateException("Rolling restart requires bounded transaction-state metadata");
            awaitFullIsr(observed, deadline, driver, false);
            var reference = driver.select(request.target(), deadline);
            if (reference.transactionalId() == null || !reference.requested().equals(request.target())
                    || reference.leader() < 1 || reference.leader() > 3
                    || !reference.broker().equals("broker-" + reference.leader()))
                throw new IllegalStateException("Open sink coordinator selection is incomplete");
            var order = new ArrayList<>(List.of("broker-1", "broker-2", "broker-3"));
            if (request.order() != KafkaBrokerControl.RollingOrder.FIXED) {
                order.remove(reference.broker());
                order.add(request.order() == KafkaBrokerControl.RollingOrder.COORDINATOR_FIRST ? 0 : 2, reference.broker());
            }
            for (int index = 0; index < order.size(); index++) {
                check(deadline);
                String name = order.get(index);
                var physical = driver.broker(name);
                boolean[] attempted = {false};
                long started = driver.clockMillis();
                var guarded = new Driver() {
                    public KafkaBrokerControl.Snapshot inspect(MonotonicDeadline d) { return physical.inspect(d); }
                    public List<KafkaBrokerControl.Leadership> leaders(List<KafkaLogCapture.Partition> ps, MonotonicDeadline d) throws Exception { return physical.leaders(ps, d); }
                    public void beforeMutation(List<KafkaBrokerControl.Leadership> values) {
                        if (!completeInventory(observed, values) || !KafkaBrokerControl.fullIsr(values))
                            throw new IllegalStateException("Full ISR lost before graceful stop");
                    }
                    public void mutate(boolean restart, MonotonicDeadline d) { throw new UnsupportedOperationException(); }
                    public void mutate(KafkaBrokerControl.Action action, MonotonicDeadline d) { attempted[0] = true; physical.mutate(action, d); }
                    public long nanoTime() { return driver.nanoTime(); }
                    public void pause(MonotonicDeadline d) throws InterruptedException { physical.pause(d); }
                };
                var stopped = execute(name, KafkaBrokerControl.Action.STOP, observed, deadline, guarded);
                KafkaBrokerControl.Evidence restarted = null;
                List<KafkaBrokerControl.Leadership> ready = List.of();
                boolean elected = false;
                boolean election = request.preferredElection() && index == 2;
                if (attempted[0]) {
                    boolean interrupted = Thread.interrupted();
                    boolean expired = deadline.remaining().isZero() || interrupted;
                    var healing = expired ? MonotonicDeadline.start(Duration.ofSeconds(30), driver::nanoTime) : deadline;
                    try {
                        // Reuse exactly the fixed-port restart and physical identity/leadership observer.
                        restarted = execute(name, KafkaBrokerControl.Action.RESTART, observed, healing, physical);
                        if (!restarted.confirmed()) {
                            // Missing metadata or an ambiguous TERM must never suppress the safety start.
                            var safety = healing.remaining().isZero() ? MonotonicDeadline.start(Duration.ofSeconds(30), driver::nanoTime) : healing;
                            try { physical.mutate(KafkaBrokerControl.Action.RESTART, safety); }
                            catch (RuntimeException failure) { restarted = restarted.withError("Safety restart failed: " + failure); }
                        }
                        if (!expired && stopped.confirmed() && restarted.confirmed()) {
                            ready = awaitFullIsr(observed, deadline, driver, false);
                            if (election) {
                                driver.electPreferred(observed, deadline);
                                ready = awaitFullIsr(observed, deadline, driver, true);
                                elected = true;
                            }
                        }
                        if (expired) stopped = stopped.withError("Original rolling deadline expired/interrupted; safety healing only");
                    } catch (Exception failure) {
                        if (failure instanceof InterruptedException) interrupted = true;
                        stopped = stopped.withError("Recovery barrier failed: " + failure);
                    } finally { if (interrupted) Thread.currentThread().interrupt(); }
                }
                var progress = new KafkaBrokerControl.RollingProgress(index + 1, order, request.order(), started, driver.clockMillis(),
                        reference, ready, election, elected);
                result.add(stopped.withRolling(progress));
                if (restarted != null) result.add(restarted.withRolling(progress));
                if (!stopped.confirmed() || restarted == null || !restarted.confirmed() || ready.isEmpty()) break;
            }
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            if (result.isEmpty()) result.add(new KafkaBrokerControl.Evidence("unresolved", false, null, null, List.of(), List.of(),
                    failure.toString(), KafkaBrokerControl.Action.STOP, null));
            else result.set(result.size() - 1, result.getLast().withError(failure.toString()));
        }
        return List.copyOf(result);
    }

    private static boolean completeInventory(List<KafkaLogCapture.Partition> requested, List<KafkaBrokerControl.Leadership> actual) {
        return requested.size() == actual.size() && new HashSet<>(requested).equals(actual.stream()
                .map(p -> new KafkaLogCapture.Partition(p.topic(), p.partition())).collect(java.util.stream.Collectors.toSet()));
    }
    private static List<KafkaBrokerControl.Leadership> awaitFullIsr(List<KafkaLogCapture.Partition> partitions,
            MonotonicDeadline deadline, ClusterDriver driver, boolean preferred) throws Exception {
        var reader = driver.broker("broker-1");
        String unavailable = "No complete metadata";
        while (true) {
            check(deadline);
            try {
                boolean running = true;
                for (String name : List.of("broker-1", "broker-2", "broker-3")) {
                    var state = driver.broker(name).inspect(deadline);
                    running &= state.running() && !state.paused();
                }
                var values = reader.leaders(partitions, deadline);
                check(deadline);
                if (running && completeInventory(partitions, values) && KafkaBrokerControl.fullIsr(values)
                        && (!preferred || values.stream().allMatch(p -> p.leader() == p.replicas().getFirst()))) return values;
                unavailable = "ISR, physical process, inventory or preferred leader not ready";
            } catch (InterruptedException interrupted) { throw interrupted; }
            catch (Exception failure) { unavailable = failure.toString(); }
            if (deadline.remaining().isZero()) throw new IllegalStateException("Full ISR barrier expired: " + unavailable);
            reader.pause(deadline);
        }
    }

}
