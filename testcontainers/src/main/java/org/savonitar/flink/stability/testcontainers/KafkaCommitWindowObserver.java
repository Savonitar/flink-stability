package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.List;

/** Commit proof for the exact selected transaction, after failover and before healing. */
final class KafkaCommitWindowObserver {
    private KafkaCommitWindowObserver() {}
    static KafkaCommitWindow observe(KafkaBrokerControl.Selection selected, int version, long sessionMillis,
                                     long heldAt, Duration hold, MonotonicDeadline outer,
                                     KafkaSelectedBrokerFault.Driver driver, KafkaBrokerFault.Driver physical) {
        KafkaCommitWindow.Observation ongoing = null;
        KafkaCommitWindow.Observation committed = null;
        String lastError = null;
        var remaining = hold.minusNanos(Math.max(0, driver.nanoTime() - heldAt));
        if (remaining.isZero() || remaining.isNegative()) return new KafkaCommitWindow(version, sessionMillis, null, null, "Hold expired before commit observations");
        var deadline = MonotonicDeadline.start(outer.remaining().compareTo(remaining) < 0 ? outer.remaining() : remaining, driver::nanoTime);
        var partition = new KafkaLogCapture.Partition(selected.topic(), selected.partition());
        try {
            while (!deadline.remaining().isZero()) {
                try {
                    var before = physical.inspect(deadline);
                    if (before.running() && !before.paused()) throw new IllegalStateException("Selected broker is no longer held");
                    var leaders = physical.leaders(List.of(partition), deadline);
                    var transaction = driver.transaction(selected.transactionalId(), deadline);
                    var after = physical.inspect(deadline);
                    var rechecked = physical.leaders(List.of(partition), deadline);
                    if (deadline.remaining().isZero()) break;
                    if (!before.equals(after) || !leaders.equals(rechecked) || leaders.size() != 1)
                        throw new IllegalStateException("Physical or leadership identity changed during transaction observation");
                    var leader = leaders.getFirst();
                    if (leader.leader() == selected.leader() || leader.leader() < 1 || leader.leader() != transaction.coordinatorId()
                            || leader.inSyncReplicas().contains(selected.leader()) || leader.inSyncReplicas().size() < 2)
                        throw new IllegalStateException("New transaction coordinator not confirmed");
                    var observation = new KafkaCommitWindow.Observation(transaction.transactionalId(), transaction.producerId(), transaction.producerEpoch(),
                            transaction.state(), transaction.coordinatorId(), driver.clockMillis(), driver.nanoTime() - heldAt, after, leader);
                    if (!selected.transactionalId().equals(transaction.transactionalId()) || transaction.producerId() != selected.producerId())
                        return new KafkaCommitWindow(version, sessionMillis, ongoing, observation, "Selected producer identity changed");
                    if ("ONGOING".equals(transaction.state())) {
                        if (transaction.producerEpoch() != selected.producerEpoch())
                            return new KafkaCommitWindow(version, sessionMillis, ongoing, null, "Selected producer epoch changed before commit");
                        if (ongoing == null) ongoing = observation;
                    } else if ("COMPLETE_COMMIT".equals(transaction.state())) {
                        committed = observation;
                        var result = new KafkaCommitWindow(version, sessionMillis, ongoing, committed, null);
                        return result.confirmed(selected) ? result : new KafkaCommitWindow(version, sessionMillis, ongoing, committed,
                                "Commit window missed or completed before the observed session timeout");
                    } else if (!"PREPARE_COMMIT".equals(transaction.state())) {
                        return new KafkaCommitWindow(version, sessionMillis, ongoing, null, "Transaction left commit path: " + transaction.state());
                    }
                    lastError = null;
                } catch (InterruptedException interrupted) { throw interrupted; }
                catch (Exception unavailable) { lastError = unavailable.toString(); }
                physical.pause(deadline);
            }
            return new KafkaCommitWindow(version, sessionMillis, ongoing, committed,
                    "Commit not witnessed within broker hold" + (lastError == null ? "" : ": " + lastError));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new KafkaCommitWindow(version, sessionMillis, ongoing, committed, "Commit observation interrupted");
        }
    }
}
