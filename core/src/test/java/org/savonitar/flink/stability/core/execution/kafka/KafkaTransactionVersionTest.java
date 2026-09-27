package org.savonitar.flink.stability.core.execution.kafka;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaTransactionVersionTest {
    private static final String BOOTSTRAP = "unused-broker:9092";

    @Test
    void anAbsentRequestDoesNotOpenAdminOrClaimAnObservedVersion() {
        Fixture fixture = new Fixture();

        var selected = fixture.selector().select(BOOTSTRAP, Optional.empty());

        assertEquals(KafkaTransactionVersion.Selection.notRequested(), selected);
        assertTrue(selected.permitsPass());
        assertFalse(selected.confirmed());
        assertTrue(fixture.calls.isEmpty());
        assertTrue(fixture.updates.isEmpty());
    }

    @Test
    void invalidRequestedLevelsAreRejectedBeforeOpeningAdmin() {
        for (int requested : List.of(-1, 0, 3, 65_537)) {
            Fixture fixture = new Fixture();
            assertThrows(IllegalArgumentException.class,
                    () -> fixture.selector().select(BOOTSTRAP, Optional.of(requested)));
            assertTrue(fixture.calls.isEmpty());
        }
    }

    @Test
    void anAlreadyFinalizedLevelOneOrTwoNeedsNoUpdate() {
        for (int level : List.of(1, 2)) {
            var active = observation(level, 10);
            Fixture fixture = new Fixture(active);

            var selected = fixture.select(level);

            assertTrue(selected.confirmed());
            assertTrue(selected.permitsPass());
            assertEquals(Optional.of(level), selected.requested());
            assertEquals(List.of(active), selected.observations());
            assertTrue(selected.error().isEmpty());
            assertEquals(List.of("open", "observe-1", "close"), fixture.names());
            assertTrue(fixture.updates.isEmpty());
        }
    }

    @Test
    void supportedLevelsDoNotSubstituteForFinalizedOrCompleteMetadata() {
        var supported = Optional.of(range(0, 2));
        var finalized = Optional.of(range(1, 1));
        for (var incomplete : List.of(
                new KafkaTransactionVersion.Observation(Optional.empty(), supported, OptionalLong.of(10)),
                new KafkaTransactionVersion.Observation(finalized, Optional.empty(), OptionalLong.of(10)),
                new KafkaTransactionVersion.Observation(finalized, supported, OptionalLong.empty()),
                new KafkaTransactionVersion.Observation(Optional.of(range(1, 2)), supported, OptionalLong.of(10)),
                new KafkaTransactionVersion.Observation(finalized, Optional.of(range(2, 2)), OptionalLong.of(10)))) {
            Fixture fixture = new Fixture(incomplete);

            var selected = fixture.select(1);

            assertFalse(selected.confirmed(), incomplete.toString());
            assertFalse(selected.permitsPass(), incomplete.toString());
            assertTrue(selected.error().isPresent());
            assertEquals(List.of(incomplete), selected.observations());
            assertEquals(List.of("open", "observe-1", "close"), fixture.names());
            assertTrue(fixture.updates.isEmpty());
        }
    }

    @Test
    void validatesThenAppliesAndWaitsForObservedConvergenceInBothDirections() {
        for (int target : List.of(1, 2)) {
            int initialLevel = target == 1 ? 2 : 1;
            var initial = observation(initialLevel, 10);
            var pending = observation(initialLevel, 11);
            var active = observation(target, 12);
            Fixture fixture = new Fixture(initial, pending, active);

            var selected = fixture.select(target);

            FeatureUpdate.UpgradeType type = target == 1
                    ? FeatureUpdate.UpgradeType.SAFE_DOWNGRADE : FeatureUpdate.UpgradeType.UPGRADE;
            assertTrue(selected.confirmed());
            assertEquals(List.of(initial, pending, active), selected.observations());
            assertEquals(List.of(
                    new Update((short) target, type, true),
                    new Update((short) target, type, false)), fixture.updates);
            assertEquals(List.of("open", "observe-1", "validate", "apply", "observe-2",
                    "sleep", "observe-3", "close"), fixture.names());
            assertEquals(Duration.ofMillis(100), fixture.calls.get(5).timeout());
            assertEquals(Duration.ofMillis(29_900), fixture.calls.get(6).timeout());
        }
    }

    @Test
    void validationOrApplicationRejectionNeverFallsBackToAnUnsafeDowngrade() {
        for (String rejected : List.of("validate", "apply")) {
            var initial = observation(2, 10);
            Fixture fixture = new Fixture(initial);
            fixture.failures.put(rejected, new IOException("safe downgrade rejected",
                    new IllegalStateException("incompatible feature dependency")));

            var selected = fixture.select(1);

            assertFalse(selected.permitsPass());
            assertEquals(List.of(initial), selected.observations());
            assertTrue(selected.error().orElseThrow().contains("safe downgrade rejected"));
            assertTrue(selected.error().orElseThrow().contains("incompatible feature dependency"));
            assertEquals(rejected.equals("validate")
                    ? List.of("open", "observe-1", "validate", "close")
                    : List.of("open", "observe-1", "validate", "apply", "close"), fixture.names());
            assertEquals(rejected.equals("validate") ? 1 : 2, fixture.updates.size());
            assertTrue(fixture.updates.stream().allMatch(update ->
                    update.type() == FeatureUpdate.UpgradeType.SAFE_DOWNGRADE));
        }
    }

    @Test
    void timeoutSharesOneBudgetAcrossOpenValidationApplicationPollingAndClose() {
        var initial = observation(2, 10);
        var pending = observation(2, 11);
        Fixture fixture = new Fixture(initial, pending);
        fixture.delays.putAll(Map.of(
                "open", Duration.ofSeconds(3),
                "observe-1", Duration.ofSeconds(4),
                "validate", Duration.ofSeconds(5),
                "apply", Duration.ofSeconds(6),
                "observe-2", Duration.ofMillis(11_900)));

        var selected = fixture.select(1);

        assertFalse(selected.permitsPass());
        assertTrue(selected.error().orElseThrow().contains("timed out"));
        assertEquals(List.of(initial, pending), selected.observations());
        assertEquals(List.of("open", "observe-1", "validate", "apply", "observe-2",
                "sleep", "close"), fixture.names());
        assertEquals(List.of(Duration.ofSeconds(30), Duration.ofSeconds(27), Duration.ofSeconds(23),
                        Duration.ofSeconds(18), Duration.ofSeconds(12), Duration.ZERO),
                fixture.calls.stream().filter(call -> !call.name().equals("sleep"))
                        .map(Call::timeout).toList());
        assertEquals(Duration.ofSeconds(30).toNanos(), fixture.clock.get());
    }

    @Test
    void aMatchingResponseThatArrivesAfterTheDeadlineCannotConfirm() {
        var initial = observation(2, 10);
        var matching = observation(1, 11);
        Fixture fixture = new Fixture(initial, matching);
        fixture.delays.put("observe-2", Duration.ofSeconds(31));

        var selected = fixture.select(1);

        assertFalse(selected.confirmed());
        assertEquals(List.of(initial, matching), selected.observations());
        assertTrue(selected.error().orElseThrow().contains("timed out"));
        assertEquals(new Call("close", Duration.ZERO), fixture.calls.getLast());
    }

    @Test
    void metadataEpochCannotGoBackwardsBetweenAnyTwoPolls() {
        var initial = observation(2, 10);
        for (List<KafkaTransactionVersion.Observation> responses : List.of(
                List.of(initial, observation(1, 9)),
                List.of(initial, observation(2, 12), observation(1, 11)))) {
            Fixture fixture = new Fixture(responses.toArray(KafkaTransactionVersion.Observation[]::new));

            var selected = fixture.select(1);

            assertFalse(selected.confirmed(), "An epoch rollback cannot be accepted as convergence");
            assertTrue(selected.error().orElseThrow().contains("epoch went backwards"));
            assertEquals(responses, selected.observations());
            assertEquals("close", fixture.names().getLast());
        }
    }

    @Test
    void closeFailureRetainsTheObservationAndAnyEarlierSelectionFailure() {
        for (boolean earlierFailure : List.of(false, true)) {
            var initial = observation(earlierFailure ? 2 : 1, 10);
            Fixture fixture = new Fixture(initial);
            fixture.failures.put("close", new IOException("close failed"));
            if (earlierFailure) {
                fixture.failures.put("validate", new IOException("validation rejected"));
            }

            var selected = fixture.select(1);

            assertFalse(selected.permitsPass());
            assertEquals(List.of(initial), selected.observations());
            assertTrue(selected.error().orElseThrow().contains("Admin close: java.io.IOException: close failed"));
            if (earlierFailure) {
                assertTrue(selected.error().orElseThrow().contains("validation rejected"));
            }
            assertEquals(1, fixture.names().stream().filter("close"::equals).count());
        }
    }

    @Test
    void openingFailureIsEvidenceAndHasNoUnownedAdminToClose() {
        Fixture fixture = new Fixture();
        fixture.failures.put("open", new IOException("Admin creation failed"));

        var selected = fixture.select(1);

        assertFalse(selected.permitsPass());
        assertTrue(selected.observations().isEmpty());
        assertTrue(selected.error().orElseThrow().contains("Admin creation failed"));
        assertEquals(List.of("open"), fixture.names());
    }

    @Test
    void interruptionRestoresTheCallerFlagAndRetainsAvailableEvidence() {
        boolean originallyInterrupted = Thread.interrupted();
        try {
            for (String interrupted : List.of("open", "observe-1", "validate", "apply", "sleep", "close")) {
                Thread.interrupted();
                Fixture fixture = new Fixture(observation(2, 10), observation(2, 11));
                // Reach close without polling when close itself is the interrupted operation.
                int target = interrupted.equals("close") ? 2 : 1;
                fixture.failures.put(interrupted, new InterruptedException("selection interrupted"));

                var selected = fixture.select(target);

                assertTrue(Thread.currentThread().isInterrupted(), interrupted);
                assertFalse(selected.permitsPass(), interrupted);
                assertTrue(selected.error().orElseThrow().contains("selection interrupted"), interrupted);
                assertEquals(interrupted.equals("open") || interrupted.equals("observe-1")
                        ? 0 : interrupted.equals("sleep") ? 2 : 1, selected.observations().size(), interrupted);
                assertEquals(interrupted.equals("open") ? "open" : "close", fixture.names().getLast());
            }
        } finally {
            Thread.interrupted();
            if (originallyInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static KafkaTransactionVersion.Range range(int minimum, int maximum) {
        return new KafkaTransactionVersion.Range((short) minimum, (short) maximum);
    }

    private static KafkaTransactionVersion.Observation observation(int level, long epoch) {
        return new KafkaTransactionVersion.Observation(Optional.of(range(level, level)),
                Optional.of(range(0, 2)), OptionalLong.of(epoch));
    }

    private record Call(String name, Duration timeout) {}

    private record Update(short version, FeatureUpdate.UpgradeType type, boolean validateOnly) {}

    private static final class Fixture implements KafkaTransactionVersion.Operations {
        private final AtomicLong clock = new AtomicLong();
        private final Deque<KafkaTransactionVersion.Observation> observations = new ArrayDeque<>();
        private final List<Call> calls = new ArrayList<>();
        private final List<Update> updates = new ArrayList<>();
        private final Map<String, Duration> delays = new HashMap<>();
        private final Map<String, Exception> failures = new HashMap<>();
        private int observationCount;

        private Fixture(KafkaTransactionVersion.Observation... observations) {
            this.observations.addAll(List.of(observations));
        }

        private KafkaTransactionVersion selector() {
            return new KafkaTransactionVersion((bootstrap, timeout) -> {
                assertEquals(BOOTSTRAP, bootstrap);
                call("open", timeout);
                return this;
            }, clock::get, duration -> {
                calls.add(new Call("sleep", duration));
                if (failures.get("sleep") instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
                clock.addAndGet(duration.toNanos());
            });
        }

        private KafkaTransactionVersion.Selection select(int target) {
            return selector().select(BOOTSTRAP, Optional.of(target));
        }

        private List<String> names() {
            return calls.stream().map(Call::name).toList();
        }

        private void call(String name, Duration timeout) throws Exception {
            calls.add(new Call(name, timeout));
            clock.addAndGet(delays.getOrDefault(name, Duration.ZERO).toNanos());
            Exception failure = failures.get(name);
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public KafkaTransactionVersion.Observation observe(Duration timeout) throws Exception {
            call("observe-" + ++observationCount, timeout);
            assertFalse(observations.isEmpty(), "Unexpected extra feature observation");
            return observations.removeFirst();
        }

        @Override
        public void update(short version, FeatureUpdate.UpgradeType type, boolean validateOnly,
                           Duration timeout) throws Exception {
            updates.add(new Update(version, type, validateOnly));
            call(validateOnly ? "validate" : "apply", timeout);
        }

        @Override
        public void close(Duration timeout) throws Exception {
            call("close", timeout);
        }
    }
}
