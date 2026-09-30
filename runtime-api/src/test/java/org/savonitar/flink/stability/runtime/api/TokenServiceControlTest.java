package org.savonitar.flink.stability.runtime.api;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

class TokenServiceControlTest {
    private static final String PROVIDER = "12345678-1234-1234-1234-123456789abc";
    private static final String JOB = "1".repeat(32);

    @Test
    void legacyEventsOmitContextAndContextParticipatesInEventIdentity() {
        var legacy = new TokenServiceControl.Event(1, TokenServiceControl.Kind.ISSUED,
                "jobmanager-1#1", "jobmanager", 1000, 100, 1, 0,
                TokenServiceControl.Mode.HEALTHY, OptionalLong.of(1), "issued");
        assertTrue(legacy.registration().isEmpty());
        assertTrue(legacy.participantInstance().isEmpty());
        var registration = new TokenServiceControl.RegistrationSnapshot(PROVIDER, "JOB", 1,
                JOB, "test-job", false, 1, List.of());
        var correlated = new TokenServiceControl.Event(1, TokenServiceControl.Kind.ISSUED,
                "jobmanager-1#1", "jobmanager", 1000, 100, 1, 0,
                TokenServiceControl.Mode.HEALTHY, OptionalLong.of(1), "issued",
                Optional.of(registration), Optional.of(PROVIDER));
        assertNotEquals(legacy, correlated);
        assertEquals(Optional.of(registration), correlated.registration());
        assertNotEquals(correlated, new TokenServiceControl.Event(1, TokenServiceControl.Kind.ISSUED,
                "jobmanager-1#1", "jobmanager", 1000, 100, 1, 0,
                TokenServiceControl.Mode.HEALTHY, OptionalLong.of(1), "issued",
                Optional.of(registration), Optional.of("87654321-4321-4321-4321-cba987654321")));
    }

    @Test
    void snapshotCopiesTheJournalAndAcknowledgesOnlyItsContiguousSentPrefix() {
        var first = new TokenServiceControl.Lifecycle(4, "REGISTER", 2, JOB, "test-job");
        var journal = new ArrayList<>(List.of(first));
        var snapshot = new TokenServiceControl.RegistrationSnapshot(PROVIDER, "JOB", 2,
                JOB, "test-job", false, 3, journal);
        journal.add(new TokenServiceControl.Lifecycle(5, "UNREGISTER", 3, JOB, "test-job"));
        assertEquals(List.of(first), snapshot.journal());
        assertEquals(4, snapshot.sentThrough());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.journal().clear());
        assertThrows(IllegalArgumentException.class, () -> new TokenServiceControl.RegistrationSnapshot(
                PROVIDER, "JOB", 2, JOB, "test-job", false, 2, List.of(first)));
        assertThrows(IllegalArgumentException.class, () -> new TokenServiceControl.RegistrationSnapshot(
                PROVIDER, "JOB", 2, JOB, "test-job", false, 3, List.of(first, first)));
        assertEquals(4, new TokenServiceControl.RegistrationSnapshot(PROVIDER, "BOOTSTRAP", 3,
                "-", "-", false, 4, List.of()).sentThrough());
    }
}
