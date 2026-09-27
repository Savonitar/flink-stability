package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.FlinkHaEvidence;
import org.savonitar.flink.stability.core.execution.PhaseExecutionEvidence;
import org.savonitar.flink.stability.core.execution.SubjectClassOrigins;
import org.savonitar.flink.stability.core.flink.FlinkJobObservation;
import org.savonitar.flink.stability.core.flink.FlinkJobState;
import org.savonitar.flink.stability.runtime.api.FlinkHaControl;
import org.savonitar.flink.stability.runtime.api.FlinkComponentRole;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkHaEvidenceRendererTest {
    @Test
    void retainsSampledControlHistoryAndActualPerProcessSessionTimeouts() {
        var leader = new FlinkHaControl.LeaderIdentity("jobmanager-1", "container-1", "rpc-1", "session-1");
        var leadership = new FlinkHaControl.Leadership(leader, leader, leader);
        var observations = new FlinkHaControl.Observations(List.of(
                new FlinkHaControl.LeadershipObservation(1, 1, 10, 10,
                        FlinkHaControl.ObservationMoment.INITIAL, Optional.of(leadership), Optional.empty()),
                new FlinkHaControl.LeadershipObservation(2, 7, 20, 80,
                        FlinkHaControl.ObservationMoment.ROUTING, Optional.of(leadership), Optional.empty()),
                new FlinkHaControl.LeadershipObservation(3, 1, 90, 90,
                        FlinkHaControl.ObservationMoment.PRE_FENCE, Optional.empty(), Optional.of("leader unreadable"))),
                List.of(new FlinkHaControl.SessionEvidence("jobmanager-1", FlinkComponentRole.JOB_MANAGER,
                        "container-1", "jobmanager-1#1", 2_000,
                        List.of(new FlinkHaControl.NegotiatedSession("0x123", 4_000, "retained session log")), false)), false);
        var evidence = new FlinkHaEvidence(new FlinkHaEvidence.Expected(List.of(), true, false),
                Optional.empty(), Optional.of(observations), FlinkHaEvidence.Outcome.UNCONFIRMED,
                "Effective session timeout differs from the requested value");
        var node = new ObjectMapper().createObjectNode();

        FlinkHaEvidenceRenderer.render(node, evidence, List.of());

        assertEquals("unconfirmed", node.path("status").asText());
        assertTrue(node.path("haRequired").asBoolean());
        assertEquals("sampled", node.at("/observations/coverage").asText());
        assertEquals(7, node.at("/observations/leadership/1/sampleCount").asInt());
        assertEquals(20, node.at("/observations/leadership/1/firstObservedAtMillis").asInt());
        assertEquals(80, node.at("/observations/leadership/1/lastObservedAtMillis").asInt());
        assertEquals("session-1", node.at("/observations/leadership/0/leadership/resourceManager/sessionId").asText());
        assertEquals("leader unreadable", node.at("/observations/leadership/2/error").asText());
        assertFalse(node.at("/observations/leadership/2").has("leadership"));
        assertEquals("container-1", node.at("/observations/sessions/0/runtimeId").asText());
        assertEquals(2_000, node.at("/observations/sessions/0/requestedTimeoutMillis").asInt());
        assertEquals(4_000, node.at("/observations/sessions/0/negotiated/0/timeoutMillis").asInt());
        assertEquals("retained session log", node.at("/observations/sessions/0/negotiated/0/logLine").asText());
    }

    @Test
    void retainsObservedExitMetadataWithoutInventingUnknownValues() {
        var node = new ObjectMapper().createObjectNode();
        FlinkHaEvidenceRenderer.state(node, new FlinkHaControl.ProcessState("container", false, false,
                Optional.of(137L), Optional.of(true), Optional.of("2026-09-27T18:00:00Z")));
        assertEquals(137, node.path("exitCode").asInt());
        assertTrue(node.path("oomKilled").asBoolean());
        assertEquals("2026-09-27T18:00:00Z", node.path("finishedAt").asText());
        var unknown = new ObjectMapper().createObjectNode();
        FlinkHaEvidenceRenderer.state(unknown, new FlinkHaControl.ProcessState("container", false, false));
        assertFalse(unknown.has("exitCode"));
        assertFalse(unknown.has("oomKilled"));
        assertFalse(unknown.has("finishedAt"));
    }

    @Test
    void retainsUnconfirmedPhysicalLeadershipRecoveryAndTokenProofWithoutInventingSuccess() {
        var request = new FlinkHaControl.LeaderFaultRequest(FlinkHaControl.Mode.PAUSE,
                Duration.ofSeconds(4), Duration.ofSeconds(30), Optional.of(
                        new FlinkHaControl.TokenFault(TokenServiceControl.Mode.FAIL, Duration.ZERO)));
        var leader = new FlinkHaControl.LeaderIdentity("jobmanager-1", "container-1", "rpc-1", "session-1");
        var replacement = new FlinkHaControl.LeaderIdentity("jobmanager-2", "container-2", "rpc-2", "session-2");
        var event = new TokenServiceControl.Event(1, TokenServiceControl.Kind.FAILED,
                "jobmanager-2#1", "provider", 1_500, 50, 7, 2, TokenServiceControl.Mode.FAIL,
                OptionalLong.empty(), "HTTP 503");
        var snapshot = new TokenServiceControl.Snapshot(List.of(event), false, true, 0, 1);
        var raw = new FlinkHaControl.LeaderFaultEvidence(request,
                Optional.of(new FlinkHaControl.Leadership(leader, leader, leader)),
                Optional.of(new FlinkHaControl.Leadership(replacement, replacement, replacement)),
                Optional.of(leader), true, true,
                Optional.of(new FlinkHaControl.ProcessState("container-1", true, true)),
                Optional.of(new FlinkHaControl.ProcessState("container-1", true, false)),
                1_000, 5_000, 0, 0, false, Optional.empty(), Optional.of(snapshot),
                Optional.of(snapshot), List.of("fixture saturation retained"));
        var before = new FlinkJobObservation.Attempt(Optional.of(new FlinkJobObservation(900,
                FlinkJobState.RUNNING, 3, 0, Optional.empty(), List.of(), List.of())), Optional.empty());
        var after = new FlinkJobObservation.Attempt(Optional.of(new FlinkJobObservation(5_100,
                FlinkJobState.FINISHED, 1, 1, Optional.of(new FlinkJobObservation.Restore(3, 5_050)),
                List.of(), List.of())), Optional.empty());
        var loops = List.of(new PhaseExecutionEvidence.LoopIteration("$/phases/0/steps/0", 2, 3));
        var fault = new PhaseExecutionEvidence.LeaderFault("$/phases/0/steps/0/loop/steps/0",
                loops, "stable-job-id", before, after, raw, List.of("HTTP 404 during new leader startup"));
        var expected = new FlinkHaEvidence.Expected(List.of(new FlinkHaEvidence.DeclaredFault(
                fault.path(), loops, request)), true, true);
        var origins = new SubjectClassOrigins(FlinkHaEvidence.TOKEN_CONTAINER_PATH, List.of(
                new SubjectClassOrigins.ProcessOrigin("jobmanager-2#1", Map.of(
                        FlinkHaEvidence.TOKEN_PROVIDER, List.of(FlinkHaEvidence.TOKEN_CONTAINER_PATH)))),
                Optional.of("one class-load log missing"));
        var evidence = new FlinkHaEvidence(expected, Optional.of(new FlinkHaEvidence.TokenEvidence(
                Optional.of("a".repeat(64)), Optional.of(origins), Optional.of(snapshot),
                List.of("full snapshot incomplete"))), Optional.empty(), FlinkHaEvidence.Outcome.UNCONFIRMED,
                "Synthetic service was saturated");
        var node = new ObjectMapper().createObjectNode();

        FlinkHaEvidenceRenderer.render(node, evidence, List.of(fault));

        assertEquals("unconfirmed", node.path("status").asText());
        assertEquals(2, node.at("/requestedFaults/0/loopIterations/0/iteration").asInt());
        assertEquals("stable-job-id", node.at("/leaderFaults/0/jobId").asText());
        assertEquals("container-1", node.at("/leaderFaults/0/target/runtimeId").asText());
        assertEquals("session-2", node.at("/leaderFaults/0/after/resourceManager/sessionId").asText());
        assertTrue(node.at("/leaderFaults/0/faultState/paused").asBoolean());
        assertFalse(node.at("/leaderFaults/0/healedState/paused").asBoolean());
        assertEquals(3, node.at("/leaderFaults/0/jobAfter/latestRestore/checkpointId").asInt());
        assertEquals("HTTP 404 during new leader startup", node.at("/leaderFaults/0/observationErrors/0").asText());
        assertEquals("fixture saturation retained", node.at("/leaderFaults/0/errors/0").asText());
        assertTrue(node.at("/tokens/snapshot/saturated").asBoolean());
        assertEquals(7, node.at("/tokenEvents/0/requestId").asInt());
        assertEquals(2, node.at("/tokenEvents/0/revision").asInt());
        assertEquals("HTTP 503", node.at("/tokenEvents/0/detail").asText());
        assertEquals("a".repeat(64), node.at("/tokens/pluginSha256").asText());
        assertEquals("jobmanager-2#1", node.at("/tokens/classes/0/process").asText());
        assertEquals("one class-load log missing", node.at("/tokens/classLoadFailure").asText());
    }
}
