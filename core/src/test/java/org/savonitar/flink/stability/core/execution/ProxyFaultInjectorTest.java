package org.savonitar.flink.stability.core.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.runtime.api.KafkaProxyEndpoint;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProxyFaultInjectorTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PATH = "$/phases/1/steps/0";
    private static final String FAULT_ID = "phases-1-steps-0";
    private static final String IMAGE = "quay.io/kroxylicious/kroxylicious:0.21.0";

    @TempDir
    Path control;

    @Test
    void recordsTheDroppedResponseAndTheBrokerAnswerTheClientNeverSaw() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of(
                // The broker's first answer is an error: the filter lets it through, and the
                // client's retry is the one whose successful response is lost.
                """
                {"event":"request-forwarded","claim":1,"transactionalId":"eos-0-3",
                 "producerId":1000,"producerEpoch":3,"committed":true}""",
                """
                {"event":"response-forwarded","claim":1,"error":"NOT_COORDINATOR"}""",
                """
                {"event":"request-forwarded","claim":2,"transactionalId":"eos-0-3",
                 "producerId":1000,"producerEpoch":3,"committed":true}""",
                """
                {"event":"response-dropped","claim":2,"occurrence":1,"error":"NONE",
                 "producerId":1000,"producerEpoch":4,"beforeDeadline":true}"""));

        PhaseExecutionEvidence.NetworkFault fault = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_RESPONSE));

        JsonNode rule = proxy.rule();
        assertEquals(FAULT_ID, rule.get("faultId").textValue());
        assertEquals("end-txn", rule.get("api").textValue());
        assertEquals("commit", rule.get("result").textValue());
        assertEquals("eos", rule.get("transactionalIdPrefix").textValue());
        assertEquals("drop-response", rule.get("action").textValue());
        assertEquals(Duration.ofSeconds(10).toNanos(), rule.get("triggerDeadlineNanos").longValue());
        assertTrue(fault.triggered());
        assertEquals(FAULT_ID, fault.faultId());
        assertEquals(IMAGE, fault.proxyImage());
        assertEquals(List.of("NOT_COORDINATOR"), fault.forwardedErrors());
        PhaseExecutionEvidence.DroppedMessage dropped = fault.dropped().getFirst();
        assertTrue(dropped.beforeDeadline());
        assertEquals("eos-0-3", dropped.transactionalId());
        assertEquals(3, dropped.producerEpoch());
        assertTrue(dropped.committed());
        assertEquals(new PhaseExecutionEvidence.BrokerAnswer("NONE", 1000, (short) 4),
                dropped.brokerAnswer().orElseThrow());
        assertFalse(Files.exists(control.resolve("rules/" + FAULT_ID + ".json")),
                "the rule must be healed");
    }

    @Test
    void attachesTheRetryTheProxySawAfterHealToItsDroppedMessage() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of(
                """
                {"event":"request-forwarded","claim":2,"transactionalId":"eos-0-3",
                 "producerId":1000,"producerEpoch":3,"committed":true}""",
                """
                {"event":"response-dropped","claim":2,"occurrence":1,"timeMillis":5000,
                 "error":"NONE","producerId":1000,"producerEpoch":4,"beforeDeadline":true}"""));
        ProxyFaultInjector injector = proxy.injector();
        PhaseExecutionEvidence.NetworkFault fault = injector.inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_RESPONSE));
        assertEquals(Optional.empty(), fault.dropped().getFirst().retry(),
                "the retry comes a client request timeout after the step ended");

        // The client times out and sends the same EndTxn again, long after the heal.
        proxy.append("""
                {"event":"retry-observed","claim":7,"timeMillis":34000,"clientId":"other"}""");
        proxy.append("""
                {"event":"retry-observed","claim":2,"timeMillis":35000,"clientId":"producer-eos-0-3"}""");
        PhaseExecutionEvidence.NetworkFault completed = injector.withObservedRetries(fault);

        PhaseExecutionEvidence.DroppedMessage dropped = completed.dropped().getFirst();
        assertEquals(2, dropped.claim());
        assertEquals(new PhaseExecutionEvidence.Retry(35_000, "producer-eos-0-3"),
                dropped.retry().orElseThrow());
        assertEquals(fault.withDropped(completed.dropped()), completed,
                "only the dropped messages change");
    }

    @Test
    void aFaultThatMeetsNoTrafficBeforeItsDeadlineIsNotTriggered() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of());

        PhaseExecutionEvidence.NetworkFault fault = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST));

        assertFalse(fault.triggered());
        assertEquals(List.of(), fault.dropped());
        assertTrue(proxy.elapsed.compareTo(Duration.ofSeconds(10)) >= 0,
                "the injector must wait out the trigger deadline: " + proxy.elapsed);
    }

    @Test
    void partialCompletionRetainsEveryDropButStillMissesTheRequestedCount() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of(requestDrop(true)));
        proxy.whileHealing = List.of(requestDrop(false)
                .replace("\"claim\":1", "\"claim\":2")
                .replace("\"occurrence\":1", "\"occurrence\":2"));

        PhaseExecutionEvidence.NetworkFault result = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST, 2));

        assertEquals(2, result.occurrences());
        assertEquals(2, result.dropped().size());
        assertEquals(1, result.droppedBeforeDeadline());
        assertFalse(result.triggered());
        assertTrue(proxy.elapsed.compareTo(Duration.ofSeconds(10)) >= 0);
        assertFalse(Files.exists(control.resolve("rules/" + FAULT_ID + ".json")));
    }

    @Test
    void aDropThatLandsOnlyWhileTheRuleHealsIsReportedButDoesNotTrigger() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of());
        proxy.whileHealing = List.of(
                """
                {"event":"request-forwarded","claim":1,"transactionalId":"eos-0-3",
                 "producerId":1000,"producerEpoch":3,"committed":true}""",
                """
                {"event":"response-dropped","claim":1,"occurrence":1,"error":"NONE",
                 "producerId":1000,"producerEpoch":4,"beforeDeadline":false}""");

        PhaseExecutionEvidence.NetworkFault fault = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_RESPONSE));

        assertEquals(1, fault.dropped().size(), "the late drop still happened and is reported");
        assertFalse(fault.dropped().getFirst().beforeDeadline());
        assertEquals(0, fault.droppedBeforeDeadline());
        assertFalse(fault.triggered());
    }

    @Test
    void aProxyThatNeverConfirmsTheHealFailsTheStep() throws IOException {
        FakeProxy proxy = new FakeProxy(control, List.of(
                """
                {"event":"request-dropped","claim":1,"occurrence":1,"transactionalId":"eos-0-3",
                 "producerId":1000,"producerEpoch":3,"committed":true,"beforeDeadline":true}"""));
        proxy.neverHeals = true;

        IOException failure = assertThrows(IOException.class, () -> proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST)));

        assertTrue(failure.getMessage().contains("did not heal"), failure.getMessage());
        assertFalse(Files.exists(control.resolve("rules/" + FAULT_ID + ".json")));
    }

    @Test
    void aLateDropObservedAfterTheHarnessOversleepsCannotTrigger() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of());
        proxy.afterArm = List.of(requestDrop(false));
        proxy.triggerPause = Duration.ofSeconds(11);

        PhaseExecutionEvidence.NetworkFault result = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST));

        assertEquals(1, result.dropped().size());
        assertFalse(result.triggered());
    }

    @Test
    void aTimelyDropStillCountsWhenTheHarnessObservesItAfterItsOwnWaitExpires() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of());
        proxy.afterArm = List.of(requestDrop(true));
        proxy.triggerPause = Duration.ofSeconds(11);

        PhaseExecutionEvidence.NetworkFault result = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST));

        assertTrue(result.triggered(), "the proxy clock decides when the drop happened");
    }

    @Test
    void missingDeadlineEvidenceCannotTrigger() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of(requestDrop(true)
                .replace(",\"beforeDeadline\":true", "")));

        PhaseExecutionEvidence.NetworkFault result = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST));

        assertFalse(result.triggered());
    }

    @Test
    void aDroppedBrokerErrorCannotProveSuccessfulResponseLoss() throws Exception {
        FakeProxy proxy = new FakeProxy(control, List.of(
                """
                {"event":"request-forwarded","claim":1,"transactionalId":"eos-0-3",
                 "producerId":1000,"producerEpoch":3,"committed":true}""",
                """
                {"event":"response-dropped","claim":1,"occurrence":1,"error":"NOT_COORDINATOR",
                 "producerId":1000,"producerEpoch":3,"beforeDeadline":true}"""));

        PhaseExecutionEvidence.NetworkFault result = proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_RESPONSE));

        assertTrue(result.dropped().getFirst().beforeDeadline());
        assertFalse(result.triggered());
    }

    @Test
    void aRuleTheProxyRejectsFailsTheStep() throws IOException {
        FakeProxy proxy = new FakeProxy(control, List.of());
        proxy.rejectWith = "unsupported api";

        IOException failure = assertThrows(IOException.class, () -> proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST)));

        assertTrue(failure.getMessage().contains("unsupported api"), failure.getMessage());
    }

    @Test
    void aProxyThatNeverArmsTheRuleFailsAfterTheAcknowledgementTimeout() throws IOException {
        FakeProxy proxy = new FakeProxy(control, List.of());
        proxy.silent = true;

        IOException failure = assertThrows(IOException.class, () -> proxy.injector().inject(
                PATH, fault(ExecutableScenarioPlan.NetworkFaultAction.DROP_REQUEST)));

        assertTrue(failure.getMessage().contains("did not arm"), failure.getMessage());
        assertEquals(ProxyFaultInjector.ACKNOWLEDGEMENT_TIMEOUT, proxy.elapsed);
        assertFalse(Files.exists(control.resolve("rules/" + FAULT_ID + ".json")),
                "a failed injection must not leave its rule armed");
    }

    private static String requestDrop(boolean beforeDeadline) {
        return """
                {"event":"request-dropped","claim":1,"occurrence":1,"transactionalId":"eos-0-3",
                 "producerId":1000,"producerEpoch":3,"committed":true,"beforeDeadline":%s}
                """.formatted(beforeDeadline).strip();
    }

    private static ExecutableScenarioPlan.EndTxnFault fault(
            ExecutableScenarioPlan.NetworkFaultAction action) {
        return fault(action, 1);
    }

    private static ExecutableScenarioPlan.EndTxnFault fault(
            ExecutableScenarioPlan.NetworkFaultAction action, int occurrences) {
        return new ExecutableScenarioPlan.EndTxnFault(
                "kafka-proxy",
                Optional.of(ExecutableScenarioPlan.TransactionResult.COMMIT),
                Optional.of("eos"),
                action,
                occurrences,
                Duration.ofSeconds(10));
    }

    /**
     * Plays the fault filter on the injector's own polling thread: every wait advances a fake
     * clock and lets the "proxy" react to the rule files, so the test is deterministic.
     */
    private static final class FakeProxy {
        private final Path control;
        private final List<String> dropEvents;
        private Duration elapsed = Duration.ZERO;
        private JsonNode rule;
        private boolean armed;
        private boolean healed;
        private boolean silent;
        private boolean neverHeals;
        private String rejectWith;
        private List<String> whileHealing = List.of();
        private List<String> afterArm = List.of();
        private Duration triggerPause = Duration.ZERO;

        private FakeProxy(Path control, List<String> dropEvents) throws IOException {
            this.control = control;
            this.dropEvents = new ArrayList<>(dropEvents);
            Files.createDirectories(control.resolve("rules"));
            Files.createDirectories(control.resolve("events"));
        }

        private ProxyFaultInjector injector() {
            return new ProxyFaultInjector(
                    new KafkaProxyEndpoint("kafka-proxy", "kafka-proxy:9092", IMAGE, control),
                    () -> elapsed.toNanos(),
                    this::react);
        }

        private JsonNode rule() {
            return rule;
        }

        private void react(Duration pause) throws InterruptedException {
            elapsed = elapsed.plus(pause);
            if (silent) {
                return;
            }
            Path file = control.resolve("rules/" + FAULT_ID + ".json");
            try {
                if (!armed && Files.exists(file)) {
                    rule = JSON.readTree(Files.readString(file));
                    armed = true;
                    if (rejectWith != null) {
                        append("{\"event\":\"rejected\",\"reason\":\"" + rejectWith + "\"}");
                        return;
                    }
                    append("{\"event\":\"armed\",\"timeMillis\":1}");
                    for (String event : dropEvents) {
                        append(event.replace("\n", ""));
                    }
                } else if (armed && !healed && !Files.exists(file) && !neverHeals) {
                    healed = true;
                    for (String event : whileHealing) {
                        append(event.replace("\n", ""));
                    }
                    append("{\"event\":\"healed\",\"timeMillis\":2}");
                } else if (armed && Files.exists(file) && !afterArm.isEmpty()) {
                    elapsed = elapsed.plus(triggerPause);
                    for (String event : afterArm) {
                        append(event.replace("\n", ""));
                    }
                    afterArm = List.of();
                }
            } catch (IOException failure) {
                throw new AssertionError(failure);
            }
        }

        private void append(String line) throws IOException {
            Files.writeString(control.resolve("events/" + FAULT_ID + ".jsonl"), line + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }
}
