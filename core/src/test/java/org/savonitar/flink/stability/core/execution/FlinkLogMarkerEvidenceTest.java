package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.*;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class FlinkLogMarkerEvidenceTest {
    @TempDir Path temporary;
    private static final FlinkLogMarker REQUIRED = new FlinkLogMarker("patched", "CUSTOM-FIX", "taskmanager", true);

    @Test void firstMatchingLineIsBoundToEveryIncarnationAndScope() throws Exception {
        var first = component("taskmanager-1", "tm-old", "taskmanager-1#1");
        var second = component("taskmanager-1", "tm-new", "taskmanager-1#2");
        var jm = component("jobmanager-1", "jm", "jobmanager-1#1");
        var logs = List.of(log(first, "before\nINFO CUSTOM-FIX ready\nINFO CUSTOM-FIX later\n"),
                log(second, "INFO CUSTOM-FIX replacement\n"), log(jm, "no matching marker\n"));
        var result = FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(first, second, jm), logs, true);
        assertTrue(result.requiredConfirmed());
        assertEquals(List.of("tm-old", "tm-new"), result.observations().stream().map(FlinkLogMarkerEvidence.Observation::runtimeId).toList());
        assertEquals("INFO CUSTOM-FIX ready", result.observations().getFirst().firstMatch());
        assertEquals("CUSTOM-FIX", result.observations().getFirst().matchedText());
        assertEquals("INFO CUSTOM-FIX replacement", result.observations().getLast().firstMatch());
    }

    @Test void missingReplacementDuplicateBindingsAndUnfencedLogsCannotProveRequiredMarkers() throws Exception {
        var first = component("taskmanager-1", "tm-old", "taskmanager-1#1");
        var replacement = component("taskmanager-1", "tm-new", "taskmanager-1#2");
        var observed = log(first, "CUSTOM-FIX\n");
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(first, replacement), List.of(observed), true).requiredConfirmed());
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(first), List.of(observed), false).requiredConfirmed());
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(first), List.of(observed, observed), true).requiredConfirmed());
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(first,
                component("taskmanager-1", "tm-borrowed", "taskmanager-1#1")), List.of(observed), true).requiredConfirmed());
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(first),
                List.of(observed, log(replacement, "CUSTOM-FIX\n")), true).requiredConfirmed());
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(), List.of(), true).requiredConfirmed());
    }

    @Test void regexMatchesIndividualLinesAndOptionalAbsenceRemainsDiagnostic() throws Exception {
        var component = component("jobmanager-1", "jm", "jobmanager-1#1");
        var marker = new FlinkLogMarker("across-lines", "START.*END", "jobmanager", false);
        var result = FlinkLogMarkerEvidence.collect(List.of(marker), List.of(component), List.of(log(component, "START\nEND\n")), true);
        assertTrue(result.requiredConfirmed());
        assertNull(result.observations().getFirst().firstMatch());
        assertEquals("No match", result.observations().getFirst().absence());
    }

    @Test void replacementCannotBorrowPredecessorOutputThroughPathsOrHardLinks() throws Exception {
        var first = component("taskmanager-1", "tm-old", "taskmanager-1#1");
        var replacement = component("taskmanager-1", "tm-new", "taskmanager-1#2");
        var observed = log(first, "CUSTOM-FIX predecessor\n");
        var directory = Files.createDirectory(temporary.resolve("alias"));
        var hardLink = Files.createLink(temporary.resolve("replacement.log"), observed.path());
        for (Path path : List.of(observed.path(), directory.resolve("..").resolve(observed.path().getFileName()), hardLink)) {
            var borrowed = new FlinkComponentLog(replacement.classLoadProcess().orElseThrow(), path, false, Optional.empty());
            var result = FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(first, replacement), List.of(observed, borrowed), true);
            assertFalse(result.requiredConfirmed(), path.toString());
            assertTrue(result.observations().stream().allMatch(item -> item.firstMatch() == null));
            assertTrue(result.observations().stream().allMatch(item -> item.absence().contains("share one retained output file")));
        }
    }

    @Test void malformedAndDuplicateDeclarationsFailBeforeAnyRuntime() {
        for (String regex : List.of("", " ", "[")) assertThrows(IllegalArgumentException.class,
                () -> new FlinkLogMarker("name", regex, "taskmanager", true));
        assertThrows(IllegalArgumentException.class, () -> new FlinkLogMarker("bad name", "x", "taskmanager", false));
        assertThrows(IllegalArgumentException.class, () -> new FlinkLogMarker("name", "x", "worker", false));
        assertThrows(IllegalArgumentException.class, () -> FlinkLogMarker.validate(List.of(REQUIRED, REQUIRED)));
    }

    @Test void unreadableLinkedAndTruncatedAbsentLogsAreExplicit() throws Exception {
        var component = component("taskmanager-1", "tm", "taskmanager-1#1");
        var log = log(component, "ordinary output\n");
        var truncated = new FlinkComponentLog(log.process(), log.path(), true, Optional.empty());
        var result = FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(component), List.of(truncated), true);
        assertFalse(result.requiredConfirmed());
        assertEquals("Log capture was truncated", result.observations().getFirst().absence());
        Files.delete(log.path());
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(component), List.of(log), true).requiredConfirmed());
        var target = Files.writeString(temporary.resolve("target.log"), "CUSTOM-FIX\n");
        Files.createSymbolicLink(log.path(), target);
        assertFalse(FlinkLogMarkerEvidence.collect(List.of(REQUIRED), List.of(component), List.of(log), true).requiredConfirmed());
    }

    private FlinkComponentLog log(FlinkComponentProvisioningEvidence component, String text) throws Exception {
        String process = component.classLoadProcess().orElseThrow();
        return new FlinkComponentLog(process, Files.writeString(temporary.resolve(process + ".log"), text), false, Optional.empty());
    }
    private static FlinkComponentProvisioningEvidence component(String name, String id, String process) {
        return FlinkComponentProvisioningEvidence.verified(name,
                name.startsWith("taskmanager") ? FlinkComponentRole.TASK_MANAGER : FlinkComponentRole.JOB_MANAGER,
                id, "flink:2.2.0", "sha256:" + "a".repeat(64), "b".repeat(64), "c".repeat(64), List.of())
                .withProcessConfiguration(Map.of(), process);
    }
}
