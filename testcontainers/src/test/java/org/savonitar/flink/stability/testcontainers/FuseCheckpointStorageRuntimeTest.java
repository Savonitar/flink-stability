package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.runtime.api.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class FuseCheckpointStorageRuntimeTest {
    @TempDir Path temporary;
    private static final StorageFaultControl.Instance INSTANCE = new StorageFaultControl.Instance(
            "taskmanager-1", "taskmanager-1#1", "container-a", "instance-a", FlinkComponentRole.TASK_MANAGER, 25);

    @Test void retainsAllControlAndIoRecordsWithExactNativeErrnoAndHeldWriteBytes() {
        var operations = parse(line(1, "READY", "HEALTHY", 0, 0) + line(2, "ARM", "ARMED", 1, 0)
                + line(3, "write", "ARMED", -1, 5), true);
        assertEquals(3, operations.size());
        assertEquals("READY", operations.getFirst().operation());
        assertEquals(INSTANCE, operations.getLast().instance());
        assertEquals(-1, operations.getLast().result()); assertEquals(5, operations.getLast().errno());
        assertEquals(0, operations.getLast().returnedBytes());
        assertEquals("ns/job/chk-1/state", operations.getLast().path());
    }

    @Test void activeTailCanBeIncompleteButQuiescentTailCannot() {
        String prefix = line(1, "READY", "HEALTHY", 0, 0);
        assertEquals(1, parse(prefix + "{\"version\":", false).size());
        assertThrows(IllegalStateException.class, () -> parse(prefix + "{\"version\":", true));
        assertThrows(IllegalStateException.class, () -> parse("", true));
    }

    @Test void cumulativeCountersAreRetainedAndCannotDisappearRegressOrCoerce() {
        String ready = counterLine(1, "READY", 0, 0, 0);
        String summary = counterLine(2, "SUMMARY", 2, 1000, 16);
        var operations = parse(ready + summary, true);
        assertEquals(new StorageFaultControl.IoCounter(2, 16), operations.getLast().counters().get("read"));
        assertEquals(1000, operations.getLast().counters().get("getattr").count());
        assertThrows(UnsupportedOperationException.class, () -> operations.getLast().counters().clear());
        for (String invalid : new String[]{
                counterLine(3, "SUMMARY", 1, 1001, 8),
                counterLine(3, "SUMMARY", 2, 1000, 16),
                line(3, "HEAL", "HEALTHY", 0, 0),
                counterLine(3, "SUMMARY", 3, 1001, 24).replace("\"release\"", "\"unknown\""),
                counterLine(3, "SUMMARY", 3, 1001, 24).replace("\"count\":3", "\"count\":3.0"),
                counterLine(3, "SUMMARY", 3, 1001, 24).replace("\"bytes\":0", "\"bytes\":1")})
            assertThrows(IllegalStateException.class, () -> parse(ready + summary + invalid, true));
        assertThrows(IllegalStateException.class, () -> parse(line(1, "SUMMARY", "HEALTHY", 0, 0), true));
        assertThrows(IllegalStateException.class, () -> parse(counterLine(1, "READY", 1, 0, 1), true));
    }

    @Test void overlongPathReceiptIsAnExplicitOperationErrorWithoutInventingJournalFailure() {
        String receipt = line(1, "getattr", "HEALTHY", -1, 36)
                .replace("\"reason\":\"test\"", "\"reason\":\"receipt-path-truncated\",\"pathTruncated\":true");
        assertTrue(parse(receipt, true).getFirst().pathTruncated());
        assertThrows(IllegalStateException.class, () -> parse(receipt.replace("\"errno\":36", "\"errno\":5"), true));
        assertThrows(IllegalStateException.class, () -> parse(receipt.replace("\"pathTruncated\":true", "\"pathTruncated\":\"true\""), true));
        assertTrue(parse(line(1, "getattr", "HEALTHY", 0, 0), true).getFirst().counters().isEmpty(), "Historical receipts remain readable");
    }

    @Test void foreignIdentityGapsDuplicatesAndExplicitIncompleteNeverBecomeComplete() {
        String valid = line(1, "READY", "HEALTHY", 0, 0);
        for (String changed : new String[]{valid.replace("instance-a", "instance-b"), valid.replace("\"daemonPid\":25", "\"daemonPid\":26"),
                valid.replace("\"attemptId\":\"attempt\"", "\"attemptId\":\"other\""), valid.replace("\"storageId\":\"storage\"", "\"storageId\":\"foreign\""),
                valid.replace("\"namespace\":\"ns\"", "\"namespace\":\"elsewhere\""), valid.replace("\"sequence\":1", "\"sequence\":2"),
                valid + valid, valid.replace("\"journalComplete\":true", "\"journalComplete\":false"),
                valid.replace("\"sequence\":1", "\"sequence\":1,\"sequence\":1")})
            assertThrows(IllegalStateException.class, () -> parse(changed, true));
    }

    @Test void explicitDaemonBoundsMatchCaptureBoundsAndRecordOverflowFailsClosed() {
        assertEquals("--max-journal-bytes 16777216 --max-journal-records 25000",
                FuseCheckpointStorageRuntime.journalLimitArguments());
        StringBuilder journal = new StringBuilder();
        for (int sequence = 1; sequence <= 25_001; sequence++)
            journal.append(line(sequence, "getattr", "HEALTHY", 0, 0));
        assertTrue(journal.length() < FuseCheckpointStorageRuntime.MAX_JOURNAL_BYTES);
        var overflow = assertThrows(IllegalStateException.class, () -> parse(journal.toString(), true));
        assertTrue(overflow.getMessage().contains("record budget"));
    }

    @Test void malformedUtf8AndEvidenceBudgetsFailClosed() {
        assertThrows(IllegalStateException.class, () -> FuseCheckpointStorageRuntime.parseJournal(new byte[]{(byte)0xc0, (byte)0x80}, INSTANCE, "attempt", "storage", "ns", false));
        assertThrows(IllegalStateException.class, () -> FuseCheckpointStorageRuntime.parseJournal(new byte[FuseCheckpointStorageRuntime.MAX_JOURNAL_BYTES + 1], INSTANCE, "attempt", "storage", "ns", true));
        assertThrows(IllegalStateException.class, () -> parse(line(1, "READY", "HEALTHY", 0, 0).replace("reason", "x".repeat(8192)), true));
    }

    @Test void verifiesEveryPreparedByteAndRejectsSymlinkOrChangedBundle() throws Exception {
        Path root = temporary.toRealPath().resolve("prepared"); Files.createDirectories(root.resolve("bundle"));
        Files.writeString(root.resolve("build-evidence.json"), "{}");
        for (String name : FuseCheckpointStorage.REQUIRED_FILES) Files.writeString(root.resolve("bundle").resolve(name), name);
        var hashes = new java.util.HashMap<String,String>();
        for (String name : FuseCheckpointStorage.REQUIRED_FILES) hashes.put(name, Digests.sha256(name.getBytes(StandardCharsets.UTF_8)));
        var target = new FuseCheckpointStorage(root, Digests.sha256("{}".getBytes(StandardCharsets.UTF_8)),
                "sha256:" + "a".repeat(64), FuseCheckpointStorage.SUPPORTED_PLATFORM, hashes);
        assertDoesNotThrow(() -> FuseCheckpointStorageRuntime.verifyBundle(target));
        Path daemon = root.resolve("bundle/checkpoint-fuse");
        Files.writeString(daemon, "changed");
        assertThrows(IllegalArgumentException.class, () -> FuseCheckpointStorageRuntime.verifyBundle(target));
        Files.delete(daemon); Files.createSymbolicLink(daemon, root.resolve("bundle/probe.jar"));
        assertThrows(IllegalArgumentException.class, () -> FuseCheckpointStorageRuntime.verifyBundle(target));
    }

    @Test void localSocketRequiredBeforeTransferringPreparedHelperBytes() {
        assertDoesNotThrow(() -> FuseCheckpointStorageRuntime.requireLocalEndpoint(java.net.URI.create("unix:///var/run/docker.sock")));
        for (String value : new String[]{"tcp://localhost:2375", "https://docker.example:2376", "unix://foreign/path", "unix:///sock?x=1"})
            assertThrows(IllegalArgumentException.class, () -> FuseCheckpointStorageRuntime.requireLocalEndpoint(java.net.URI.create(value)));
    }

    @Test void nativeMountTypeAndPermissionOptionsMatchActualPr3Format() {
        String line = "75 116 0:52 / /flink/checkpoints rw,nosuid,nodev,relatime - fuse /dev/fuse rw,user_id=0,group_id=0,default_permissions,allow_other";
        assertEquals(line, FuseCheckpointStorageRuntime.verifiedMountLine(line + "\n"));
        for (String changed : new String[]{line.replace(" - fuse ", " - ext4 "), line.replace("/flink/checkpoints", "/flink/evidence"),
                line.replace(",allow_other", ""), line.replace("user_id=0", "user_id=9999"), line + "\n" + line})
            assertThrows(IllegalStateException.class, () -> FuseCheckpointStorageRuntime.verifiedMountLine(changed));
    }

    @Test void supervisorRejectsAnythingExceptGeneratedEvidencePath() {
        assertTrue(FuseCheckpointStorageRuntime.supervisor("/flink/evidence/flink-instance-abc.log").startsWith("exec tail "));
        for (String path : new String[]{"/flink/checkpoints/output", "/flink/evidence/../raw", "/flink/evidence/a;id", "/flink/evidence/a'"})
            assertThrows(IllegalArgumentException.class, () -> FuseCheckpointStorageRuntime.supervisor(path));
    }

    private static java.util.List<StorageFaultControl.Operation> parse(String json, boolean quiescent) {
        return FuseCheckpointStorageRuntime.parseJournal(json.getBytes(StandardCharsets.UTF_8), INSTANCE, "attempt", "storage", "ns", quiescent);
    }
    private static String counterLine(int sequence, String operation, long reads, long metadata, long bytes) {
        String counters = java.util.stream.Stream.of("read", "getattr", "readdir", "access", "flush", "release")
                .map(name -> "\"" + name + "\":{\"count\":" + (name.equals("read") ? reads : name.equals("getattr") ? metadata : 0)
                        + ",\"bytes\":" + (name.equals("read") ? bytes : 0) + "}")
                .collect(java.util.stream.Collectors.joining(","));
        return line(sequence, operation, "HEALTHY", 0, 0).replace("\"reason\":\"test\"", "\"reason\":\"test\",\"counters\":{" + counters + "}");
    }
    private static String line(int sequence, String operation, String state, long result, int errno) {
        return "{\"version\":1,\"sequence\":" + sequence + ",\"timestampMs\":100,\"monotonicNs\":200,\"daemonPid\":25,"
                + "\"attemptId\":\"attempt\",\"storageId\":\"storage\",\"instanceId\":\"instance-a\",\"namespace\":\"ns\","
                + "\"faultId\":null,\"revision\":0,\"state\":\"" + state + "\",\"journalComplete\":true,"
                + "\"operation\":\"" + operation + "\",\"path\":\"ns/job/chk-1/state\",\"faultOperation\":null,"
                + "\"result\":" + result + ",\"errno\":" + errno + ",\"requestedBytes\":10,\"returnedBytes\":0,\"datasync\":null,\"reason\":\"test\"}\n";
    }
}
