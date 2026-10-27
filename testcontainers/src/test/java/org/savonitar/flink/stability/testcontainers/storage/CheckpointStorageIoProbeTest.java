package org.savonitar.flink.stability.testcontainers.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Ordinary filesystem checks; FUSE errno and permission qualification require the isolated live probe. */
class CheckpointStorageIoProbeTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir Path temporary;
    private Path view;
    private Path raw;
    private Path evidence;

    @BeforeEach void roots() throws Exception {
        Path canonical = temporary.toRealPath();
        view = Files.createDirectory(canonical.resolve("view"));
        raw = Files.createDirectory(canonical.resolve("raw"));
        evidence = Files.createDirectory(canonical.resolve("evidence"));
    }

    @Test void healthyOperationsRemainVisibleToAnIndependentSession() throws Exception {
        try (Session first = session(); Session second = session()) {
            ok(first.call("mkdir", "nested"));
            Map<String, Object> created = first.call("create", "nested/checkpoint", encoded("prefix"));
            ok(created);
            assertEquals(6, created.get("writtenBytes"));
            assertEquals(encoded("prefix"), second.call("read", "nested/checkpoint").get("base64"));
            ok(first.call("open", "checkpoint", "nested/checkpoint"));
            ok(first.call("force", "checkpoint", "false"));
            ok(first.call("force", "checkpoint", "true"));
            ok(first.call("close", "checkpoint"));
            ok(first.call("rename", "nested/checkpoint", "nested/renamed"));
            assertEquals(List.of("renamed"), second.call("list", "nested").get("entries"));
            Map<String, Object> stat = second.call("stat", "nested/renamed");
            ok(stat);
            assertEquals(6L, stat.get("size"));
            assertEquals(true, stat.get("regularFile"));
            ok(second.call("delete", "nested/renamed"));
            assertEquals(List.of(), first.call("list", "nested").get("entries"));
            ok(first.call("delete", "nested"));
            assertEquals(List.of(), second.call("list", "@").get("entries"));
        }
    }

    @Test void heldWritesUseTheOriginalDescriptorAfterRenameAndPathReplacement() throws Exception {
        try (Session session = session()) {
            ok(session.call("create", "active", encoded("old")));
            Map<String, Object> opened = session.call("open", "held", "active");
            ok(opened);
            assertEquals(3L, opened.get("position"));
            ok(session.call("rename", "active", "retained"));
            ok(session.call("create", "active", encoded("new")));
            Map<String, Object> write = session.call("write", "held", encoded("-suffix"));
            ok(write);
            assertEquals(3L, write.get("positionBefore"));
            assertEquals(7, write.get("writtenBytes"));
            assertEquals(10L, write.get("positionAfter"));
            assertEquals(encoded("old-suffix"), session.call("read", "retained").get("base64"));
            assertEquals(encoded("new"), session.call("read", "active").get("base64"));
            ok(session.call("close", "held"));
            assertEquals("rejected", session.call("write", "held", encoded("x")).get("status"));
        }
    }

    @Test void readProvidesAnIndependentKnownDigest() throws Exception {
        Files.writeString(view.resolve("known"), "abc");
        try (Session session = session()) {
            Map<String, Object> read = session.call("read", "known");
            ok(read);
            assertEquals(3, read.get("readBytes"));
            assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", read.get("sha256"));
            assertEquals("YWJj", read.get("base64"));
        }
    }

    @Test void createNewCannotOverwriteExistingBytesAndRetainsTheActualException() throws Exception {
        Files.writeString(view.resolve("checkpoint"), "untouched");
        try (Session session = session()) {
            Map<String, Object> result = session.call("create", "checkpoint", encoded("replacement"));
            assertEquals("io-exception", result.get("status"));
            assertEquals("open", result.get("phase"));
            assertEquals("java.nio.file.FileAlreadyExistsException", result.get("exceptionClass"));
            assertTrue(((String) result.get("exceptionMessage")).contains("checkpoint"));
            assertEquals(false, result.get("exceptionMessageTruncated"));
            assertEquals("untouched", Files.readString(view.resolve("checkpoint")));
            ok(session.call("hello"));
        }
    }

    @Test void rawAccessNeverFallsBackToTheViewAndEvidenceUsesItsSeparateRoot() throws Exception {
        Files.writeString(view.resolve("existing"), "view-only");
        try (Session session = session()) {
            Map<String, Object> missing = session.call("raw-read", "existing");
            assertEquals("io-exception", missing.get("status"));
            assertEquals("java.nio.file.NoSuchFileException", missing.get("exceptionClass"));
            assertEquals(true, missing.get("rawAccessProbe"));
            ok(session.call("raw-write", "raw-only", encoded("raw")));
            assertEquals("raw", Files.readString(raw.resolve("raw-only")));
            assertFalse(Files.exists(view.resolve("raw-only")));
            Map<String, Object> logged = session.call("evidence-write", "receipt", encoded("evidence"));
            ok(logged);
            assertEquals(true, logged.get("evidenceAccessProbe"));
            assertEquals("evidence", Files.readString(evidence.resolve("receipt")));
            assertFalse(Files.exists(view.resolve("receipt")));
            assertFalse(Files.exists(raw.resolve("receipt")));
        }
    }

    @Test void traversalAndNonCanonicalPathsCannotTouchTheOutsideFile() throws Exception {
        Path outside = view.getParent().resolve("outside");
        Files.writeString(outside, "preserved");
        try (Session session = session()) {
            for (String path : List.of("../outside", "a/../../outside", "/outside", "./outside", "a//b",
                    "a/", "a\\b", "", "@", "a".repeat(257))) {
                assertEquals("rejected", session.call("create", path, encoded("bad")).get("status"), path);
                assertEquals("rejected", session.call("raw-write", path, encoded("bad")).get("status"), path);
                assertEquals("rejected", session.call("evidence-write", path, encoded("bad")).get("status"), path);
            }
            assertEquals("preserved", Files.readString(outside));
            assertEquals(List.of(), session.call("list", "@").get("entries"));
        }
    }

    @Test void symbolicFilesAndParentDirectoriesAreRejectedForAllRoots() throws Exception {
        Path outside = Files.createDirectory(view.getParent().resolve("outside"));
        Files.writeString(outside.resolve("existing"), "preserved");
        for (Path root : List.of(view, raw, evidence)) {
            Files.createSymbolicLink(root.resolve("linked-dir"), outside);
            Files.createSymbolicLink(root.resolve("linked-file"), outside.resolve("existing"));
        }
        try (Session session = session()) {
            assertEquals("rejected", session.call("read", "linked-file").get("status"));
            assertEquals("rejected", session.call("read", "linked-dir/existing").get("status"));
            assertEquals("rejected", session.call("raw-read", "linked-file").get("status"));
            assertEquals("rejected", session.call("raw-write", "linked-dir/new", encoded("x")).get("status"));
            assertEquals("rejected", session.call("evidence-write", "linked-dir/new", encoded("x")).get("status"));
            assertEquals("preserved", Files.readString(outside.resolve("existing")));
            assertFalse(Files.exists(outside.resolve("new")));
        }
    }

    @Test void rootsMustBeDisjointRealDirectoriesAndWatchdogIsBounded() throws Exception {
        Path linked = Files.createSymbolicLink(view.getParent().resolve("linked"), view);
        assertThrows(IllegalArgumentException.class, () -> new CheckpointStorageIoProbe(linked, raw, evidence, 120));
        assertThrows(IllegalArgumentException.class, () -> new CheckpointStorageIoProbe(view, view.resolve("child"), evidence, 120));
        assertThrows(IllegalArgumentException.class, () -> new CheckpointStorageIoProbe(view, raw, view.resolve("logs"), 120));
        assertThrows(IllegalArgumentException.class, () -> new CheckpointStorageIoProbe(view, raw, evidence, 0));
        assertThrows(IllegalArgumentException.class, () -> new CheckpointStorageIoProbe(view, raw, evidence, 121));
    }

    @Test void payloadsAreBoundedAndCanonicalBeforeAnyFileIsCreated() throws Exception {
        try (Session session = session()) {
            for (String payload : List.of("", "YQ", "@@@@", "Y Q==",
                    Base64.getEncoder().encodeToString(new byte[CheckpointStorageIoProbe.MAX_PAYLOAD_BYTES + 1]))) {
                assertEquals("rejected", session.call("create", "never-created", payload).get("status"));
                assertFalse(Files.exists(view.resolve("never-created")));
            }
            String maximum = Base64.getEncoder().encodeToString(new byte[CheckpointStorageIoProbe.MAX_PAYLOAD_BYTES]);
            ok(session.call("create", "maximum", maximum));
            assertEquals(CheckpointStorageIoProbe.MAX_PAYLOAD_BYTES, Files.size(view.resolve("maximum")));
        }
    }

    @Test void oversizedReadAndDirectoryListingAreIncompleteNotSuccessful() throws Exception {
        Files.write(view.resolve("oversized"), new byte[CheckpointStorageIoProbe.MAX_READ_BYTES + 1]);
        Path large = Files.createDirectory(view.resolve("large"));
        for (int index = 0; index <= CheckpointStorageIoProbe.MAX_DIRECTORY_ENTRIES; index++)
            Files.createFile(large.resolve("file-" + index));
        try (Session session = session()) {
            Map<String, Object> read = session.call("read", "oversized");
            assertEquals("rejected", read.get("status"));
            assertFalse(read.containsKey("sha256"));
            Map<String, Object> list = session.call("list", "large");
            assertEquals("rejected", list.get("status"));
            assertFalse(list.containsKey("entries"));
        }
    }

    @Test void handleLimitDoesNotReplaceAnExistingDescriptorAndCloseReleasesCapacity() throws Exception {
        Files.writeString(view.resolve("file"), "content");
        try (Session session = session()) {
            for (int index = 0; index < CheckpointStorageIoProbe.MAX_HANDLES; index++)
                ok(session.call("open", "h" + index, "file"));
            assertEquals("rejected", session.call("open", "extra", "file").get("status"));
            assertEquals("rejected", session.call("open", "h0", "file").get("status"));
            ok(session.call("close", "h0"));
            ok(session.call("open", "extra", "file"));
            assertEquals("rejected", session.call("force", "extra", "yes").get("status"));
            ok(session.call("force", "extra", "true"));
        }
    }

    @Test void requestIdsAndCommandCountAreBounded() throws Exception {
        try (CheckpointStorageIoProbe probe = probe()) {
            assertEquals("rejected", probe.execute("2\thello").get("status"));
            ok(probe.execute("1\thello"));
            assertEquals("rejected", probe.execute("1\thello").get("status"));
            assertEquals("rejected", probe.execute("02\thello").get("status"));
            ok(probe.execute("2\thello"));
            assertEquals("rejected", probe.execute("3\tunknown").get("status"));
            ok(probe.execute("4\thello"));
        }
        try (Session session = session()) {
            for (int index = 0; index < CheckpointStorageIoProbe.MAX_COMMANDS; index++) ok(session.call("hello"));
            assertEquals("rejected", session.call("hello").get("status"));
        }
    }

    @Test void streamRejectsTruncationNonAsciiAndOversizedCommandsWithoutExecutingThem() throws Exception {
        for (byte[] invalid : List.of("1\tcreate\tforbidden\teA==".getBytes(StandardCharsets.US_ASCII),
                "1\thellö\n".getBytes(StandardCharsets.UTF_8),
                ("1\t" + "a".repeat(CheckpointStorageIoProbe.MAX_COMMAND_BYTES) + "\n").getBytes(StandardCharsets.US_ASCII))) {
            try (CheckpointStorageIoProbe probe = probe()) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                probe.serve(new ByteArrayInputStream(invalid), output);
                var receipt = JSON.readTree(output.toByteArray());
                assertEquals("rejected", receipt.get("status").asText());
                assertEquals("protocol", receipt.get("operation").asText());
                assertFalse(Files.exists(view.resolve("forbidden")));
            }
        }
    }

    @Test void streamProducesParseableJsonAndStopsAfterQuit() throws Exception {
        Files.createFile(view.resolve("quote\"and\nnewline"));
        try (CheckpointStorageIoProbe probe = probe()) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            probe.serve(new ByteArrayInputStream("1\tlist\t@\r\n2\tquit\n3\tcreate\tignored\teA==\n"
                    .getBytes(StandardCharsets.US_ASCII)), output);
            List<String> lines = output.toString(StandardCharsets.UTF_8).lines().toList();
            assertEquals(2, lines.size());
            assertEquals("quote\"and\nnewline", JSON.readTree(lines.get(0)).get("entries").get(0).asText());
            assertEquals("quit", JSON.readTree(lines.get(1)).get("operation").asText());
            assertFalse(Files.exists(view.resolve("ignored")));
        }
    }

    @Test void helloContainsBoundedProcessIdentityWithoutEnvironmentData() throws Exception {
        try (Session session = session()) {
            Map<String, Object> hello = session.call("hello");
            ok(hello);
            assertEquals(ProcessHandle.current().pid(), hello.get("pid"));
            assertEquals(System.getProperty("java.version"), hello.get("javaVersion"));
            assertEquals(120, hello.get("timeoutSeconds"));
            assertEquals(124, hello.get("watchdogExitCode"));
            assertFalse(hello.containsKey("environment"));
            if (Boolean.TRUE.equals(hello.get("linuxIdentityAvailable"))) {
                assertEquals(4, ((List<?>) hello.get("uid")).size());
                assertEquals(4, ((List<?>) hello.get("gid")).size());
                assertTrue(((String) hello.get("capEff")).matches("[0-9a-fA-F]+"));
            }
        }
    }

    @Test void processWatchdogTerminatesAnIdleSessionWithoutWaitingForInput() throws Exception {
        Process process = launch("1");
        try {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "The probe watchdog did not terminate its process");
            assertEquals(CheckpointStorageIoProbe.WATCHDOG_EXIT_CODE, process.exitValue());
        } finally {
            process.destroyForcibly();
        }
    }

    @Test void normalInputEofClosesTheSessionWithoutWaitingForTheWatchdog() throws Exception {
        Files.writeString(view.resolve("held"), "data");
        Process process = launch("120");
        try {
            process.getOutputStream().write("1\thello\n2\topen\theld\theld\n".getBytes(StandardCharsets.US_ASCII));
            process.getOutputStream().close();
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            assertEquals(0, process.exitValue(), () -> new String(readStderr(process), StandardCharsets.UTF_8));
            List<String> receipts = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).lines().toList();
            assertEquals(2, receipts.size());
            for (String receipt : receipts) assertEquals("ok", JSON.readTree(receipt).get("status").asText());
        } finally {
            process.destroyForcibly();
        }
    }

    private Process launch(String timeout) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), CheckpointStorageIoProbe.class.getName(),
                "--root", view.toString(), "--raw-root", raw.toString(), "--evidence-root", evidence.toString(),
                "--timeout-seconds", timeout).start();
    }

    private static byte[] readStderr(Process process) {
        try { return process.getErrorStream().readAllBytes(); }
        catch (Exception failure) { throw new AssertionError(failure); }
    }

    private CheckpointStorageIoProbe probe() throws Exception {
        return new CheckpointStorageIoProbe(view, raw, evidence, 120);
    }

    private Session session() throws Exception { return new Session(probe()); }
    private static String encoded(String text) { return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8)); }
    private static void ok(Map<String, Object> receipt) { assertEquals("ok", receipt.get("status"), receipt.toString()); }

    private static final class Session implements AutoCloseable {
        private final CheckpointStorageIoProbe probe;
        private int id;
        private Session(CheckpointStorageIoProbe probe) { this.probe = probe; }
        private Map<String, Object> call(String... fields) { return probe.execute(++id + "\t" + String.join("\t", fields)); }
        @Override public void close() throws Exception { probe.close(); }
    }
}
