package org.savonitar.flink.stability.testcontainers.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointStateInspectorTest {
    private static final String JOB = "a".repeat(32);
    @TempDir Path temporary;
    private Path root;
    private Path directory;
    private Path state;

    @BeforeEach
    void setup() throws Exception {
        root = temporary.toRealPath().resolve("view");
        directory = Files.createDirectories(root.resolve(JOB).resolve("chk-7"));
        Files.write(directory.resolve("_metadata"), new byte[]{1, 2, 3});
        state = Files.write(directory.resolve("keyed-state"), new byte[]{4, 5, 6, 7});
    }

    @Test
    void readsAndHashesOnlyMetadataSelectedKeyedFilesWithExactIdentity() throws Exception {
        // A source/sink file alongside it must not become selected evidence.
        Files.writeString(directory.resolve("unrelated-operator-state"), "other");
        var receipt = inspect(metadata(List.of(file(state, 4))));
        assertEquals(7, receipt.checkpointId());
        assertEquals(JOB, receipt.jobId());
        assertEquals(CheckpointStateInspector.OPERATOR_UID, receipt.operatorUid());
        assertEquals(1, receipt.files().size());
        assertEquals(state.toString(), receipt.files().get(0).path());
        assertEquals(digest(new byte[]{4, 5, 6, 7}), receipt.files().get(0).sha256());
        assertEquals(digest(new byte[]{1, 2, 3}), receipt.metadata().sha256());
        var json = new ObjectMapper().readTree(receipt.toJson());
        assertEquals(1, json.path("version").intValue());
        assertEquals(7, json.path("checkpointId").longValue());
        assertEquals(4, json.path("files").get(0).path("size").longValue());
    }

    @Test
    void boundedReaderReceivesExactMetadataAndCheckpointPointer() throws Exception {
        CheckpointStateInspector.inspect(root, JOB, 7, (bytes, pointer) -> {
            assertArrayEquals(new byte[]{1, 2, 3}, bytes);
            assertEquals(directory.toUri().toString(), pointer);
            return metadata(List.of(file(state, 4)));
        });
    }

    @Test
    void releasedFlinkMetadataRoundTripPreservesUidAndAbsoluteKeyedFileHandle() throws Exception {
        writeReleasedMetadata(new FileStateHandle(new org.apache.flink.core.fs.Path(state.toUri()), 4));
        var receipt = CheckpointStateInspector.inspect(root, JOB, 7, CheckpointStateInspector::readMetadata);
        assertEquals(CheckpointStateInspector.OPERATOR_UID, receipt.operatorUid());
        assertEquals(List.of(state.toString()), receipt.files().stream().map(CheckpointStateInspector.FileEvidence::path).toList());
        assertEquals(digest(new byte[]{4, 5, 6, 7}), receipt.files().get(0).sha256());
    }

    @Test
    void releasedFlinkRelativeFileHandleResolvesAgainstExactCheckpointPointer() throws Exception {
        writeReleasedMetadata(new RelativeFileStateHandle(new org.apache.flink.core.fs.Path(state.toUri()), state.getFileName().toString(), 4));
        var receipt = CheckpointStateInspector.inspect(root, JOB, 7, CheckpointStateInspector::readMetadata);
        assertEquals(state.toString(), receipt.files().get(0).path());
    }

    @Test
    void releasedFlinkInlineStateDoesNotBecomeMaterializationEvidence() throws Exception {
        writeReleasedMetadata(new ByteStreamStateHandle("inline", new byte[]{4, 5, 6, 7}));
        IOException failure = assertThrows(IOException.class,
                () -> CheckpointStateInspector.inspect(root, JOB, 7, CheckpointStateInspector::readMetadata));
        assertTrue(failure.getMessage().contains("Inline"));
    }

    private void writeReleasedMetadata(StreamStateHandle delegate) throws Exception {
        var operator = new OperatorState("Keyed Prefix v1", CheckpointStateInspector.OPERATOR_UID, new OperatorID(), 1, 128);
        operator.putState(0, OperatorSubtaskState.builder().setManagedKeyedState(StateObjectCollection.singleton(
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(0, 0), new long[]{0}), delegate))).build());
        var metadata = new CheckpointMetadata(7, List.of(operator), List.of());
        try (var output = Files.newOutputStream(directory.resolve("_metadata"))) {
            Checkpoints.storeCheckpointMetadata(metadata, output);
        }
    }

    @Test
    void missingWrongOrRepeatedKeyedIdentityCannotProduceEvidence() {
        assertThrows(IOException.class, () -> inspect(new CheckpointStateInspector.Metadata(8,
                CheckpointStateInspector.OPERATOR_UID, List.of(file(state, 4)))));
        assertThrows(IOException.class, () -> inspect(new CheckpointStateInspector.Metadata(7,
                "source", List.of(file(state, 4)))));
        assertThrows(IOException.class, () -> inspect(metadata(List.of())));
        assertThrows(IOException.class, () -> inspect(metadata(List.of(file(state, 4), file(state, 4)))));
    }

    @Test
    void rejectsReferencesOutsideExactCheckpointWithoutReadingThem() throws Exception {
        for (URI path : List.of(root.resolve(JOB).resolve("chk-8/state").toUri(),
                root.resolve("other-job/chk-7/state").toUri(), directory.resolve("../state").toUri(),
                URI.create("file://host/checkpoint/state"), URI.create("https://example.invalid/state"))) {
            assertThrows(IOException.class, () -> inspect(metadata(List.of(new CheckpointStateInspector.ManagedFile(path, 4)))));
        }
        assertThrows(IOException.class, () -> inspect(metadata(List.of(file(directory.resolve("_metadata"), 3)))));
    }

    @Test
    void rejectsSymlinkFilesAndAncestorDirectories() throws Exception {
        Path alias = directory.resolve("alias");
        Files.createSymbolicLink(alias, state);
        assertThrows(IOException.class, () -> inspect(metadata(List.of(file(alias, 4)))));
        Path outside = Files.createDirectory(temporary.toRealPath().resolve("other"));
        Files.createSymbolicLink(directory.resolve("subdir"), outside);
        assertThrows(IOException.class, () -> inspect(metadata(List.of(file(directory.resolve("subdir/state"), 4)))));
        Path aliasedRoot = temporary.toRealPath().resolve("root-alias");
        Files.createSymbolicLink(aliasedRoot, root);
        assertThrows(IOException.class, () -> CheckpointStateInspector.inspect(aliasedRoot, JOB, 7,
                (bytes, pointer) -> metadata(List.of(file(state, 4)))));
    }

    @Test
    void rejectsMetadataAndFileLimitsBeforeLoadingOrReadingState() throws Exception {
        Files.write(directory.resolve("_metadata"), new byte[CheckpointStateInspector.MAX_METADATA_BYTES + 1]);
        assertThrows(IOException.class, () -> CheckpointStateInspector.inspect(root, JOB, 7,
                (bytes, pointer) -> { fail("oversized metadata must never reach deserializer"); return null; }));
        Files.write(directory.resolve("_metadata"), new byte[]{1});
        assertThrows(IOException.class, () -> inspect(metadata(List.of(file(state, 0)))));
        assertThrows(IOException.class, () -> inspect(metadata(List.of(file(state, 5)))));
        assertThrows(IOException.class, () -> inspect(metadata(List.of(file(state, CheckpointStateInspector.MAX_TOTAL_STATE_BYTES + 1)))));
        assertThrows(IOException.class, () -> inspect(metadata(java.util.Collections.nCopies(CheckpointStateInspector.MAX_FILES + 1, file(state, 4)))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "+7", "07", "9223372036854775808", "x"})
    void rejectsNoncanonicalCheckpointIds(String id) {
        assertThrows(IllegalArgumentException.class, () -> CheckpointStateInspector.arguments(new String[]{
                "--root", root.toString(), "--job-id", JOB, "--checkpoint-id", id}));
    }

    @Test
    void rejectsExtraDuplicateUnsafeAndMissingArguments() {
        assertThrows(IllegalArgumentException.class, () -> CheckpointStateInspector.arguments(new String[]{}));
        assertThrows(IllegalArgumentException.class, () -> CheckpointStateInspector.arguments(new String[]{
                "--root", root.toString(), "--root", root.toString(), "--checkpoint-id", "7"}));
        assertThrows(IllegalArgumentException.class, () -> CheckpointStateInspector.arguments(new String[]{
                "--root", "relative", "--job-id", JOB, "--checkpoint-id", "7"}));
        assertThrows(IllegalArgumentException.class, () -> CheckpointStateInspector.arguments(new String[]{
                "--root", root.toString(), "--job-id", "../../escape", "--checkpoint-id", "7"}));
    }

    @Test
    void reflectiveMetadataSelectionRejectsAbsentDuplicateUnknownAndInlineHandles() {
        assertThrows(IOException.class, () -> CheckpointStateInspector.metadataModel(new FakeMetadata(List.of())));
        FakeOperator empty = new FakeOperator(List.of());
        assertThrows(IOException.class, () -> CheckpointStateInspector.metadataModel(new FakeMetadata(List.of(empty))));
        assertThrows(IOException.class, () -> CheckpointStateInspector.metadataModel(new FakeMetadata(List.of(empty, empty))));
        assertThrows(IOException.class, () -> CheckpointStateInspector.metadataModel(new FakeMetadata(List.of(new FakeOperator(List.of(new Object()))))));
    }

    private CheckpointStateInspector.Receipt inspect(CheckpointStateInspector.Metadata metadata) throws Exception {
        return CheckpointStateInspector.inspect(root, JOB, 7, (bytes, pointer) -> metadata);
    }

    private static CheckpointStateInspector.Metadata metadata(List<CheckpointStateInspector.ManagedFile> files) {
        return new CheckpointStateInspector.Metadata(7, CheckpointStateInspector.OPERATOR_UID, files);
    }

    private static CheckpointStateInspector.ManagedFile file(Path path, long size) {
        return new CheckpointStateInspector.ManagedFile(path.toUri(), size);
    }

    private static String digest(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    public record FakeMetadata(List<FakeOperator> operators) {
        public long getCheckpointId() { return 7; }
        public List<FakeOperator> getOperatorStates() { return operators; }
    }
    public record FakeOperator(List<Object> handles) {
        public Optional<String> getOperatorUid() { return Optional.of(CheckpointStateInspector.OPERATOR_UID); }
        public int getParallelism() { return 1; }
        public Map<Integer, FakeSubtask> getSubtaskStates() { return Map.of(0, new FakeSubtask(handles)); }
    }
    public record FakeSubtask(List<Object> handles) {
        public List<Object> getManagedKeyedState() { return handles; }
    }
}
