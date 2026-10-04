package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.savonitar.flink.stability.core.campaign.CampaignDocuments;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalogLoader;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CampaignCommandTest {
    @TempDir Path root;

    @BeforeEach void resolveTemporaryRoot() throws java.io.IOException {
        // macOS may allocate @TempDir below /var -> /private/var; keep production path guards strict.
        root = root.toRealPath();
    }

    @Test void generateReplayAndShrinkProduceOfflineValidCatalogsWithoutExecution() throws Exception {
        fixture();
        assertSuccess(generate("jobs/generated"));
        Path generated = root.resolve("jobs/generated");
        Path originalYaml = scenario(generated);
        assertSuccess(execute("campaign", "replay", "--manifest", generated.resolve("campaign.json").toString(),
                "--workspace-root", root.toString(), "--output", "jobs/replayed", "--artifact-root", root.toString()));
        assertArrayEquals(Files.readAllBytes(originalYaml), Files.readAllBytes(scenario(root.resolve("jobs/replayed"))));
        assertArrayEquals(Files.readAllBytes(generated.resolve("campaign.json")), Files.readAllBytes(root.resolve("jobs/replayed/campaign.json")));
        assertSuccess(execute("campaign", "shrink", "--manifest", generated.resolve("campaign.json").toString(),
                "--workspace-root", root.toString(), "--output", "jobs/shrunk", "--artifact-root", root.toString()));
        var candidates = new SpecificationCatalogLoader().load(root.resolve("jobs/shrunk"));
        assertEquals(4, candidates.scenarios().size());
        for (String name : candidates.scenarios().keySet()) {
            assertSuccess(execute("validate", "--catalog-root", root.resolve("jobs/shrunk").toString(),
                    "--scenario", name, "--artifact-root", root.toString(), "--offline"));
        }
        String name = new SpecificationCatalogLoader().load(generated).scenarios().keySet().iterator().next();
        assertSuccess(execute("validate", "--catalog-root", generated.toString(), "--scenario", name,
                "--artifact-root", root.toString(), "--offline"));
    }

    @Test void replayRejectsTamperingAndUnknownVersionBeforeOutput() throws Exception {
        fixture(); assertSuccess(generate("jobs/generated"));
        Path manifest = root.resolve("jobs/generated/campaign.json");
        ObjectNode original = CampaignOutput.read(manifest);
        for (String field : List.of("recipe_sha256", "scenario_sha256", "expected_sha256")) {
            ObjectNode changed = original.deepCopy().put(field, "0".repeat(64));
            Path tampered = root.resolve("tampered.json"); Files.write(tampered, CampaignDocuments.json(changed));
            var result = execute("campaign", "replay", "--manifest", tampered.toString(), "--workspace-root", root.toString(),
                    "--output", "jobs/rejected", "--artifact-root", root.toString());
            assertNotEquals(0, result.code()); assertTrue(result.error().contains("SHA-256 mismatch"), result.error());
            assertFalse(Files.exists(root.resolve("jobs/rejected")));
        }
    }

    @Test void refusesCanonicalOutputExistingDirectoriesSymlinksAndMissingArtifacts() throws Exception {
        fixture();
        assertNotEquals(0, generate("scenarios/generated").code());
        assertFalse(Files.exists(root.resolve("scenarios")));
        assertSuccess(generate("jobs/generated"));
        byte[] manifest = Files.readAllBytes(root.resolve("jobs/generated/campaign.json"));
        assertNotEquals(0, generate("jobs/generated").code());
        assertArrayEquals(manifest, Files.readAllBytes(root.resolve("jobs/generated/campaign.json")));
        Path outside = Files.createDirectory(root.resolve("outside"));
        Files.createSymbolicLink(root.resolve("jobs/link"), outside);
        assertNotEquals(0, generate("jobs/link/escape").code()); assertFalse(Files.exists(outside.resolve("escape")));
        Files.delete(root.resolve("job.jar"));
        var failure = generate("jobs/missing-artifact");
        assertNotEquals(0, failure.code()); assertTrue(failure.error().contains("artifact.local.not-found"), failure.error());
        assertFalse(Files.exists(root.resolve("jobs/missing-artifact")));
    }

    @Test void nestedHelpIsOfflineAndSeedIsRequired() {
        assertSuccess(execute("campaign", "generate", "--help"));
        assertSuccess(execute("campaign", "replay", "--help"));
        assertSuccess(execute("campaign", "shrink", "--help"));
        assertNotEquals(0, execute("campaign", "generate").code());
    }

    private void fixture() throws Exception {
        Path catalog = Files.createDirectory(root.resolve("catalog"));
        ValidateSpecificationsCommandTest.createJar(root.resolve("connector.jar"), false);
        ValidateSpecificationsCommandTest.createJar(root.resolve("job.jar"), true);
        ValidateSpecificationsCommandTest.writePair(catalog, "base", "connector.jar", "job.jar", "");
        Path source = catalog.resolve("base.yaml");
        Files.writeString(source, Files.readString(source) + "\nhealth_retry_limit: 0\n");
        Files.writeString(root.resolve("constraints.json"), """
                {"phase":"verify-running","min_faults":2,"max_faults":2,"min_gap_ms":0,"max_gap_ms":10,
                 "faults":[[
                   {"kill":{"target":{"kind":"named","role":"taskmanager","name":"taskmanager-1"}}},
                   {"wait":{"duration":"1s"}},
                   {"restart":{"component":"taskmanager","name":"taskmanager-1"}}
                 ]]}
                """);
    }

    private Invocation generate(String output) {
        return execute("campaign", "generate", "--catalog-root", root.resolve("catalog").toString(), "--scenario", "base",
                "--constraints", root.resolve("constraints.json").toString(), "--seed", "42", "--workspace-root", root.toString(),
                "--output", output, "--artifact-root", root.toString());
    }
    private static Path scenario(Path root) throws Exception {
        try (var files = Files.list(root)) {
            return files.filter(path -> path.toString().endsWith(".yaml") && !path.toString().endsWith(".expected.yaml")).findFirst().orElseThrow();
        }
    }
    private static void assertSuccess(Invocation result) { assertEquals(0, result.code(), result.error()); }
    private static Invocation execute(String... args) {
        StringWriter out = new StringWriter(), err = new StringWriter();
        int code = new CommandLine(new FlinkStabilityCommand()).setOut(new PrintWriter(out, true)).setErr(new PrintWriter(err, true)).execute(args);
        return new Invocation(code, out.toString(), err.toString());
    }
    private record Invocation(int code, String output, String error) {}
}
