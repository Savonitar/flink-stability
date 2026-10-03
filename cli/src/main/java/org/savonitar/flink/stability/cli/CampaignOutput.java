package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.artifact.ArtifactPlanResolver;
import org.savonitar.flink.stability.core.artifact.ArtifactResolutionOptions;
import org.savonitar.flink.stability.core.campaign.CampaignDocuments;
import org.savonitar.flink.stability.core.campaign.CampaignGenerator;
import org.savonitar.flink.stability.runtime.api.Digests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/** Offline artifact preflight followed by publication to a new local jobs directory. */
final class CampaignOutput {
    record Entry(ObjectNode recipe, CampaignGenerator.Generated generated) {}
    private CampaignOutput() {}

    static ObjectNode read(Path input) throws IOException {
        if (Files.size(input) > 4 * 1024 * 1024) throw new IllegalArgumentException("Campaign input exceeds 4 MiB");
        return CampaignDocuments.read(Files.readAllBytes(input));
    }

    static Entry replay(Path input) throws IOException {
        ObjectNode manifest = read(input);
        CampaignDocuments.fields(manifest, Set.of("recipe", "recipe_sha256", "scenario_sha256", "expected_sha256"), Set.of());
        if (!(manifest.get("recipe") instanceof ObjectNode recipe)
                || !CampaignDocuments.digest(recipe).equals(manifest.path("recipe_sha256").asText()))
            throw new IllegalArgumentException("Campaign recipe SHA-256 mismatch");
        var generated = new CampaignGenerator().generate(recipe);
        if (!Digests.sha256(generated.scenarioYaml()).equals(manifest.path("scenario_sha256").asText())
                || !Digests.sha256(generated.expectedYaml()).equals(manifest.path("expected_sha256").asText()))
            throw new IllegalArgumentException("Campaign replay YAML SHA-256 mismatch");
        return new Entry(recipe, generated);
    }

    static Path destination(Path workspace, Path requested) throws IOException {
        Path root = workspace.toAbsolutePath().normalize();
        Path jobs = root.resolve("jobs");
        Path output = requested.isAbsolute() ? requested.normalize() : root.resolve(requested).normalize();
        if (!output.startsWith(jobs) || output.equals(jobs)) throw new IllegalArgumentException("Campaign output must be a new directory below workspace jobs/");
        for (Path parent = output; parent != null; parent = parent.getParent())
            if (Files.isSymbolicLink(parent)) throw new IllegalArgumentException("Campaign output cannot traverse symlinks");
        if (Files.exists(output, LinkOption.NOFOLLOW_LINKS)) throw new IllegalArgumentException("Campaign output already exists: " + output);
        return output;
    }

    static void write(Path destination, List<Entry> entries, Path artifacts, ObjectNode reductions) throws IOException {
        var resolver = new ArtifactPlanResolver();
        for (Entry entry : entries) try (var ignored = resolver.resolve(entry.generated().resolved(), new ArtifactResolutionOptions(artifacts, true))) {
            // Exactly the artifact preparation used by validate --offline. All candidates pass before any are published.
        }
        Files.createDirectories(destination.getParent());
        Path staging = Files.createTempDirectory(destination.getParent(), ".campaign-");
        try {
            for (Entry entry : entries) {
                var generated = entry.generated();
                Path catalog = reductions == null ? staging : Files.createDirectory(staging.resolve(generated.name()));
                Files.write(catalog.resolve(generated.name() + ".yaml"), generated.scenarioYaml());
                Files.write(catalog.resolve(generated.name() + ".expected.yaml"), generated.expectedYaml());
                Files.write(catalog.resolve("campaign.json"), CampaignDocuments.json(generated.manifest(entry.recipe())));
            }
            if (reductions != null) Files.write(staging.resolve("reductions.json"), CampaignDocuments.json(reductions));
            Files.move(staging, destination);
        } finally {
            if (Files.exists(staging)) try (var paths = Files.walk(staging)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
    }
}
