package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.campaign.CampaignDocuments;
import org.savonitar.flink.stability.core.campaign.CampaignGenerator;
import org.savonitar.flink.stability.core.campaign.CampaignShrinker;
import org.savonitar.flink.stability.core.spec.document.SpecificationCatalogLoader;
import org.savonitar.flink.stability.core.spec.resolution.ResolutionRequest;
import org.savonitar.flink.stability.core.spec.resolution.ScenarioPlanResolver;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "campaign", mixinStandardHelpOptions = true,
        description = "Generate, replay or reduce seeded fault schedules without running them.",
        subcommands = {CampaignCommand.Generate.class, CampaignCommand.Replay.class, CampaignCommand.Shrink.class})
public final class CampaignCommand implements Runnable {
    @CommandLine.Spec private CommandLine.Model.CommandSpec spec;
    @Override public void run() { spec.commandLine().usage(spec.commandLine().getOut()); }

    static abstract class OutputCommand implements Callable<Integer> {
        @CommandLine.Spec CommandLine.Model.CommandSpec spec;
        @CommandLine.Option(names = "--workspace-root", defaultValue = ".", description = "Output must be a new directory below this workspace's jobs/.") Path workspace;
        @CommandLine.Option(names = "--output", required = true, description = "New directory relative to workspace root (jobs/...).") Path output;
        @CommandLine.Option(names = "--artifact-root", defaultValue = ".", description = "Local artifact root; all preparation is offline.") Path artifacts;
        abstract List<CampaignOutput.Entry> entries() throws Exception;
        ObjectNode reductions;
        @Override public Integer call() {
            try {
                Path destination = CampaignOutput.destination(workspace, output);
                List<CampaignOutput.Entry> entries = entries();
                CampaignOutput.write(destination, entries, artifacts, reductions);
                spec.commandLine().getOut().println("generated: " + entries.size() + " scenario(s) in " + destination + " (offline; no execution)");
                return 0;
            } catch (Exception exception) {
                spec.commandLine().getErr().println("error: " + exception.getMessage()); return CommandLine.ExitCode.SOFTWARE;
            }
        }
    }

    @CommandLine.Command(name = "generate", mixinStandardHelpOptions = true, description = "Choose a fault schedule from explicit constraints and seed.")
    static final class Generate extends OutputCommand {
        @CommandLine.Option(names = "--catalog-root", required = true) Path catalog;
        @CommandLine.Option(names = "--scenario", required = true) String scenario;
        @CommandLine.Option(names = "--constraints", required = true, description = "Strict JSON constraints; see docs/CAMPAIGNS.md.") Path constraints;
        @CommandLine.Option(names = "--seed", required = true) long seed;
        @CommandLine.Option(names = {"-p", "--parameter"}) List<String> parameters = new java.util.ArrayList<>();
        @Override List<CampaignOutput.Entry> entries() throws Exception {
            var bundle = new SpecificationCatalogLoader().load(catalog).scenario(scenario)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown base scenario: " + scenario));
            var resolved = new ScenarioPlanResolver().resolve(bundle, new ResolutionRequest(java.util.Map.of(),
                    CliParameterParser.parse(parameters, spec.commandLine())));
            var generator = new CampaignGenerator();
            ObjectNode recipe = generator.recipe(resolved, CampaignOutput.read(constraints), seed);
            return List.of(new CampaignOutput.Entry(recipe, generator.generate(recipe)));
        }
    }

    @CommandLine.Command(name = "replay", mixinStandardHelpOptions = true, description = "Regenerate byte-identical YAML from a saved seed recipe and verify its hashes.")
    static final class Replay extends OutputCommand {
        @CommandLine.Option(names = "--manifest", required = true) Path manifest;
        @Override List<CampaignOutput.Entry> entries() throws Exception { return List.of(CampaignOutput.replay(manifest)); }
    }

    @CommandLine.Command(name = "shrink", mixinStandardHelpOptions = true, description = "Emit valid removal/shortening candidates; no execution or minimality claim.")
    static final class Shrink extends OutputCommand {
        @CommandLine.Option(names = "--manifest", required = true) Path manifest;
        @Override List<CampaignOutput.Entry> entries() throws Exception {
            var source = CampaignOutput.replay(manifest);
            var result = new CampaignShrinker().shrink(source.recipe());
            reductions = CampaignDocuments.object(); var rejected = reductions.putArray("rejected");
            result.rejected().forEach(value -> rejected.addObject().put("index", value.index())
                    .put("operation", value.operation()).put("reason", value.reason()));
            return result.candidates().stream().map(value -> new CampaignOutput.Entry(value.recipe(), value.generated())).toList();
        }
    }
}
