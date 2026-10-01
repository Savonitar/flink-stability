package org.savonitar.flink.stability.core.artifact;

import org.savonitar.flink.stability.core.spec.document.Diagnostic;
import org.savonitar.flink.stability.core.spec.document.ResolutionScope;
import org.savonitar.flink.stability.core.spec.document.SpecificationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Checks each resolved side's assertion against the immutable prepared primary bytes. */
final class ConnectorPrimaryPins {
    private ConnectorPrimaryPins() {}

    static List<PreparedScenarioPlan.ConnectorPrimaryEvidence> verify(PreparedScenarioPlan plan) {
        List<PreparedScenarioPlan.ConnectorPrimaryEvidence> evidence = new ArrayList<>();
        List<Diagnostic> issues = new ArrayList<>();
        var scenario = plan.scenarioPlan().scenario();
        for (var side : scenario.sides()) {
            var connectors = side.document().at("/subject/connectors");
            connectors.fields().forEachRemaining(entry -> {
                String path = "$/subject/connectors/" + entry.getKey().replace("~", "~0").replace("/", "~1");
                var declared = entry.getValue().get("sha256");
                Optional<String> pin = declared == null ? Optional.empty() : Optional.of(declared.asText());
                var resolved = plan.artifact(side.side(), path + "/artifact");
                if (resolved.isEmpty()) {
                    if (declared != null) issues.add(new Diagnostic(scenario.template().source(),
                            ResolutionScope.valueOf(side.side().name()), "artifact.connector.pin-mismatch",
                            path + "/sha256", "Connector primary is unavailable for its declared SHA-256 pin"));
                    return;
                }
                var primary = resolved.orElseThrow();
                if (declared != null && (!declared.isTextual() || !declared.textValue().matches("[0-9a-f]{64}"))) {
                    issues.add(new Diagnostic(scenario.template().source(), ResolutionScope.valueOf(side.side().name()),
                            "artifact.connector.pin-invalid", path + "/sha256",
                            "Connector SHA-256 pin must contain exactly 64 lowercase hexadecimal characters"));
                } else if (pin.filter(value -> !value.equals(primary.sha256())).isPresent()) {
                    issues.add(new Diagnostic(scenario.template().source(), ResolutionScope.valueOf(side.side().name()),
                            "artifact.connector.pin-mismatch", path + "/sha256",
                            "Connector primary SHA-256 does not match its declared pin: expected "
                                    + pin.orElseThrow() + ", observed " + primary.sha256()));
                }
                evidence.add(new PreparedScenarioPlan.ConnectorPrimaryEvidence(side.side(), entry.getKey(),
                        primary.declaredReference(), pin, primary.sha256()));
            });
        }
        if (!issues.isEmpty()) throw new SpecificationException(SpecificationException.Stage.ARTIFACT, issues);
        return List.copyOf(evidence);
    }
}
