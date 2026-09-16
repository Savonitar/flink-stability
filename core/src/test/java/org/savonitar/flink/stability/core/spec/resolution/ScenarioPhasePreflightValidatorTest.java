package org.savonitar.flink.stability.core.spec.resolution;

import org.savonitar.flink.stability.core.spec.document.ScenarioSpecification;
import org.savonitar.flink.stability.core.spec.document.SpecificationLoader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScenarioPhasePreflightValidatorTest {
    private final SpecificationLoader loader = new SpecificationLoader();
    private final ScenarioParameterResolver parameterResolver =
            new ScenarioParameterResolver(loader);
    private final ScenarioPreflightValidator validator = new ScenarioPreflightValidator();

    @Test
    void rejectsDuplicatePhaseNames() {
        ScenarioPreflightException exception = reject(document ->
                phases(document).add(phase(document).deepCopy()));

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.phase.duplicate-name",
                "$/phases/1/name");
    }

    @Test
    void rejectsAllMissingJobReferencesIncludingOnNestedSteps() {
        ScenarioPreflightException exception = reject(document -> {
            ArrayNode outer = replaceSteps(document);
            ObjectNode loop = outer.addObject().putObject("loop");
            loop.put("times", 1);
            ArrayNode nested = loop.putArray("steps");

            nested.addObject().putObject("start").put("job", "missing-start");
            nested.addObject().putObject("checkpoint").put("job", "missing-checkpoint");
            nested.addObject().putObject("savepoint").put("job", "missing-savepoint");
            ObjectNode restore = nested.addObject().putObject("restore");
            restore.put("job", "missing-restore");
            restore.put("from", "latest-checkpoint");
            restore.put("mode", "claim");

            addJobAwait(nested, "job-state", "missing-state");
            addJobAwait(nested, "checkpoint-completed", "missing-checkpoint-await");
            addJobAwait(nested, "savepoint-completed", "missing-savepoint-await");
            ObjectNode log = addAwait(nested, "log-marker");
            log.put("job", "missing-log");
            log.put("marker", "marker");
        });

        String nested = "$/phases/0/steps/0/loop/steps/";
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "0/start/job");
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "1/checkpoint/job");
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "2/savepoint/job");
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "3/restore/job");
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "4/await/condition/job");
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "5/await/condition/job");
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "6/await/condition/job");
        assertHasIssue(exception, ResolutionScope.COMMON, "preflight.reference.job-not-found",
                nested + "7/await/condition/job");
        assertEquals(8, exception.issues().stream()
                .filter(issue -> issue.code().equals("preflight.reference.job-not-found"))
                .count());
    }

    @Test
    void acceptsGlobalLogMarkerWithoutAJobReference() {
        assertAccepts(document -> {
            ObjectNode condition = addAwait(replaceSteps(document), "log-marker");
            condition.put("marker", "checkpoint-precommit");
        });
    }

    @Test
    void validatesRecordThresholdReferences() {
        assertAccepts(document -> {
            ObjectNode condition = addAwait(replaceSteps(document), "record-threshold");
            condition.put("cluster", "main");
            condition.put("topic", "output");
            condition.put("count", 1);
        });

        ScenarioPreflightException exception = reject(document -> {
            ObjectNode condition = addAwait(replaceSteps(document), "record-threshold");
            condition.put("cluster", "main");
            condition.put("topic", "missing");
            condition.put("count", 1);
        });
        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-topic-not-found",
                "$/phases/0/steps/0/await/condition/topic");
    }

    @Test
    void validatesKafkaTransactionAwaitClusterAndPrefix() {
        assertAccepts(document -> addTransactionAwait(replaceSteps(document), "main", "minimal"));

        ScenarioPreflightException missingPrefix = reject(document ->
                addTransactionAwait(replaceSteps(document), "main", "other-prefix"));
        assertHasIssue(
                missingPrefix,
                ResolutionScope.SINGLE,
                "preflight.await.transactional-sink-not-found",
                "$/phases/0/steps/0/await/condition/transactional_id_prefix");

        ScenarioPreflightException missingCluster = reject(document ->
                addTransactionAwait(replaceSteps(document), "missing", "other-prefix"));
        assertHasIssue(
                missingCluster,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-cluster-not-found",
                "$/phases/0/steps/0/await/condition/cluster");
        assertFalse(missingCluster.issues().stream().anyMatch(issue ->
                issue.code().equals("preflight.await.transactional-sink-not-found")));
    }

    @Test
    void rejectsOnlyTaskmanagerCountsAboveTheResolvedTopology() {
        assertAccepts(document -> addTaskmanagerCountAwait(replaceSteps(document), 1));

        ScenarioPreflightException exception = reject(document ->
                addTaskmanagerCountAwait(replaceSteps(document), 2));
        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.await.taskmanager-count-exceeds-configured",
                "$/phases/0/steps/0/await/condition/value");
    }

    @Test
    void rejectsRuntimeSelectorsWithoutCascadingIntoSelectorDetails() {
        ScenarioPreflightException exception = reject(document -> {
            ObjectNode target = replaceSteps(document)
                    .addObject().putObject("kill").putObject("target");
            target.put("kind", "selector");
            target.put("role", "taskmanager");
            ObjectNode hosting = target.putObject("hosting");
            hosting.put("job", "missing-job");
            hosting.put("vertex", "sink");
            hosting.put("subtask", 0);
        });

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "capability.runtime-target-selector.unsupported",
                "$/phases/0/steps/0/kill/target/kind");
        assertEquals(1, exception.issues().stream()
                .filter(issue -> issue.path().startsWith("$/phases/0/steps/0/kill/target"))
                .count());
    }

    @Test
    void resolvesStaticTargetsAgainstRoleAndDeclaredCount() {
        assertAccepts(document -> {
            document.withObject("setup").withObject("flink").put("taskmanagers", 2);
            addNamedKill(replaceSteps(document), "taskmanager", "taskmanager-2");
        });

        ScenarioPreflightException wrongRole = reject(document ->
                addNamedKill(replaceSteps(document), "taskmanager", "broker-1"));
        assertHasIssue(
                wrongRole,
                ResolutionScope.SINGLE,
                "preflight.target.named-not-found",
                "$/phases/0/steps/0/kill/target/name");

        ScenarioPreflightException aboveCount = reject(document -> {
            document.withObject("setup").withObject("flink").put("taskmanagers", 2);
            addNamedKill(replaceSteps(document), "taskmanager", "taskmanager-3");
        });
        assertHasIssue(
                aboveCount,
                ResolutionScope.SINGLE,
                "preflight.target.named-not-found",
                "$/phases/0/steps/0/kill/target/name");
    }

    @Test
    void acceptsUniquelyResolvedBrokerTargetAndRejectsAmbiguity() {
        assertAccepts(document -> addNamedKill(replaceSteps(document), "broker", "broker-1"));
        assertAccepts(document -> {
            ((ObjectNode) document.at("/setup/kafka/clusters/main")).put("brokers", 2);
            addKafkaCluster(document, "other");
            ((ObjectNode) document.at("/setup/kafka/clusters/other")).put("brokers", 1);
            addNamedKill(replaceSteps(document), "broker", "broker-2");
        });

        ScenarioPreflightException exception = reject(document -> {
            addKafkaCluster(document, "other");
            addNamedKill(replaceSteps(document), "broker", "broker-1");
        });
        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.target.broker-cluster-ambiguous",
                "$/phases/0/steps/0/kill/target/name");
    }

    @Test
    void acceptsSingleClusterKafkaRestartAndRejectsTwoClusterAmbiguity() {
        assertAccepts(document -> addRestart(replaceSteps(document), "kafka"));

        ScenarioPreflightException exception = reject(document -> {
            addKafkaCluster(document, "other");
            addRestart(replaceSteps(document), "kafka");
        });
        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.restart.kafka-cluster-ambiguous",
                "$/phases/0/steps/0/restart/component");
    }

    @Test
    void acceptsRestoreFromAPriorNamedArtifact() {
        assertAccepts(document -> {
            ArrayNode steps = replaceSteps(document);
            addCheckpoint(steps, "eos-job", "before-failure");
            addNamedRestore(steps, "eos-job", "checkpoint", "before-failure");
        });
    }

    @Test
    void distinguishesForwardUnknownTypeAndJobStateArtifactReferences() {
        ScenarioPreflightException forward = reject(document -> {
            ArrayNode steps = replaceSteps(document);
            addNamedRestore(steps, "eos-job", "checkpoint", "later");
            addCheckpoint(steps, "eos-job", "later");
        });
        assertHasIssue(
                forward,
                ResolutionScope.COMMON,
                "preflight.state-artifact.not-prior",
                "$/phases/0/steps/0/restore/from/name");

        ScenarioPreflightException unknown = reject(document ->
                addNamedRestore(replaceSteps(document), "eos-job", "checkpoint", "unknown"));
        assertHasIssue(
                unknown,
                ResolutionScope.COMMON,
                "preflight.state-artifact.not-found",
                "$/phases/0/steps/0/restore/from/name");

        ScenarioPreflightException wrongType = reject(document -> {
            ArrayNode steps = replaceSteps(document);
            addCheckpoint(steps, "eos-job", "before-failure");
            addNamedRestore(steps, "eos-job", "savepoint", "before-failure");
        });
        assertHasIssue(
                wrongType,
                ResolutionScope.COMMON,
                "preflight.state-artifact.type-mismatch",
                "$/phases/0/steps/1/restore/from/name");

        ScenarioPreflightException wrongJob = reject(document -> {
            addSecondJob(document, "second-job");
            ArrayNode steps = replaceSteps(document);
            addCheckpoint(steps, "eos-job", "before-failure");
            addNamedRestore(steps, "second-job", "checkpoint", "before-failure");
        });
        assertHasIssue(
                wrongJob,
                ResolutionScope.COMMON,
                "preflight.state-artifact.job-mismatch",
                "$/phases/0/steps/1/restore/from/name");
    }

    @Test
    void rejectsDuplicateNamedStateArtifacts() {
        ScenarioPreflightException exception = reject(document -> {
            ArrayNode steps = replaceSteps(document);
            addCheckpoint(steps, "eos-job", "same-name");
            addCheckpoint(steps, "eos-job", "same-name");
        });

        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.state-artifact.duplicate-identity",
                "$/phases/0/steps/1/checkpoint/as");
    }

    @Test
    void rejectsNamedArtifactsInRepeatedLoopsButAcceptsOneIteration() {
        ScenarioPreflightException repeated = reject(document -> {
            ObjectNode loop = replaceSteps(document).addObject().putObject("loop");
            loop.put("times", 2);
            addCheckpoint(loop.putArray("steps"), "eos-job", "inside-loop");
        });
        assertHasIssue(
                repeated,
                ResolutionScope.SINGLE,
                "preflight.state-artifact.repeated-name",
                "$/phases/0/steps/0/loop/steps/0/checkpoint/as");

        assertAccepts(document -> {
            ArrayNode steps = replaceSteps(document);
            ObjectNode loop = steps.addObject().putObject("loop");
            loop.put("times", 1);
            addCheckpoint(loop.putArray("steps"), "eos-job", "inside-loop");
            addNamedRestore(steps, "eos-job", "checkpoint", "inside-loop");
        });
    }

    @Test
    void acceptsLatestAndExternalStateReferencesWithoutLocalDeclarations() {
        assertAccepts(document -> {
            ArrayNode steps = replaceSteps(document);
            addLatestRestore(steps, "eos-job", "latest-checkpoint");
            ObjectNode restore = addRestore(steps, "eos-job");
            ObjectNode from = restore.putObject("from");
            from.put("type", "savepoint");
            from.put("path", "s3://state/savepoint-1");
        });
    }

    @Test
    void validatesInlineValidatorReferencesWithoutMakingTypesUnique() {
        assertAccepts(document -> {
            ArrayNode steps = replaceSteps(document);
            addInlineRecordCount(steps, "main", "output");
            addInlineRecordCount(steps, "main", "output");
        });

        ScenarioPreflightException exception = reject(document -> {
            ArrayNode steps = replaceSteps(document);
            addInlineRecordCount(steps, "main", "missing");
            ObjectNode log = steps.addObject().putObject("validate");
            log.put("type", "flink.log-match");
            log.put("job", "missing-job");
            log.put("pattern", "ERROR");
        });
        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.reference.kafka-topic-not-found",
                "$/phases/0/steps/0/validate/topic");
        assertHasIssue(
                exception,
                ResolutionScope.COMMON,
                "preflight.reference.job-not-found",
                "$/phases/0/steps/1/validate/job");
        assertFalse(exception.issues().stream()
                .anyMatch(issue -> issue.code().equals("preflight.terminal.duplicate-type")));
    }

    @Test
    void rejectsInlineIdSetWhenCustomInputCannotProvideAManifest() {
        ScenarioPreflightException exception = reject(document -> {
            ObjectNode input = (ObjectNode) document.at(
                    "/setup/kafka/clusters/main/topics/0/input_source");
            input.removeAll();
            input.put("mode", "custom");
            input.put("artifact", "./custom-input.jar");
            input.put("timeout", "30s");

            ObjectNode custom = document.putArray("terminal_validations").addObject();
            custom.put("type", "custom");
            custom.put("artifact", "./custom-validator.jar");
            custom.put("timeout", "30s");

            ObjectNode idSet = replaceSteps(document).addObject().putObject("validate");
            idSet.put("type", "kafka.id-set");
            idSet.put("cluster", "main");
            idSet.put("topic", "output");
            idSet.put("expected", "input-manifest");
        });

        assertHasIssue(
                exception,
                ResolutionScope.SINGLE,
                "preflight.input.manifest-not-available",
                "$/phases/0/steps/0/validate/expected");
    }

    private ScenarioPreflightException reject(Consumer<ObjectNode> changes) {
        ResolvedScenario scenario = resolve(changes);
        return assertThrows(
                ScenarioPreflightException.class,
                () -> validator.validateScenario(scenario));
    }

    private void assertAccepts(Consumer<ObjectNode> changes) {
        ResolvedScenario scenario = resolve(changes);
        assertDoesNotThrow(() -> validator.validateScenario(scenario));
    }

    private ResolvedScenario resolve(Consumer<ObjectNode> changes) {
        Path source = resource("minimal.yaml");
        ObjectNode document = loader.loadScenario(source).document();
        changes.accept(document);
        ScenarioSpecification specification = loader.validateScenarioDocument(source, document);
        return parameterResolver.resolve(specification, ResolutionRequest.none());
    }

    private static ObjectNode addAwait(ArrayNode steps, String type) {
        ObjectNode await = steps.addObject().putObject("await");
        ObjectNode condition = await.putObject("condition");
        condition.put("type", type);
        await.put("timeout", "1m");
        await.put("on_timeout", "inconclusive");
        return condition;
    }

    private static void addJobAwait(ArrayNode steps, String type, String job) {
        ObjectNode condition = addAwait(steps, type);
        condition.put("job", job);
        switch (type) {
            case "job-state" -> condition.put("state", "RUNNING");
            case "checkpoint-completed" -> condition.put("count", 1);
            default -> {
                // savepoint-completed needs only the job reference.
            }
        }
    }

    private static void addTransactionAwait(ArrayNode steps, String cluster, String prefix) {
        ObjectNode condition = addAwait(steps, "kafka-transaction-state");
        condition.put("cluster", cluster);
        condition.put("transactional_id_prefix", prefix);
        condition.put("state", "ONGOING");
    }

    private static void addTaskmanagerCountAwait(ArrayNode steps, int count) {
        ObjectNode condition = addAwait(steps, "taskmanager-count");
        condition.put("value", count);
    }

    private static void addNamedKill(ArrayNode steps, String role, String name) {
        ObjectNode target = steps.addObject().putObject("kill").putObject("target");
        target.put("kind", "named");
        target.put("role", role);
        target.put("name", name);
    }

    private static void addRestart(ArrayNode steps, String component) {
        steps.addObject().putObject("restart").put("component", component);
    }

    private static void addCheckpoint(ArrayNode steps, String job, String name) {
        ObjectNode checkpoint = steps.addObject().putObject("checkpoint");
        checkpoint.put("job", job);
        checkpoint.put("as", name);
    }

    private static void addNamedRestore(
            ArrayNode steps, String job, String type, String name) {
        ObjectNode restore = addRestore(steps, job);
        ObjectNode from = restore.putObject("from");
        from.put("type", type);
        from.put("name", name);
    }

    private static void addLatestRestore(ArrayNode steps, String job, String from) {
        addRestore(steps, job).put("from", from);
    }

    private static ObjectNode addRestore(ArrayNode steps, String job) {
        ObjectNode restore = steps.addObject().putObject("restore");
        restore.put("job", job);
        restore.put("mode", "claim");
        return restore;
    }

    private static void addInlineRecordCount(ArrayNode steps, String cluster, String topic) {
        ObjectNode validator = steps.addObject().putObject("validate");
        validator.put("type", "kafka.record-count");
        validator.put("cluster", cluster);
        validator.put("topic", topic);
        validator.put("expected_records", 1);
    }

    private static void addKafkaCluster(ObjectNode document, String name) {
        ObjectNode cluster = ((ObjectNode) document.at("/setup/kafka/clusters/main")).deepCopy();
        ((ArrayNode) cluster.get("topics"))
                .forEach(topic -> ((ObjectNode) topic).remove("input_source"));
        ((ObjectNode) document.at("/setup/kafka/clusters")).set(name, cluster);
    }

    private static void addSecondJob(ObjectNode document, String alias) {
        ((ArrayNode) document.at("/setup/kafka/clusters/main/topics"))
                .addObject()
                .put("name", "second-output")
                .put("partitions", 1)
                .put("replication_factor", 1);
        ObjectNode second = ((ObjectNode) document.at("/workload/jobs/0")).deepCopy();
        second.put("alias", alias);
        second.withObject("sink").put("topic", "second-output");
        second.withObject("sink").put("transactional_id_prefix", "second-prefix");
        ((ArrayNode) document.at("/workload/jobs")).add(second);
    }

    private static ArrayNode replaceSteps(ObjectNode document) {
        return phase(document).putArray("steps");
    }

    private static ArrayNode phases(ObjectNode document) {
        return (ArrayNode) document.get("phases");
    }

    private static ObjectNode phase(ObjectNode document) {
        return (ObjectNode) document.at("/phases/0");
    }

    private static PreflightIssue assertHasIssue(
            ScenarioPreflightException exception,
            ResolutionScope scope,
            String code,
            String path) {
        return exception.issues().stream()
                .filter(issue -> issue.scope() == scope
                        && issue.code().equals(code)
                        && issue.path().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Expected " + scope + ":" + code + " at " + path
                                + " but got " + exception.issues()));
    }

    private Path resource(String name) {
        try {
            return Path.of(getClass().getResource("/spec/valid/" + name).toURI());
        } catch (URISyntaxException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
