package org.savonitar.flink.stability.runtime.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Observed created-container identities and exact launch inputs for an opt-in custom broker. */
public record KafkaRuntimeEvidence(String clusterAlias, String imageReference, Optional<String> imageId,
                                   String launchType, Map<String, String> brokerConfig,
                                   List<Container> containers) {
    public KafkaRuntimeEvidence {
        brokerConfig = Map.copyOf(brokerConfig);
        containers = List.copyOf(containers);
    }

    /** A declaration or a created-but-unstarted container is not confirmation of a running subject. */
    public boolean confirms(KafkaRuntimeTarget target) {
        if (!clusterAlias.equals(target.clusterAlias()) || !imageReference.equals(target.imageReference())
                || !imageId.equals(target.imageId()) || !launchType.equals(target.launchType())
                || !brokerConfig.equals(target.brokerConfig()) || containers.size() != target.brokers()) return false;
        var aliases = java.util.stream.IntStream.rangeClosed(1, target.brokers()).mapToObj(target::brokerAlias)
                .collect(java.util.stream.Collectors.toSet());
        var ids = new java.util.HashSet<String>();
        var images = new java.util.HashSet<String>();
        for (var container : containers) {
            if (!aliases.remove(container.brokerAlias()) || !container.imageVerified() || !container.ready()
                    || container.containerId() == null || !container.containerId().matches("[0-9a-f]{64}")
                    || !ids.add(container.containerId())
                    || container.imageId() == null || !container.imageId().matches("sha256:[0-9a-f]{64}")
                    || imageId.filter(expected -> !expected.equals(container.imageId())).isPresent()) return false;
            images.add(container.imageId());
        }
        return aliases.isEmpty() && images.size() == 1;
    }

    public record Container(String brokerAlias, String containerId, String imageId,
                            Map<String, String> environment, List<String> command, String logDirectory,
                            boolean imageVerified, boolean ready, String startupScript) {
        public Container(String brokerAlias, String containerId, String imageId, Map<String, String> environment,
                         List<String> command, String logDirectory) {
            this(brokerAlias, containerId, imageId, environment, command, logDirectory, true, true, null);
        }
        public Container(String brokerAlias, String containerId, String imageId, Map<String, String> environment,
                         List<String> command, String logDirectory, boolean imageVerified, boolean ready) {
            this(brokerAlias, containerId, imageId, environment, command, logDirectory, imageVerified, ready, null);
        }
        public Container {
            environment = Map.copyOf(environment);
            command = List.copyOf(command);
            if (ready && !imageVerified) throw new IllegalArgumentException("Ready Kafka receipt requires verified image identity");
        }
        public Container withStartupScript(String script) {
            return new Container(brokerAlias, containerId, imageId, environment, command, logDirectory, imageVerified, ready, script);
        }
        public Container started() {
            return new Container(brokerAlias, containerId, imageId, environment, command, logDirectory, imageVerified, true, startupScript);
        }
    }
}
