package org.savonitar.flink.stability.runtime.api;

import java.nio.file.Path;

/** Creates one isolated physical runtime for a prepared v1 execution attempt. */
@FunctionalInterface
public interface V1AttemptRuntimeFactory {
    V1AttemptRuntime create(Path checkpointStorageRoot);
}
