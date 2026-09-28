package org.savonitar.flink.stability.runtime.api;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static org.savonitar.flink.stability.runtime.api.Checks.requireNonBlank;

/** Component operations required by the first typed phase executor. */
public interface TaskManagerControl {
    void killTaskManager(String targetName, Duration timeout) throws IOException;

    void restartTaskManager(Duration timeout) throws IOException;

    default void restartTaskManager(String targetName, Duration timeout) throws IOException {
        if (!"taskmanager-1".equals(targetName)) {
            throw new UnsupportedOperationException("Named TaskManager restart is unsupported: "
                    + targetName);
        }
        restartTaskManager(timeout);
    }

    /** Identity of the current running handle; missing evidence must not confirm a fault. */
    default Optional<Identity> taskManagerIdentity(String targetName) {
        return Optional.empty();
    }

    /** Binds one container incarnation to the ResourceID configured for its Flink process. */
    record Identity(String logicalName, String runtimeId, String resourceId) {
        public Identity {
            logicalName = requireNonBlank(logicalName, "logicalName");
            runtimeId = requireNonBlank(runtimeId, "runtimeId");
            resourceId = requireNonBlank(resourceId, "resourceId");
        }
    }
}
