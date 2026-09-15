package org.savonitar.flink.stability.runtime.api;

import java.io.IOException;
import java.time.Duration;

/** Component operations required by the first typed phase executor. */
public interface TaskManagerControl {
    void killTaskManager(String targetName, Duration timeout) throws IOException;

    void restartTaskManager(Duration timeout) throws IOException;
}
