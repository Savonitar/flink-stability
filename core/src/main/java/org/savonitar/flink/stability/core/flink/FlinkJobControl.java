package org.savonitar.flink.stability.core.flink;

import java.io.IOException;
import java.time.Duration;

/** Minimal job-control boundary required by terminal write fencing. */
public interface FlinkJobControl {
    FlinkJobState jobState(FlinkJobHandle job) throws IOException;

    FlinkJobState awaitFinished(FlinkJobHandle job, Duration timeout) throws IOException;

    /** Reads the job's recovery-relevant state under one fixed internal deadline. */
    FlinkJobObservation observe(FlinkJobHandle job) throws IOException;
}
