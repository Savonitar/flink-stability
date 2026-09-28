package org.savonitar.flink.stability.core.flink;

import java.io.IOException;
import java.time.Duration;

/** Minimal job-control boundary required by terminal write fencing. */
public interface FlinkJobControl {
    FlinkJobState jobState(FlinkJobHandle job) throws IOException;

    FlinkJobState awaitFinished(FlinkJobHandle job, Duration timeout) throws IOException;

    /** Reads the job's recovery-relevant state under one fixed internal deadline. */
    FlinkJobObservation observe(FlinkJobHandle job) throws IOException;

    /** HA recovery supplies its remaining budget rather than starting another full timeout. */
    default FlinkJobObservation observe(FlinkJobHandle job, Duration timeout) throws IOException {
        return observe(job);
    }

    /** Reads the JobManager clock after the caller's preceding action has completed. */
    default long jobManagerTimeMillis(FlinkJobHandle job) throws IOException {
        return observe(job).jobManagerTimeMillis();
    }
}
