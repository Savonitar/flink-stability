package org.savonitar.flink.stability.core.execution;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.core.execution.plan.ExecutableScenarioPlan;
import org.savonitar.flink.stability.core.flink.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SavepointLifecycleTest {
    static final String DIRECTORY = "file:/flink/checkpoints/savepoints-attempt-1-12345678";
    static final FlinkJobHandle OLD = new FlinkJobHandle("a".repeat(32));
    static final String NEW = "b".repeat(32);
    static final FlinkJobSubmission ORIGINAL = new FlinkJobSubmission("uploaded.jar", 4,
            Map.of("source.stopping-offsets","0:30000", "source.group-id","same-group", "sink.transactional-id-prefix","same-prefix"), List.of("--processingDelayMs","5"));

    @Test void restoresSameJobConfigurationWithOnlyRequestedChangesAndKeepsBothIds() {
        for (int parallelism : List.of(1,2,4)) for (var strategy : ExecutableScenarioPlan.TransactionIdNamingStrategy.values()) {
            var fake = new Fake();
            var result = execute(fake,parallelism,strategy);
            assertTrue(result.confirmed(),result.toString()); assertEquals(1,fake.stops);assertEquals(1,fake.submits);
            assertEquals(OLD.jobId(),result.oldJobId());assertEquals(NEW,result.restoredJobId());
            assertEquals(ORIGINAL.uploadedJarId(),fake.submission.uploadedJarId());
            assertEquals(ORIGINAL.programArguments(),fake.submission.programArguments());
            ORIGINAL.flinkConfiguration().forEach((key,value)->assertEquals(value,fake.submission.flinkConfiguration().get(key)));
            assertEquals(strategy.name(),fake.submission.flinkConfiguration().get(ExecutableScenarioPlan.WorkloadConfiguration.PREFIX+"sink.transaction-id-naming-strategy"));
            assertEquals(Optional.of(DIRECTORY+"/savepoint-123"),fake.submission.savepointPath());
        }
    }
    @Test void unknownStopFailedSavepointForeignPathAndUnconfirmedOldJobNeverSubmit() {
        for (String failure : List.of("unknown-stop","savepoint-failed","foreign-path","old-job","before")) {
            var fake=new Fake();fake.failure=failure;var result=execute(fake,2,ExecutableScenarioPlan.TransactionIdNamingStrategy.INCREMENTING);
            assertFalse(result.confirmed(),failure);assertNotNull(result.error());assertEquals(0,fake.submits);
            assertTrue(fake.stops<=1);
        }
    }
    @Test void unknownRestoreAndMissingProofRetainEvidenceWithoutResubmitting() {
        for (String failure : List.of("unknown-submit","same-id","wrong-path","wrong-parallelism","not-savepoint","finished","deadline")) {
            var fake=new Fake();fake.failure=failure;var result=execute(fake,2,ExecutableScenarioPlan.TransactionIdNamingStrategy.POOLING);
            assertFalse(result.confirmed(),failure);assertEquals(1,fake.submits);assertEquals(1,fake.stops);
            assertNotNull(result.savepoint());assertEquals(FlinkJobState.FINISHED,result.stoppedState());
            if (!failure.equals("unknown-submit")) assertNotNull(result.restoredJobId());
        }
    }
    @Test void locationValidationRejectsEncodedTraversalForeignMountsAndAuthorities() {
        assertTrue(SavepointLifecycle.ownedLocation(DIRECTORY,DIRECTORY+"/savepoint-a"));
        for(String path:List.of(DIRECTORY,DIRECTORY+"-foreign/x",DIRECTORY+"/../x",DIRECTORY+"/%2e%2e",DIRECTORY+"/%2e%2e/x",
                "file://host/flink/checkpoints/x","s3://bucket/x","file:/elsewhere/x",DIRECTORY+"/x?query",DIRECTORY+"/x#fragment"))
            assertFalse(SavepointLifecycle.ownedLocation(DIRECTORY,path),path);
    }
    static SavepointLifecycle.Evidence execute(Fake fake,int parallelism,ExecutableScenarioPlan.TransactionIdNamingStrategy strategy) {
        return SavepointLifecycle.execute("step",fake,OLD,ORIGINAL,DIRECTORY,new ExecutableScenarioPlan.SavepointRestore(parallelism,strategy,Duration.ofSeconds(2)),
                duration->fake.time+=duration.toNanos(),()->fake.time);
    }
    static class Fake implements FlinkScenarioControl {
        String failure="";int stops,submits;long time;FlinkJobSubmission submission;
        public String uploadJar(Path path,String hash){throw new AssertionError();}
        public FlinkJobHandle submit(FlinkJobSubmission value){throw new AssertionError("Use bounded restore submit");}
        public FlinkJobHandle submit(FlinkJobSubmission value,Duration timeout)throws IOException{
            submits++;submission=value;if(failure.equals("unknown-submit"))throw new IOException("unknown submit outcome");
            return failure.equals("same-id")?OLD:new FlinkJobHandle(NEW);
        }
        public String stopWithSavepoint(FlinkJobHandle job,String trigger,String directory,Duration timeout)throws IOException{
            assertEquals(OLD,job);assertEquals(DIRECTORY,directory);stops++;
            if(failure.equals("unknown-stop"))throw new IOException("unknown stop outcome");return trigger;
        }
        public FlinkSavepoint.Status savepointStatus(FlinkJobHandle job,String trigger,Duration timeout){
            return new FlinkSavepoint.Status(true,failure.equals("foreign-path")?"file:/elsewhere/x":DIRECTORY+"/savepoint-123",
                    failure.equals("savepoint-failed")?"failure":null,"raw savepoint response");
        }
        public FlinkSavepoint.RestoreProof restoredSavepoint(FlinkJobHandle job,Duration timeout){
            return new FlinkSavepoint.RestoreProof(failure.equals("deadline")?null:failure.equals("wrong-path")?"file:/different":DIRECTORY+"/savepoint-123",
                    !failure.equals("not-savepoint"),failure.equals("wrong-parallelism")?99:submission.parallelism(),
                    failure.equals("finished")?FlinkJobState.FINISHED:FlinkJobState.RUNNING,"raw restore response");
        }
        public FlinkJobState awaitState(FlinkJobHandle job,FlinkJobState expected,Duration timeout){return failure.equals("old-job")?FlinkJobState.CANCELED:expected;}
        public long awaitCompletedCheckpoints(FlinkJobHandle job,long count,Duration timeout){return count;}
        public FlinkJobState jobState(FlinkJobHandle job){return FlinkJobState.RUNNING;}
        public FlinkJobState awaitFinished(FlinkJobHandle job,Duration timeout){throw new AssertionError();}
        public FlinkJobObservation observe(FlinkJobHandle job){return new FlinkJobObservation(1,FlinkJobState.RUNNING,failure.equals("before")?0:1,0,Optional.empty(),List.of(),List.of());}
        public void close(){}
    }
}
