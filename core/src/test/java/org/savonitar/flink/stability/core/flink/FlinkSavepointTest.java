package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class FlinkSavepointTest {
    static final String JOB="a".repeat(32),TRIGGER="b".repeat(32),LOCATION="file:/flink/checkpoints/attempt/savepoint-x";
    final ObjectMapper json=new ObjectMapper();
    @Test void rejectsMalformedOrOverflowingVertexParallelism() {
        for (String value : List.of("4294967300", "4.5", "\"4\"", "0", "null")) {
            assertThrows(IOException.class, () -> FlinkSavepoint.restored(new FlinkJobHandle(JOB), Duration.ofSeconds(1),
                    (method, path, body, deadline) -> json.readTree(path.endsWith("/checkpoints") ? "{}" :
                            "{\"state\":\"RUNNING\",\"vertices\":[{\"parallelism\":" + value + "}]}"), System::nanoTime), value);
        }
    }
    @Test void sendsOneNonDrainingStopPollsExactTriggerAndRestoresWithoutDiscardingState() throws Exception {
        var calls=new ArrayList<String>();
        try(var client=new FlinkRestApiClient((method,path,body,timeout)->{
            calls.add(method+" "+path);
            String answer;
            if(path.endsWith("/stop")) {
                var request=json.readTree(body);assertFalse(request.path("drain").asBoolean());
                assertEquals("CANONICAL",request.path("formatType").asText());assertEquals(TRIGGER,request.path("triggerId").asText());
                answer="{\"request-id\":\""+TRIGGER+"\"}";
            } else if(path.endsWith("/savepoints/"+TRIGGER)) answer="{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"location\":\""+LOCATION+"\"}}";
            else if(path.endsWith("/run")) {
                var request=json.readTree(body);assertEquals(LOCATION,request.path("savepointPath").asText());
                assertFalse(request.path("allowNonRestoredState").asBoolean(true));assertEquals("NO_CLAIM",request.path("claimMode").asText());
                assertEquals(4,request.path("parallelism").asInt());answer="{\"jobid\":\""+JOB+"\"}";
            } else if(path.endsWith("/checkpoints")) answer="{\"latest\":{\"restored\":{\"is_savepoint\":true,\"external_path\":\""+LOCATION+"\"}}}";
            else answer="{\"state\":\"RUNNING\",\"vertices\":[{\"parallelism\":4},{\"parallelism\":1}]}";
            return new FlinkRestApiClient.HttpResponse(200,answer.getBytes(StandardCharsets.UTF_8));
        },json)) {
            var job=new FlinkJobHandle(JOB);
            assertEquals(TRIGGER,client.stopWithSavepoint(job,TRIGGER,"file:/flink/checkpoints/attempt",Duration.ofSeconds(5)));
            assertEquals(LOCATION,client.savepointStatus(job,TRIGGER,Duration.ofSeconds(5)).location());
            client.submit(new FlinkJobSubmission("jar",4,Map.of(),List.of(),Optional.of(LOCATION)),Duration.ofSeconds(5));
            var proof=client.restoredSavepoint(job,Duration.ofSeconds(5));assertTrue(proof.savepoint());assertEquals(4,proof.parallelism());
            assertEquals(2,calls.stream().filter(c->c.startsWith("POST ")).count());
        }
    }
    @Test void neverRetriesAnUncertainStopOrSubmission() throws Exception {
        for(boolean stop:List.of(true,false)) {
            var count=new java.util.concurrent.atomic.AtomicInteger();
            try(var client=new FlinkRestApiClient((method,path,body,timeout)->{count.incrementAndGet();throw new IOException("unknown outcome");},json)) {
                assertThrows(IOException.class,()->{
                    if(stop)client.stopWithSavepoint(new FlinkJobHandle(JOB),TRIGGER,"file:/flink/checkpoints/x",Duration.ofSeconds(5));
                    else client.submit(new FlinkJobSubmission("jar",1,Map.of(),List.of(),Optional.of(LOCATION)),Duration.ofSeconds(5));
                });assertEquals(1,count.get());
            }
        }
    }
    @Test void rejectsAmbiguousCompletionAndKeepsTheFailureResponse() throws Exception {
        for(String operation:List.of("{}","{\"location\":\"x\",\"failure-cause\":{}}")) {
            String answer="{\"status\":{\"id\":\"COMPLETED\"},\"operation\":"+operation+"}";
            assertThrows(IOException.class,()->FlinkSavepoint.status(new FlinkJobHandle(JOB),TRIGGER,Duration.ofSeconds(1),
                    (method,path,body,deadline)->json.readTree(answer),System::nanoTime));
        }
        var status=FlinkSavepoint.status(new FlinkJobHandle(JOB),TRIGGER,Duration.ofSeconds(1),(method,path,body,deadline)->
                json.readTree("{\"status\":{\"id\":\"COMPLETED\"},\"operation\":{\"failure-cause\":{\"message\":\"disk full\"}}}"),System::nanoTime);
        assertTrue(status.completed());assertTrue(status.failure().contains("disk full"));assertNull(status.location());
    }
}
