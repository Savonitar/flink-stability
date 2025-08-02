package org.savonitar.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A client for interacting with the Flink REST API for chaos testing scenarios.
 * This class provides methods to manage Flink jobs: upload JAR files, 
 * retrieve job statuses, and interact with savepoints.
 * <p>
 * Instances of this class are thread-safe. It is recommended to use this client 
 * in a try-with-resources block or explicitly call the {@link #close()} method 
 * to release resources properly.
 */
public class FlinkRestClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRestClient.class);
    private static final String MEDIA_TYPE_JSON = "application/json";
    private static final String MEDIA_TYPE_JAR = "application/java-archive";
    private static final int CONNECT_TIMEOUT_SECONDS = 30;
    private static final int READ_TIMEOUT_SECONDS = 60;
    private static final int WRITE_TIMEOUT_SECONDS = 60;
    private static final int SAVEPOINT_POLL_RETRIES = 30; // 30 * 2s = 1 minute
    private static final int SAVEPOINT_POLL_DELAY_MS = 2000;

    private final OkHttpClient client;
    private final String jobManagerUrl;
    private final ObjectMapper objectMapper;

    public FlinkRestClient(String jobManagerUrl) {
        if (jobManagerUrl == null || jobManagerUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Job manager URL cannot be null or empty");
        }
        this.jobManagerUrl = normalizeUrl(jobManagerUrl);
        this.client = createHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    private String normalizeUrl(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private OkHttpClient createHttpClient() {
        return new OkHttpClient.Builder()
                .connectTimeout(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .readTimeout(READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .writeTimeout(WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build();
    }

    private Request.Builder createRequestBuilder(String endpoint) {
        return new Request.Builder().url(jobManagerUrl + endpoint);
    }

    private String handleJsonResponse(Response response, String errorMessage) throws IOException {
        if (!response.isSuccessful()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            throw new IOException(errorMessage + ": " + response.code() + " - " + responseBody);
        }
        return response.body().string();
    }

    private JsonNode parseJsonResponse(String responseBody) throws IOException {
        return objectMapper.readTree(responseBody);
    }

    public String uploadJar(String jarPath) throws IOException {
        validateJarFile(jarPath);
        File jarFile = new File(jarPath);

        RequestBody requestBody = createJarUploadRequestBody(jarFile);
        Request request = createRequestBuilder("/jars/upload")
                .post(requestBody)
                .build();

        try (Response response = client.newCall(request).execute()) {
            String responseBody = handleJsonResponse(response, "Failed to upload jar");
            return extractFileNameFromResponse(responseBody);
        }
    }

    private void validateJarFile(String jarPath) throws IOException {
        if (jarPath == null || jarPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Jar path cannot be null or empty");
        }
        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new IOException("Jar file does not exist: " + jarPath);
        }
        if (!jarFile.canRead()) {
            throw new IOException("Cannot read jar file: " + jarPath);
        }
    }

    private RequestBody createJarUploadRequestBody(File jarFile) {
        return new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("jarfile", jarFile.getName(),
                        RequestBody.create(jarFile, MediaType.parse(MEDIA_TYPE_JAR)))
                .build();
    }

    private String extractFileNameFromResponse(String responseBody) throws IOException {
        JsonNode root = parseJsonResponse(responseBody);
        String fullPath = root.get("filename").asText();
        return fullPath.substring(fullPath.lastIndexOf("/") + 1);
    }

    @Override
    public void close() {
        client.connectionPool().evictAll();
    }

    public String stopJobWithSavepoint(String jobId, String targetDirectory) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> body = new HashMap<>();
        body.put("targetDirectory", targetDirectory);
        String json = mapper.writeValueAsString(body);
        LOG.info("Stopping job {} with savepoint to '{}'", jobId, targetDirectory);
        Request request = new Request.Builder()
                .url(jobManagerUrl + "/jobs/" + jobId + "/stop")
                .post(RequestBody.create(json, MediaType.parse("application/json")))
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "";
                throw new IOException("Failed to stop job with savepoint: " + response.code() + " - " + responseBody);
            }
            String responseBody = response.body().string();
            JsonNode root = mapper.readTree(responseBody);
            String requestId = root.get("request-id").asText();
            LOG.info("Stop-with-savepoint initiated, request-id: {}", requestId);
            return pollForSavepointCompletion(jobId, requestId);
        }
    }


    private String pollForSavepointCompletion(String jobId, String requestId) throws IOException {
        String url = jobManagerUrl + "/jobs/" + jobId + "/savepoints/" + requestId;
        for (int attempt = 0; attempt < SAVEPOINT_POLL_RETRIES; attempt++) {
            try {
                Thread.sleep(SAVEPOINT_POLL_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for savepoint completion", e);
            }
            
            try {
                Request request = new Request.Builder().url(url).get().build();
                try (Response response = client.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        LOG.warn("Savepoint poll attempt {} failed with status: {}", attempt + 1, response.code());
                        continue;
                    }
                    
                    String body = response.body().string();
                    JsonNode root = objectMapper.readTree(body);
                    String status = root.get("status").get("id").asText();
                    LOG.info("Savepoint status: {}", status);
                    
                    if ("COMPLETED".equals(status)) {
                        JsonNode operation = root.get("operation");
                        String location = operation.get("location").asText();
                        LOG.info("Savepoint completed: {}", location);
                        return location;
                    } else if ("FAILED".equals(status)) {
                        String failureReason = root.get("status").get("failure-cause").asText();
                        throw new IOException("Savepoint creation failed: " + failureReason);
                    }
                    // Continue polling for IN_PROGRESS or other statuses
                }
            } catch (IOException e) {
                LOG.warn("Network error during savepoint poll attempt {}: {}", attempt + 1, e.getMessage());
                // Continue retrying for network issues
            }
        }
        
        throw new IOException("Timed out waiting for savepoint completion after " + SAVEPOINT_POLL_RETRIES + " attempts.");
    }
    
    public List<String> availableJars() throws IOException {
        Request request = new Request.Builder()
                .url(jobManagerUrl + "/jars")
                .get()
                .build();
        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                throw new IOException("Failed to get jars: " + response.code() + " - " + responseBody);
            }
            LOG.debug("Jars Response body: {}", responseBody);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(responseBody);
            JsonNode files = root.get("files");
            List<String> jarIds = new ArrayList<>();
            if (files != null && files.isArray()) {
                for (JsonNode file : files) {
                    jarIds.add(file.get("id").asText());
                }
            }
            return jarIds;
        }
    }

    public String getJobStatus(String jobId) throws IOException {
        Request request = new Request.Builder()
                .url(jobManagerUrl + "/jobs/" + jobId)
                .get()
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Failed to get job status: " + response.code() + " - " + response.message());
            }
            String responseBody = response.body().string();
            LOG.debug("Job status response: {}", responseBody);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(responseBody);
            return root.get("state").asText();
        }
    }

    public String runJob(String jarId, String programArgs) throws IOException {
        return runJob(jarId, programArgs, null);
    }

    public String runJob(String jarId, String programArgs, String savepointPath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jobConfig = new HashMap<>();
        String[] argsArray = programArgs.trim().split("\\s+");
        jobConfig.put("programArgsList", argsArray);
        if (savepointPath != null) {
            jobConfig.put("savepointPath", savepointPath);
        }
        String json = mapper.writeValueAsString(jobConfig);
        LOG.info("Sending job config JSON: {}", json);
        RequestBody requestBody = RequestBody.create(
                MediaType.parse(MEDIA_TYPE_JSON),
                json
        );
        String url = jobManagerUrl + "/jars/" + jarId + "/run";
        LOG.info("Request URL: {}", url);
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();
        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body().string();
            if (!response.isSuccessful()) {
                LOG.error("Failed to start job. Status: {}, Body: {}", response.code(), responseBody);
                throw new IOException("Failed to start job: " + response.code() + " - " + responseBody);
            }
            LOG.info("Job started successfully: {}", responseBody);
            return responseBody;
        }
    }
}
