package org.savonitar.flink.stability.core.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.MultipartBody;
import org.savonitar.flink.stability.runtime.api.MonotonicDeadline;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Typed Flink 2.2 REST boundary used by v1 execution.
 *
 * <p>This client preserves program arguments as an ordered list and requires complete state
 * restoration for restored submissions.</p>
 */
public final class FlinkRestApiClient implements FlinkScenarioControl {
    private static final MediaType JSON = MediaType.get("application/json");
    private static final MediaType JAR = MediaType.get("application/java-archive");
    private static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration OBSERVATION_TIMEOUT = Duration.ofSeconds(30);
    private static final long POLL_INTERVAL_MILLIS = 250;
    private static final int MAX_ERROR_BODY_CHARS = 4096;
    private static final int MAX_OBSERVED_FAILURES = 20;
    private static final int MAX_ROOT_CAUSE_CHARS = 300;

    private final Transport transport;
    private final ObjectMapper mapper;
    private final LongSupplier nanoTime;

    public FlinkRestApiClient(String jobManagerUrl) {
        this(new OkHttpTransport(normalizeUrl(jobManagerUrl), new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(60, TimeUnit.SECONDS)
                .build()), new ObjectMapper(), System::nanoTime);
    }

    FlinkRestApiClient(Transport transport, ObjectMapper mapper) {
        this(transport, mapper, System::nanoTime);
    }

    FlinkRestApiClient(
            Transport transport,
            ObjectMapper mapper,
            LongSupplier nanoTime) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    @Override
    public String uploadJar(Path jar, String expectedSha256) throws IOException {
        Objects.requireNonNull(jar, "jar");
        Objects.requireNonNull(expectedSha256, "expectedSha256");
        Path normalized = jar.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)
                || !Files.isReadable(normalized)) {
            throw new IOException("Prepared workload JAR is not a readable regular file: "
                    + normalized);
        }
        JsonNode response;
        try (UploadSnapshot snapshot = snapshotForUpload(normalized)) {
            String actualSha256 = sha256(snapshot.path());
            if (!expectedSha256.equals(actualSha256)) {
                throw new IOException(
                        "Prepared workload JAR changed before upload; expected SHA-256 "
                                + expectedSha256 + ", actual " + actualSha256);
            }
            response = mapper.readTree(
                    transport.uploadJar(snapshot.path(), DEFAULT_CALL_TIMEOUT));
        }
        String filename = response.path("filename").asText();
        if (filename.isBlank()) {
            throw new IOException("Flink JAR-upload response did not contain filename");
        }
        int separator = Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'));
        String uploadedJarId = filename.substring(separator + 1);
        if (uploadedJarId.isBlank()) {
            throw new IOException("Flink JAR-upload response contained an invalid filename");
        }
        return uploadedJarId;
    }

    private static UploadSnapshot snapshotForUpload(Path source) throws IOException {
        Path snapshot = Files.createTempFile(
                source.getParent(), ".flink-stability-upload-", ".jar");
        try {
            try (var input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS);
                    var output = Files.newOutputStream(
                            snapshot,
                            java.nio.file.StandardOpenOption.WRITE,
                            java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
                            LinkOption.NOFOLLOW_LINKS)) {
                input.transferTo(output);
            }
            return new UploadSnapshot(snapshot);
        } catch (IOException | RuntimeException failure) {
            try {
                Files.deleteIfExists(snapshot);
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
                byte[] buffer = new byte[8192];
                for (int read; (read = input.read(buffer)) >= 0;) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java must provide SHA-256", impossible);
        }
    }

    private record UploadSnapshot(Path path) implements AutoCloseable {
        private UploadSnapshot {
            Objects.requireNonNull(path, "path");
        }

        @Override
        public void close() throws IOException {
            Files.deleteIfExists(path);
        }
    }

    public FlinkJobHandle submit(FlinkJobSubmission submission) throws IOException {
        Objects.requireNonNull(submission, "submission");
        ObjectNode body = mapper.createObjectNode();
        body.put("parallelism", submission.parallelism());
        body.set("flinkConfiguration", mapper.valueToTree(submission.flinkConfiguration()));
        body.set("programArgsList", mapper.valueToTree(submission.programArguments()));

        JsonNode response = post(
                "/jars/" + pathSegment(submission.uploadedJarId()) + "/run",
                body,
                null);
        JsonNode jobId = response.get("jobid");
        if (jobId == null || jobId.asText().isBlank()) {
            throw new IOException("Flink JAR-run response did not contain jobid");
        }
        return new FlinkJobHandle(jobId.asText());
    }

    public FlinkJobState jobState(FlinkJobHandle job) throws IOException {
        return jobState(job, null);
    }

    @Override
    public FlinkJobState awaitState(
            FlinkJobHandle job,
            FlinkJobState expected,
            Duration timeout) throws IOException {
        Objects.requireNonNull(expected, "expected");
        MonotonicDeadline deadline = MonotonicDeadline.start(timeout, nanoTime);
        while (true) {
            FlinkJobState state = jobState(job, deadline);
            if (state == expected) {
                return state;
            }
            if (state.terminal()) {
                throw new IOException("Flink job reached " + state
                        + " before expected state " + expected);
            }
            pauseBeforeNextPoll(deadline);
        }
    }

    @Override
    public long awaitCompletedCheckpoints(
            FlinkJobHandle job,
            long minimumCompleted,
            Duration timeout) throws IOException {
        Objects.requireNonNull(job, "job");
        if (minimumCompleted < 1) {
            throw new IllegalArgumentException("minimumCompleted must be positive");
        }
        MonotonicDeadline deadline = MonotonicDeadline.start(timeout, nanoTime);
        while (true) {
            long completed = completedCheckpointCount(job, deadline);
            if (completed >= minimumCompleted) {
                return completed;
            }
            FlinkJobState state = jobState(job, deadline);
            if (state.terminal()) {
                // The checkpoint statistics endpoint can lag the job-state endpoint briefly.
                // Refresh once under the same absolute deadline before deciding that a terminal
                // job missed the requested checkpoint count.
                completed = completedCheckpointCount(job, deadline);
                if (completed >= minimumCompleted) {
                    return completed;
                }
                throw new IOException("Flink job reached " + state + " after only "
                        + completed + " completed checkpoints; expected at least "
                        + minimumCompleted);
            }
            pauseBeforeNextPoll(deadline);
        }
    }

    private long completedCheckpointCount(
            FlinkJobHandle job,
            MonotonicDeadline deadline) throws IOException {
        JsonNode response = get(
                "/jobs/" + pathSegment(job.jobId()) + "/checkpoints", deadline);
        return count(response, "completed");
    }

    @Override
    public long jobManagerTimeMillis(FlinkJobHandle job) throws IOException {
        Objects.requireNonNull(job, "job");
        MonotonicDeadline deadline = MonotonicDeadline.start(OBSERVATION_TIMEOUT, nanoTime);
        return requiredLong(get("/jobs/" + pathSegment(job.jobId()), deadline), "now");
    }

    @Override
    public FlinkJobObservation observe(FlinkJobHandle job) throws IOException {
        Objects.requireNonNull(job, "job");
        MonotonicDeadline deadline = MonotonicDeadline.start(OBSERVATION_TIMEOUT, nanoTime);
        String jobPath = "/jobs/" + pathSegment(job.jobId());
        JsonNode details = get(jobPath, deadline);
        JsonNode checkpoints = get(jobPath + "/checkpoints", deadline);
        JsonNode exceptions = get(
                jobPath + "/exceptions?maxExceptions=" + MAX_OBSERVED_FAILURES, deadline);

        List<FlinkJobObservation.Subtask> subtasks = new ArrayList<>();
        for (JsonNode vertex : details.path("vertices")) {
            String vertexName = vertex.path("name").asText();
            JsonNode vertexDetails = get(
                    jobPath + "/vertices/" + pathSegment(requiredText(vertex, "id")), deadline);
            for (JsonNode subtask : vertexDetails.path("subtasks")) {
                subtasks.add(new FlinkJobObservation.Subtask(
                        vertexName,
                        requiredInt(subtask, "subtask"),
                        requiredInt(subtask, "attempt"),
                        requiredText(subtask, "status"),
                        taskManagerId(subtask.path("taskmanager-id"))));
            }
        }

        List<FlinkJobObservation.Failure> failures = new ArrayList<>();
        for (JsonNode entry : exceptions.path("exceptionHistory").path("entries")) {
            failures.add(new FlinkJobObservation.Failure(
                    requiredLong(entry, "timestamp"),
                    entry.path("exceptionName").asText(),
                    rootCause(entry.path("stacktrace").asText()),
                    taskManagerId(entry.path("taskManagerId"))));
        }

        JsonNode restore = checkpoints.path("latest").path("restored");
        Optional<FlinkJobObservation.Restore> latestRestore = restore.isObject()
                ? Optional.of(new FlinkJobObservation.Restore(
                        requiredLong(restore, "id"), requiredLong(restore, "restore_timestamp")))
                : Optional.empty();
        return new FlinkJobObservation(
                requiredLong(details, "now"),
                parseState(details.path("state").asText()),
                count(checkpoints, "completed"),
                count(checkpoints, "restored"),
                latestRestore,
                failures,
                subtasks);
    }

    private static long count(JsonNode checkpoints, String name) throws IOException {
        JsonNode count = checkpoints.path("counts").path(name);
        if (!count.canConvertToLong() || count.asLong() < 0) {
            throw new IOException(
                    "Flink checkpoint response did not contain a valid " + name + " count");
        }
        return count.asLong();
    }

    private static long requiredLong(JsonNode node, String field) throws IOException {
        JsonNode value = node.path(field);
        if (!value.canConvertToLong()) {
            throw new IOException("Flink REST response did not contain numeric " + field);
        }
        return value.asLong();
    }

    private static int requiredInt(JsonNode node, String field) throws IOException {
        JsonNode value = node.path(field);
        if (!value.canConvertToInt()) {
            throw new IOException("Flink REST response did not contain integer " + field);
        }
        return value.asInt();
    }

    private static String requiredText(JsonNode node, String field) throws IOException {
        String value = node.path(field).asText();
        if (value.isBlank()) {
            throw new IOException("Flink REST response did not contain " + field);
        }
        return value;
    }

    /** Flink reports an undeployed subtask's TaskManager as "(unassigned)". */
    private static Optional<String> taskManagerId(JsonNode node) {
        String value = node.asText();
        return value.isBlank() || value.equals("(unassigned)")
                ? Optional.empty()
                : Optional.of(value);
    }

    /** The innermost "Caused by:" line, or the first line when there is no cause chain. */
    private static String rootCause(String stackTrace) {
        List<String> lines = stackTrace.lines().toList();
        String rootCause = lines.isEmpty() ? "" : lines.getFirst();
        for (String line : lines) {
            if (line.startsWith("Caused by: ")) {
                rootCause = line.substring("Caused by: ".length());
            }
        }
        return rootCause.length() <= MAX_ROOT_CAUSE_CHARS
                ? rootCause
                : rootCause.substring(0, MAX_ROOT_CAUSE_CHARS) + "…";
    }

    @Override
    public FlinkJobState awaitFinished(FlinkJobHandle job, Duration timeout) throws IOException {
        FlinkJobState finalState = awaitTerminalState(
                job, MonotonicDeadline.start(timeout, nanoTime));
        if (finalState != FlinkJobState.FINISHED) {
            throw new IOException(
                    "Flink job ended in " + finalState + " instead of FINISHED");
        }
        return finalState;
    }

    private FlinkJobState awaitTerminalState(
            FlinkJobHandle job,
            MonotonicDeadline deadline) throws IOException {
        while (true) {
            FlinkJobState state = jobState(job, deadline);
            if (state.terminal()) {
                return state;
            }
            pauseBeforeNextPoll(deadline);
        }
    }

    private FlinkJobState jobState(FlinkJobHandle job, MonotonicDeadline deadline) throws IOException {
        Objects.requireNonNull(job, "job");
        JsonNode response = get("/jobs/" + pathSegment(job.jobId()), deadline);
        return parseState(response.path("state").asText());
    }

    private static FlinkJobState parseState(String state) throws IOException {
        try {
            return FlinkJobState.valueOf(state.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException failure) {
            throw new IOException("Unknown Flink job state: " + state, failure);
        }
    }

    private JsonNode get(String endpoint, MonotonicDeadline deadline) throws IOException {
        return execute("GET", endpoint, null, deadline);
    }

    private JsonNode post(String endpoint, JsonNode body, MonotonicDeadline deadline) throws IOException {
        return execute("POST", endpoint, mapper.writeValueAsBytes(body), deadline);
    }

    private JsonNode execute(
            String method,
            String endpoint,
            byte[] requestBody,
            MonotonicDeadline deadline) throws IOException {
        Duration remaining = deadline == null ? DEFAULT_CALL_TIMEOUT : remaining(deadline);
        byte[] response;
        try {
            response = transport.execute(method, endpoint, requestBody, remaining);
        } catch (InterruptedIOException timeout) {
            if (deadline != null) {
                throw new FlinkRestTimeoutException(
                        "Timed out waiting for Flink operation completion", timeout);
            }
            throw timeout;
        }
        if (response == null || response.length == 0) {
            throw new IOException("Flink REST response body was empty for " + endpoint);
        }
        return mapper.readTree(response);
    }

    private static String pathSegment(String value) {
        if (value.indexOf('/') >= 0 || value.indexOf('\\') >= 0 || value.contains("..")) {
            throw new IllegalArgumentException("Unsafe Flink REST path segment: " + value);
        }
        return value;
    }

    private static String bounded(Object value) {
        String text = String.valueOf(value);
        return text.length() <= MAX_ERROR_BODY_CHARS
                ? text
                : text.substring(0, MAX_ERROR_BODY_CHARS) + "…";
    }

    @Override
    public void close() {
        transport.close();
    }

    private static String normalizeUrl(String jobManagerUrl) {
        if (jobManagerUrl == null || jobManagerUrl.isBlank()) {
            throw new IllegalArgumentException("jobManagerUrl must not be blank");
        }
        return jobManagerUrl.endsWith("/")
                ? jobManagerUrl.substring(0, jobManagerUrl.length() - 1)
                : jobManagerUrl;
    }

    interface Transport extends AutoCloseable {
        byte[] execute(String method, String endpoint, byte[] body, Duration timeout)
                throws IOException;

        default byte[] uploadJar(Path jar, Duration timeout) throws IOException {
            throw new IOException("This Flink transport does not support JAR upload");
        }

        @Override
        default void close() {}
    }

    private static final class OkHttpTransport implements Transport {
        private final String jobManagerUrl;
        private final OkHttpClient client;

        private OkHttpTransport(String jobManagerUrl, OkHttpClient client) {
            this.jobManagerUrl = jobManagerUrl;
            this.client = client;
        }

        @Override
        public byte[] execute(
                String method,
                String endpoint,
                byte[] body,
                Duration timeout) throws IOException {
            Request.Builder builder = new Request.Builder().url(jobManagerUrl + endpoint);
            if ("GET".equals(method)) {
                builder.get();
            } else if ("POST".equals(method)) {
                builder.post(RequestBody.create(body, JSON));
            } else {
                throw new IllegalArgumentException("Unsupported HTTP method: " + method);
            }
            Request request = builder.build();
            Call call = client.newCall(request);
            call.timeout().timeout(safePositiveNanos(timeout), TimeUnit.NANOSECONDS);
            try (Response response = call.execute()) {
                byte[] responseBody = response.body() == null
                        ? new byte[0]
                        : response.body().bytes();
                if (!response.isSuccessful()) {
                    throw new IOException("Flink REST " + method + " " + endpoint
                            + " failed with HTTP " + response.code() + ": "
                            + bounded(new String(
                                    responseBody, java.nio.charset.StandardCharsets.UTF_8)));
                }
                return responseBody;
            }
        }

        @Override
        public byte[] uploadJar(Path jar, Duration timeout) throws IOException {
            RequestBody multipart = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart(
                            "jarfile",
                            jar.getFileName().toString(),
                            RequestBody.create(jar.toFile(), JAR))
                    .build();
            Request request = new Request.Builder()
                    .url(jobManagerUrl + "/jars/upload")
                    .post(multipart)
                    .build();
            Call call = client.newCall(request);
            call.timeout().timeout(safePositiveNanos(timeout), TimeUnit.NANOSECONDS);
            try (Response response = call.execute()) {
                byte[] responseBody = response.body() == null
                        ? new byte[0]
                        : response.body().bytes();
                if (!response.isSuccessful()) {
                    throw new IOException("Flink REST POST /jars/upload failed with HTTP "
                            + response.code() + ": " + bounded(new String(
                                    responseBody,
                                    java.nio.charset.StandardCharsets.UTF_8)));
                }
                if (responseBody.length == 0) {
                    throw new IOException("Flink REST response body was empty for /jars/upload");
                }
                return responseBody;
            }
        }

        @Override
        public void close() {
            client.connectionPool().evictAll();
        }

        private static long safePositiveNanos(Duration timeout) {
            try {
                return Math.max(1, timeout.toNanos());
            } catch (ArithmeticException overflow) {
                return Long.MAX_VALUE;
            }
        }
    }

    private static Duration remaining(MonotonicDeadline deadline) throws IOException {
        return deadline.remainingOrThrow(() -> new FlinkRestTimeoutException(
                "Timed out waiting for Flink operation completion"));
    }

    private static void pauseBeforeNextPoll(MonotonicDeadline deadline) throws IOException {
        long sleepNanos = Math.min(
                TimeUnit.MILLISECONDS.toNanos(POLL_INTERVAL_MILLIS),
                remaining(deadline).toNanos());
        try {
            TimeUnit.NANOSECONDS.sleep(sleepNanos);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for Flink", interrupted);
        }
    }
}
