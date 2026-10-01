package org.savonitar.flink.stability.testcontainers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Loopback-only token fixture. Docker reaches it through Testcontainers' explicit host tunnel. */
final class SyntheticTokenService implements TokenServiceControl, AutoCloseable {
    private static final int MAX_BODY = 16_384;
    private static final int MAX_EVENTS = 10_000;
    private static final int MAX_PROVIDERS = 128;
    private final HttpServer server;
    private final ThreadPoolExecutor executor;
    private final long lifetimeMillis;
    private final int eventLimit;
    private final String serviceId = UUID.randomUUID().toString();
    private final List<Event> events = new ArrayList<>();
    private final Map<Long, Issued> issued = new HashMap<>();
    private final Map<String, Ledger> providers = new HashMap<>();
    private int journalRecords;
    private Settings settings = new Settings(0, Mode.HEALTHY, Duration.ZERO);
    private Optional<JobTarget> jobTarget = Optional.empty();
    private long eventSequence;
    private long requestSequence;
    private long tokenSequence;
    private int activeRequests;
    private int maxConcurrentRequests;
    private boolean overflow;
    private boolean saturated;
    private boolean closed;

    static SyntheticTokenService start(Duration tokenLifetime) throws IOException {
        return new SyntheticTokenService(tokenLifetime, MAX_EVENTS, 8, 32);
    }

    /** Smaller bounds make overflow and saturation observable without large test loads. */
    SyntheticTokenService(Duration tokenLifetime, int eventLimit, int workers, int queueSize)
            throws IOException {
        Objects.requireNonNull(tokenLifetime, "tokenLifetime");
        if (tokenLifetime.compareTo(Duration.ofMillis(100)) < 0
                || tokenLifetime.compareTo(Duration.ofHours(1)) > 0
                || eventLimit < 1 || workers < 1 || queueSize < 1) {
            throw new IllegalArgumentException("Invalid synthetic token service bounds");
        }
        lifetimeMillis = tokenLifetime.toMillis();
        this.eventLimit = eventLimit;
        server = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 32);
        executor = new ThreadPoolExecutor(workers, workers, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueSize), task -> {
                    Thread thread = new Thread(task, "flink-stability-token-service");
                    thread.setDaemon(true);
                    return thread;
                }, (task, pool) -> {
                    synchronized (this) {
                        if (!closed) {
                            saturated = true;
                            record(Kind.REJECTED, "service", "service", 0, settings,
                                    OptionalLong.empty(), "HTTP worker queue saturated");
                        }
                    }
                    throw new RejectedExecutionException("Synthetic token HTTP worker queue saturated");
                });
        server.setExecutor(executor);
        server.createContext("/", this::handle);
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    @Override
    public synchronized long configure(Mode mode, Duration delay) {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(delay, "delay");
        if (closed || delay.isNegative() || delay.compareTo(Duration.ofSeconds(30)) > 0
                || (mode == Mode.DELAY ? delay.compareTo(Duration.ofMillis(1)) < 0 : !delay.isZero())) {
            throw new IllegalArgumentException("Invalid synthetic token fault or closed service");
        }
        jobTarget = Optional.empty();
        settings = new Settings(settings.revision() + 1, mode, delay);
        record(Kind.MODE_CHANGED, "service", "service", 0, settings,
                OptionalLong.empty(), "delayMillis=" + delay.toMillis());
        notifyAll(); // Healing also releases requests currently held by the delay fault.
        return settings.revision();
    }

    @Override
    public synchronized long configure(Mode mode, Duration delay, JobTarget target) {
        if (mode != Mode.DELAY && mode != Mode.FAIL) {
            throw new IllegalArgumentException("Submitted-job targeting supports delay and fail only");
        }
        Objects.requireNonNull(target, "target");
        long revision = configure(mode, delay);
        jobTarget = Optional.of(target);
        return revision;
    }

    @Override
    public synchronized Snapshot snapshot() {
        return new Snapshot(events, overflow, saturated, activeRequests, maxConcurrentRequests);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String process = exchange.getRequestHeaders().getFirst("X-Flink-Stability-Process");
            String role = exchange.getRequestHeaders().getFirst("X-Flink-Stability-Role");
            String clientRequest = exchange.getRequestHeaders().getFirst("X-Flink-Stability-Request");
            String instance = exchange.getRequestHeaders().getFirst("X-Flink-Stability-Instance");
            String path = exchange.getRequestURI().getPath();
            if (!"POST".equals(exchange.getRequestMethod())
                    || exchange.getRequestURI().getRawQuery() != null
                    || process == null || !process.matches("(?:jobmanager|taskmanager)-[1-9][0-9]*#[1-9][0-9]*")
                    || role == null || !List.of("jobmanager", "taskmanager").contains(role)
                    || !process.startsWith(role + "-")
                    || clientRequest == null || !clientRequest.matches("[1-9][0-9]{0,18}")
                    || !canonicalUuid(instance)) {
                reply(exchange, 400, new byte[0]);
                return;
            }
            byte[] body = exchange.getRequestBody().readNBytes(MAX_BODY + 1);
            if (body.length > MAX_BODY) {
                reply(exchange, 413, new byte[0]);
                return;
            }
            if (path.equals("/token")) {
                final RegistrationSnapshot registration;
                try {
                    registration = parseRequest(body);
                    if (!role.equals("jobmanager") || !registration.providerInstance().equals(instance)) {
                        throw new IllegalArgumentException("Invalid provider identity");
                    }
                } catch (IllegalArgumentException | CharacterCodingException invalid) {
                    synchronized (this) {
                        record(Kind.REJECTED, process, role, 0, settings,
                                OptionalLong.empty(), "Invalid acquisition snapshot", null, instance);
                    }
                    reply(exchange, 400, new byte[0]);
                    return;
                }
                acquire(exchange, process, role, instance, registration);
                return;
            }
            int status;
            synchronized (this) {
                if (closed || overflow || saturated) {
                    status = 503;
                } else if (path.equals("/fault-observed")) {
                    status = acknowledgeFault(process, role, instance, body);
                } else if (path.equals("/receipt")) {
                    Long token = issued.entrySet().stream()
                            .filter(item -> Arrays.equals(body, item.getValue().bytes()))
                            .map(Map.Entry::getKey).findFirst().orElse(null);
                    if (token == null) {
                        record(Kind.REJECTED, process, role, 0, settings,
                                OptionalLong.empty(), "Unrecognized synthetic token receipt", null, instance);
                        status = 400;
                    } else {
                        record(Kind.RECEIVED, process, role, 0, settings,
                                OptionalLong.of(token), "receiver acknowledged issued token",
                                issued.get(token).registration(), instance);
                        status = overflow ? 503 : 200;
                    }
                } else if ((path.equals("/init-provider") || path.equals("/init-receiver"))
                        && body.length == 0) {
                    record(path.equals("/init-provider") ? Kind.PROVIDER_INITIALIZED : Kind.RECEIVER_INITIALIZED,
                            process, role, 0, settings, OptionalLong.empty(), "SPI initialized", null, instance);
                    status = overflow ? 503 : 200;
                } else {
                    status = 404;
                }
            }
            reply(exchange, status, new byte[0]);
        }
    }

    /** Called under the service monitor; acknowledgements refer to an actual failed request. */
    private int acknowledgeFault(String process, String role, String instance, byte[] body) {
        String[] fields = new String(body, StandardCharsets.UTF_8).split("\n", -1);
        final long request, revision;
        final int status;
        try {
            if (fields.length != 4 || !fields[3].isEmpty()) return 400;
            request = Long.parseLong(fields[0]);
            revision = Long.parseLong(fields[1]);
            status = Integer.parseInt(fields[2]);
        } catch (NumberFormatException invalid) {
            return 400;
        }
        var start = events.stream().filter(event -> event.kind() == Kind.REQUEST_STARTED
                && event.requestId() == request && event.revision() == revision
                && event.process().equals(process) && event.role().equals(role)
                && event.participantInstance().filter(instance::equals).isPresent()).findFirst();
        if (start.isEmpty() || !((status == 503 && start.get().mode() == Mode.FAIL)
                || (status == 598 && start.get().mode() == Mode.LINKAGE_ERROR))
                || events.stream().noneMatch(event -> event.requestId() == request
                        && event.kind() == Kind.FAILED && event.detail().equals("HTTP " + status))
                || events.stream().anyMatch(event -> event.requestId() == request
                        && (event.kind() == Kind.FAULT_OBSERVED
                        || event.kind() == Kind.FAILED && !event.detail().equals("HTTP " + status)))) {
            return 400;
        }
        record(Kind.FAULT_OBSERVED, process, role, request,
                new Settings(revision, start.get().mode(), Duration.ZERO),
                OptionalLong.empty(), "HTTP " + status, start.get().registration().orElse(null), instance);
        return overflow ? 503 : 200;
    }

    private void acquire(HttpExchange exchange, String process, String role, String instance,
                         RegistrationSnapshot registration) throws IOException {
        Settings selected;
        long request;
        synchronized (this) {
            if (closed || overflow || saturated) {
                reply(exchange, 503, new byte[0]);
                return;
            }
            if (!ingest(process, role, registration)) {
                record(Kind.REJECTED, process, role, 0, settings, OptionalLong.empty(),
                        "Conflicting or incomplete lifecycle prefix", registration, instance);
                reply(exchange, overflow ? 503 : 400, new byte[0]);
                return;
            }
            selected = jobTarget.isPresent() && !("JOB".equals(registration.scope())
                    && jobTarget.orElseThrow().jobId().equals(registration.jobId())
                    && jobTarget.orElseThrow().jobAlias().equals(registration.jobAlias())
                    && !registration.coverageInvalid())
                    ? new Settings(settings.revision(), Mode.HEALTHY, Duration.ZERO) : settings;
            request = ++requestSequence;
            activeRequests++;
            maxConcurrentRequests = Math.max(maxConcurrentRequests, activeRequests);
            record(Kind.REQUEST_STARTED, process, role, request, selected,
                    OptionalLong.empty(), "acquisition entered", registration, instance);
        }
        try {
            if (selected.mode() == Mode.DELAY) {
                long deadline = System.nanoTime() + selected.delay().toNanos();
                synchronized (this) {
                    for (long remaining; !closed && settings.revision() == selected.revision()
                            && (remaining = deadline - System.nanoTime()) > 0;) {
                        TimeUnit.NANOSECONDS.timedWait(this, remaining);
                    }
                }
            }
            int status;
            byte[] token = new byte[0];
            synchronized (this) {
                if (closed || overflow || saturated || selected.mode() == Mode.FAIL) {
                    status = 503;
                } else if (selected.mode() == Mode.LINKAGE_ERROR) {
                    status = 598;
                } else {
                    long sequence = ++tokenSequence;
                    long now = System.currentTimeMillis();
                    token = ("flink-stability-token-v2\n" + serviceId + "\n" + sequence
                            + "\n" + now + "\n" + (now + lifetimeMillis) + "\n"
                            + registration.providerInstance() + "\n" + registration.scope() + "\n"
                            + registration.generation() + "\n" + registration.jobId() + "\n"
                            + encodedAlias(registration.jobId(), registration.jobAlias()) + "\n"
                            + registration.coverageInvalid() + "\n" + registration.sentThrough() + "\n")
                            .getBytes(StandardCharsets.UTF_8);
                    record(Kind.ISSUED, process, role, request, selected,
                            OptionalLong.of(sequence), "synthetic token issued", registration, instance);
                    if (overflow) {
                        token = new byte[0];
                        status = 503;
                    } else {
                        issued.put(sequence, new Issued(token, registration));
                        status = 200;
                    }
                }
                if (status != 200) {
                    record(Kind.FAILED, process, role, request, selected,
                            OptionalLong.empty(), "HTTP " + status, registration, instance);
                }
            }
            exchange.getResponseHeaders().set("X-Flink-Stability-Request-Id", Long.toString(request));
            exchange.getResponseHeaders().set("X-Flink-Stability-Revision", Long.toString(selected.revision()));
            reply(exchange, status, token);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            synchronized (this) {
                record(Kind.FAILED, process, role, request, selected,
                        OptionalLong.empty(), "acquisition interrupted", registration, instance);
            }
        } catch (IOException disconnected) {
            synchronized (this) {
                record(Kind.FAILED, process, role, request, selected,
                        OptionalLong.empty(), "HTTP response delivery failed", registration, instance);
            }
            throw disconnected;
        } finally {
            synchronized (this) {
                activeRequests--;
                record(Kind.REQUEST_FINISHED, process, role, request, selected,
                        OptionalLong.empty(), "acquisition exited", registration, instance);
            }
        }
    }

    private static void reply(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
        }
    }

    /** Called under this object's monitor; sequence and insertion order remain identical. */
    private void record(Kind kind, String process, String role, long request, Settings selected,
                        OptionalLong token, String detail) {
        record(kind, process, role, request, selected, token, detail, null, null);
    }

    private void record(Kind kind, String process, String role, long request, Settings selected,
                        OptionalLong token, String detail, RegistrationSnapshot registration,
                        String participantInstance) {
        if (events.size() == eventLimit) {
            overflow = true;
            return;
        }
        events.add(new Event(++eventSequence, kind, process, role, System.currentTimeMillis(),
                System.nanoTime(), request, selected.revision(), selected.mode(), token, detail,
                Optional.ofNullable(registration), Optional.ofNullable(participantInstance)));
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            notifyAll();
        }
        server.stop(0);
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                synchronized (this) {
                    saturated = true; // Incomplete shutdown cannot produce complete evidence.
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            synchronized (this) {
                saturated = true;
            }
        }
    }

    private static boolean canonicalUuid(String value) {
        return value != null && value.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    private static long number(String value) {
        if (!value.matches("0|[1-9][0-9]{0,18}")) throw new IllegalArgumentException("Invalid number");
        return Long.parseLong(value);
    }

    private static String encodedAlias(String jobId, String alias) {
        return jobId.equals("-") ? "-" : Base64.getUrlEncoder().withoutPadding()
                .encodeToString(alias.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodedAlias(String jobId, String encoded) throws CharacterCodingException {
        if (jobId.equals("-")) {
            if (!encoded.equals("-")) throw new IllegalArgumentException("Invalid bootstrap alias");
            return "-";
        }
        if (!jobId.matches("[0-9a-f]{32}")) throw new IllegalArgumentException("Invalid job ID");
        byte[] bytes = Base64.getUrlDecoder().decode(encoded);
        if (bytes.length > 128) throw new IllegalArgumentException("Alias exceeds its bound");
        String alias = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        if (alias.isBlank() || !encodedAlias(jobId, alias).equals(encoded)) {
            throw new IllegalArgumentException("Invalid alias encoding");
        }
        return alias;
    }

    private static RegistrationSnapshot parseRequest(byte[] body) throws CharacterCodingException {
        String[] fields = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(body))
                .toString().split("\n", -1);
        if (fields.length < 10 || !fields[0].equals("flink-stability-request-v2")
                || !canonicalUuid(fields[1]) || !List.of("BOOTSTRAP", "JOB").contains(fields[2])
                || !(fields[6].equals("true") || fields[6].equals("false"))) {
            throw new IllegalArgumentException("Invalid request envelope");
        }
        long generation = number(fields[3]);
        String alias = decodedAlias(fields[4], fields[5]);
        if (fields[2].equals("BOOTSTRAP") ? !fields[4].equals("-")
                : fields[4].equals("-") || generation < 1) {
            throw new IllegalArgumentException("Invalid request scope");
        }
        long count = number(fields[8]);
        if (count > 32 || fields.length != 10 + count || !fields[fields.length - 1].isEmpty()) {
            throw new IllegalArgumentException("Invalid journal bound");
        }
        List<Lifecycle> journal = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            String[] item = fields[9 + index].split("\t", -1);
            if (item.length != 5) throw new IllegalArgumentException("Invalid journal record");
            String eventAlias = decodedAlias(item[3], item[4]);
            long eventGeneration = number(item[2]);
            if (!item[3].equals("-") && eventGeneration < 1) {
                throw new IllegalArgumentException("Invalid journal generation");
            }
            journal.add(new Lifecycle(number(item[0]), item[1], eventGeneration, item[3], eventAlias));
        }
        return new RegistrationSnapshot(fields[1], fields[2], generation, fields[4], alias,
                Boolean.parseBoolean(fields[6]), number(fields[7]), journal);
    }

    /** Validate the complete request before committing any new journal records. Called under this. */
    private boolean ingest(String process, String role, RegistrationSnapshot registration) {
        Ledger ledger = providers.get(registration.providerInstance());
        if (ledger == null) {
            if (providers.size() == MAX_PROVIDERS) {
                overflow = true;
                return false;
            }
            ledger = new Ledger(process, role);
        }
        if (!ledger.process.equals(process) || !ledger.role.equals(role)
                || registration.acknowledgedSequence() > ledger.through) return false;
        long through = ledger.through;
        int added = 0;
        for (Lifecycle item : registration.journal()) {
            if (item.sequence() <= ledger.through) {
                if (!item.equals(ledger.records.get(item.sequence()))) return false;
            } else {
                if (through == Long.MAX_VALUE || item.sequence() != ++through) return false;
                added++;
            }
        }
        if (journalRecords + added > MAX_EVENTS) {
            overflow = true;
            return false;
        }
        for (Lifecycle item : registration.journal()) ledger.records.putIfAbsent(item.sequence(), item);
        ledger.through = through;
        journalRecords += added;
        providers.putIfAbsent(registration.providerInstance(), ledger);
        // Response ACK uses registration.sentThrough(), never this possibly newer ledger.through.
        return true;
    }

    private static final class Ledger {
        private final String process;
        private final String role;
        private final Map<Long, Lifecycle> records = new HashMap<>();
        private long through;

        private Ledger(String process, String role) {
            this.process = process;
            this.role = role;
        }
    }

    private record Issued(byte[] bytes, RegistrationSnapshot registration) {}

    private record Settings(long revision, Mode mode, Duration delay) {}
}
