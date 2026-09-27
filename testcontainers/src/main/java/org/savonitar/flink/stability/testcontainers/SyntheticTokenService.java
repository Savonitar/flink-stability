package org.savonitar.flink.stability.testcontainers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.savonitar.flink.stability.runtime.api.TokenServiceControl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Loopback-only token fixture. Docker reaches it through Testcontainers' explicit host tunnel. */
final class SyntheticTokenService implements TokenServiceControl, AutoCloseable {
    private static final int MAX_BODY = 1024;
    private static final int MAX_EVENTS = 10_000;
    private final HttpServer server;
    private final ThreadPoolExecutor executor;
    private final long lifetimeMillis;
    private final int eventLimit;
    private final String serviceId = UUID.randomUUID().toString();
    private final List<Event> events = new ArrayList<>();
    private final Map<Long, byte[]> issued = new HashMap<>();
    private Settings settings = new Settings(0, Mode.HEALTHY, Duration.ZERO);
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
        settings = new Settings(settings.revision() + 1, mode, delay);
        record(Kind.MODE_CHANGED, "service", "service", 0, settings,
                OptionalLong.empty(), "delayMillis=" + delay.toMillis());
        notifyAll(); // Healing also releases requests currently held by the delay fault.
        return settings.revision();
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
            String path = exchange.getRequestURI().getPath();
            if (!"POST".equals(exchange.getRequestMethod())
                    || exchange.getRequestURI().getRawQuery() != null
                    || process == null || !process.matches("(?:jobmanager|taskmanager)-[1-9][0-9]*#[1-9][0-9]*")
                    || role == null || !List.of("jobmanager", "taskmanager").contains(role)
                    || !process.startsWith(role + "-")
                    || clientRequest == null || !clientRequest.matches("[1-9][0-9]{0,18}")) {
                reply(exchange, 400, new byte[0]);
                return;
            }
            byte[] body = exchange.getRequestBody().readNBytes(MAX_BODY + 1);
            if (body.length > MAX_BODY) {
                reply(exchange, 413, new byte[0]);
                return;
            }
            if (path.equals("/token")) {
                if (body.length != 0) {
                    reply(exchange, 400, new byte[0]);
                } else {
                    acquire(exchange, process, role);
                }
                return;
            }
            int status;
            synchronized (this) {
                if (closed || overflow || saturated) {
                    status = 503;
                } else if (path.equals("/fault-observed")) {
                    status = acknowledgeFault(process, role, body);
                } else if (path.equals("/receipt")) {
                    Long token = issued.entrySet().stream()
                            .filter(item -> Arrays.equals(body, item.getValue()))
                            .map(Map.Entry::getKey).findFirst().orElse(null);
                    if (token == null) {
                        record(Kind.REJECTED, process, role, 0, settings,
                                OptionalLong.empty(), "Unrecognized synthetic token receipt");
                        status = 400;
                    } else {
                        record(Kind.RECEIVED, process, role, 0, settings,
                                OptionalLong.of(token), "receiver acknowledged issued token");
                        status = overflow ? 503 : 200;
                    }
                } else if ((path.equals("/init-provider") || path.equals("/init-receiver"))
                        && body.length == 0) {
                    record(path.equals("/init-provider") ? Kind.PROVIDER_INITIALIZED : Kind.RECEIVER_INITIALIZED,
                            process, role, 0, settings, OptionalLong.empty(), "SPI initialized");
                    status = overflow ? 503 : 200;
                } else {
                    status = 404;
                }
            }
            reply(exchange, status, new byte[0]);
        }
    }

    /** Called under the service monitor; acknowledgements refer to an actual failed request. */
    private int acknowledgeFault(String process, String role, byte[] body) {
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
                && event.process().equals(process) && event.role().equals(role)).findFirst();
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
                OptionalLong.empty(), "HTTP " + status);
        return overflow ? 503 : 200;
    }

    private void acquire(HttpExchange exchange, String process, String role) throws IOException {
        Settings selected;
        long request;
        synchronized (this) {
            if (closed || overflow || saturated) {
                reply(exchange, 503, new byte[0]);
                return;
            }
            selected = settings;
            request = ++requestSequence;
            activeRequests++;
            maxConcurrentRequests = Math.max(maxConcurrentRequests, activeRequests);
            record(Kind.REQUEST_STARTED, process, role, request, selected,
                    OptionalLong.empty(), "acquisition entered");
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
                    token = ("flink-stability-token-v1\n" + serviceId + "\n" + sequence
                            + "\n" + now + "\n" + (now + lifetimeMillis) + "\n")
                            .getBytes(StandardCharsets.UTF_8);
                    record(Kind.ISSUED, process, role, request, selected,
                            OptionalLong.of(sequence), "synthetic token issued");
                    if (overflow) {
                        token = new byte[0];
                        status = 503;
                    } else {
                        issued.put(sequence, token);
                        status = 200;
                    }
                }
                if (status != 200) {
                    record(Kind.FAILED, process, role, request, selected,
                            OptionalLong.empty(), "HTTP " + status);
                }
            }
            exchange.getResponseHeaders().set("X-Flink-Stability-Request-Id", Long.toString(request));
            exchange.getResponseHeaders().set("X-Flink-Stability-Revision", Long.toString(selected.revision()));
            reply(exchange, status, token);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            synchronized (this) {
                record(Kind.FAILED, process, role, request, selected,
                        OptionalLong.empty(), "acquisition interrupted");
            }
        } catch (IOException disconnected) {
            synchronized (this) {
                record(Kind.FAILED, process, role, request, selected,
                        OptionalLong.empty(), "HTTP response delivery failed");
            }
            throw disconnected;
        } finally {
            synchronized (this) {
                activeRequests--;
                record(Kind.REQUEST_FINISHED, process, role, request, selected,
                        OptionalLong.empty(), "acquisition exited");
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
        if (events.size() == eventLimit) {
            overflow = true;
            return;
        }
        events.add(new Event(++eventSequence, kind, process, role, System.currentTimeMillis(),
                System.nanoTime(), request, selected.revision(), selected.mode(), token, detail));
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

    private record Settings(long revision, Mode mode, Duration delay) {}
}
