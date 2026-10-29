package org.savonitar.flink.stability.testcontainers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.*;
import org.savonitar.flink.stability.runtime.api.*;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.MountableFile;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** One attempt-owned backing volume and independently controlled mount per Flink incarnation. */
final class FuseCheckpointStorageRuntime implements StorageFaultControl, AutoCloseable {
    static final String VIEW = "/flink/checkpoints";
    static final String RAW = "/flink/checkpoint-backing";
    static final String EVIDENCE = "/flink/evidence";
    static final String HELPER = "/opt/flink/checkpoint-storage";
    static final String CONTROL = "/run/flink-checkpoint-storage";
    static final String JOURNAL = CONTROL + "/journal.jsonl";
    static final String SOCKET = CONTROL + "/control.sock";
    static final String CACHE_POLICY = "direct_io=1,writeback_cache=0,entry_timeout=0,attr_timeout=0,negative_timeout=0,kernel_cache=0,auto_cache=0";
    static final int MAX_JOURNAL_BYTES = 16 * 1024 * 1024;
    static final int MAX_JOURNAL_RECORDS = 25_000;
    private static final ObjectMapper JSON = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private static final ThreadLocal<AtomicBoolean> DRIVER_ACTIVE = new ThreadLocal<>();
    private final FuseCheckpointStorage target;
    private final DockerClient docker;
    private final ContainerFileReader fileReader;
    private final Path captureRoot;
    private long retainedBytes;
    private final String attemptId = "attempt-" + UUID.randomUUID();
    private final String storageId = "storage-" + UUID.randomUUID();
    private final String namespace = "job_" + UUID.randomUUID().toString().replace("-", "");
    private final String volume = "flink-checkpoint-" + UUID.randomUUID();
    private final List<Mount> mounts = new ArrayList<>();
    private final List<FileEvidence> files = new ArrayList<>();
    // Serializes evidence mutations, but acquiring it is charged to the caller's deadline.
    // Driver workers never own this lock or mutate the evidence they eventually return.
    private final ReentrantLock operationLock = new ReentrantLock();
    private final AtomicReference<String> operationFailure = new AtomicReference<>();
    private volatile Snapshot lastSnapshot;
    private final Object volumeDriverLock = new Object();
    private volatile boolean volumeCreated;
    private volatile boolean closed;

    FuseCheckpointStorageRuntime(FuseCheckpointStorage target, Path evidenceRoot) {
        this(target, evidenceRoot, FuseCheckpointStorageRuntime::localDocker);
    }

    /** Driver injection keeps deadline/lifecycle tests independent of a reachable Docker daemon. */
    FuseCheckpointStorageRuntime(FuseCheckpointStorage target, Path evidenceRoot, Supplier<DockerClient> docker) {
        this(target, evidenceRoot, docker, (container, path, maximum) ->
                container.copyFileFromContainer(path, input -> input.readNBytes(maximum + 1)));
    }

    FuseCheckpointStorageRuntime(FuseCheckpointStorage target, Path evidenceRoot, Supplier<DockerClient> docker,
                                 ContainerFileReader fileReader) {
        this.target = Objects.requireNonNull(target);
        this.fileReader = Objects.requireNonNull(fileReader);
        verifyBundle(target);
        this.captureRoot = evidenceRoot.resolve("checkpoint-storage-journals");
        try { Files.createDirectory(captureRoot); Files.setPosixFilePermissions(captureRoot, java.nio.file.attribute.PosixFilePermissions.fromString("rwx------")); }
        catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
        this.docker = Objects.requireNonNull(docker.get());
        lastSnapshot = snapshot();
    }

    @FunctionalInterface interface ContainerFileReader {
        byte[] read(VerifiedFlinkContainer container, String path, int maximum);
    }

    private static DockerClient localDocker() {
        var factory = DockerClientFactory.instance();
        requireLocalEndpoint(factory.getTransportConfig().getDockerHost());
        return factory.client();
    }

    static void requireLocalEndpoint(java.net.URI endpoint) {
        if (endpoint == null || !"unix".equals(endpoint.getScheme()) || endpoint.getHost() != null
                || endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null
                || endpoint.getPath() == null || !endpoint.getPath().startsWith("/"))
            throw new IllegalArgumentException("Checkpoint helpers require a local Unix Docker endpoint");
    }

    static void verifyBundle(FuseCheckpointStorage target) {
        try {
            Path manifest = target.bundleRoot().resolve("build-evidence.json");
            verifyFile(manifest, target.manifestSha256(), 4 * 1024 * 1024);
            for (var file : target.files().entrySet())
                verifyFile(target.bundleRoot().resolve("bundle").resolve(file.getKey()), file.getValue(), 32 * 1024 * 1024);
        } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
    }

    private static void verifyFile(Path path, String expected, int maximum) throws java.io.IOException {
        Path cursor = path.toAbsolutePath();
        while (cursor != null) {
            if (Files.isSymbolicLink(cursor)) throw new IllegalArgumentException("Helper path contains a symbolic link");
            cursor = cursor.getParent();
        }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > maximum)
            throw new IllegalArgumentException("Helper file is absent, nonregular or exceeds its bound");
        try (var in = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            if (!expected.equals(Digests.sha256(in))) throw new IllegalArgumentException("Prepared helper digest differs: " + path.getFileName());
        }
    }

    Mount configure(VerifiedFlinkContainer container, String logicalName, FlinkComponentRole role) {
        var deadline = deadline(Duration.ofSeconds(30));
        return operation(deadline, "configure storage incarnation", () -> configure(container, logicalName, role, deadline));
    }

    private Mount configure(VerifiedFlinkContainer container, String logicalName, FlinkComponentRole role,
                            ContainerOperationDeadline deadline) {
        if (!volumeCreated) {
            volumeCreated = true;
            driver(deadline, "create checkpoint backing volume", () -> {
                synchronized (volumeDriverLock) {
                    checkActive(deadline);
                    return docker.createVolumeCmd().withName(volume)
                            .withLabels(Map.of("org.savonitar.flink-stability.storage", storageId)).exec();
                }
            });
        }
        Mount mount = new Mount(container, logicalName, role);
        mounts.add(mount);
        container.configureStorage(this, mount);
        for (String name : FuseCheckpointStorage.REQUIRED_FILES) {
            container.withCopyFileToContainer(MountableFile.forHostPath(target.bundleRoot().resolve("bundle").resolve(name),
                    name.equals("probe.jar") ? 0444 : 0555), HELPER + "/" + name);
        }
        container.withCreateContainerCmdModifier(command -> {
            command.withUser("0:0").withEntrypoint("/bin/sh", "-c");
            var host = command.getHostConfig();
            List<Bind> binds = new ArrayList<>(host.getBinds() == null ? List.of() : Arrays.asList(host.getBinds()));
            binds.add(new Bind(volume, new Volume(RAW), AccessMode.rw));
            host.withBinds(binds).withCapDrop(Capability.ALL).withCapAdd(Capability.SYS_ADMIN)
                    .withDevices(new Device("rwm", "/dev/fuse", "/dev/fuse"))
                    .withPrivileged(false).withSecurityOpts(List.of("no-new-privileges:true"));
            command.withCmd(supervisor(mount.logPath()));
        });
        return mount;
    }

    /** The daemon and independent capture parser use the same fixed attempt bounds. */
    static String journalLimitArguments() {
        return "--max-journal-bytes " + MAX_JOURNAL_BYTES + " --max-journal-records " + MAX_JOURNAL_RECORDS;
    }

    static String supervisor(String log) {
        if (!log.matches("/flink/evidence/[A-Za-z0-9_.-]+")) throw new IllegalArgumentException("Invalid log path");
        return "exec tail --retry -n +1 -F '" + log + "'";
    }

    void verifyCreated(Mount mount) {
        var deadline = deadline(Duration.ofSeconds(30));
        operation(deadline, "verify created storage incarnation", () -> { verifyCreated(mount, deadline); return null; });
    }

    private void verifyCreated(Mount mount, ContainerOperationDeadline deadline) {
        for (var expected : target.files().entrySet()) {
            byte[] bytes = driver(deadline, "verify container helper " + expected.getKey(), () ->
                    fileReader.read(mount.container, HELPER + "/" + expected.getKey(), 32 * 1024 * 1024));
            if (bytes.length > 32 * 1024 * 1024) throw new IllegalStateException("Container helper exceeds byte limit");
            String actual = Digests.sha256(bytes);
            if (!expected.getValue().equals(actual)) throw new IllegalStateException("Container helper bytes differ");
        }
        var inspected = driver(deadline, "inspect created storage incarnation", () ->
                docker.inspectContainerCmd(mount.container.getContainerId()).exec());
        ImageIdentityEvidence identity = mount.container.imageIdentityEvidence(mount.container.getContainerId());
        if (!identity.declaredImageId().equals(Optional.of(target.imageId()))
                || !identity.containerImageId().equals(inspected.getImageId()))
            throw new IllegalStateException("FUSE container image identity differs from its pre-start receipt");
        var host = inspected.getHostConfig();
        if (!"0:0".equals(inspected.getConfig().getUser())
                || Boolean.TRUE.equals(host.getPrivileged())
                || !Set.of(Capability.ALL).equals(new HashSet<>(Arrays.asList(host.getCapDrop())))
                || !Set.of(Capability.SYS_ADMIN).equals(new HashSet<>(Arrays.asList(host.getCapAdd())))
                || host.getDevices() == null || host.getDevices().length != 1
                || !"/dev/fuse".equals(host.getDevices()[0].getPathOnHost())
                || !"/dev/fuse".equals(host.getDevices()[0].getPathInContainer()))
            throw new IllegalStateException("FUSE container image or privilege contract differs");
        if (inspected.getMounts() == null || inspected.getMounts().stream().filter(m ->
                m.getDestination() != null && RAW.equals(m.getDestination().getPath()) && volume.equals(m.getName())
                        && Boolean.TRUE.equals(m.getRW())).count() != 1
                || inspected.getMounts().stream().anyMatch(m -> m.getDestination() != null && VIEW.equals(m.getDestination().getPath())))
            throw new IllegalStateException("Owned backing volume or isolated FUSE mount layout differs");
        if (host.getSecurityOpts() == null || !host.getSecurityOpts().contains("no-new-privileges:true"))
            throw new IllegalStateException("FUSE container no-new-privileges is unconfirmed");
        if (!Arrays.equals(inspected.getConfig().getEntrypoint(), new String[]{"/bin/sh", "-c"})
                || !Arrays.equals(inspected.getConfig().getCmd(), new String[]{supervisor(mount.logPath())}))
            throw new IllegalStateException("FUSE supervisor differs from prepared launch contract");
    }

    void start(Mount mount) {
        var deadline = deadline(Duration.ofSeconds(30));
        operation(deadline, "start storage incarnation", () -> { start(mount, deadline); return null; });
    }

    private void start(Mount mount, ContainerOperationDeadline deadline) {
        requireSuccess(exec(mount, "0:0", deadline, "/bin/sh", "-eu", "-c",
                "mkdir -p " + RAW + " " + VIEW + " " + CONTROL + "; chmod 700 " + RAW + " " + CONTROL
                        + "; chmod 755 " + VIEW + "; printf sentinel > " + RAW + "/raw-sentinel"));
        String daemon = HELPER + "/checkpoint-fuse --backing " + RAW + " --mount " + VIEW
                + " --control " + SOCKET + " --journal " + JOURNAL + " --attempt " + attemptId
                + " --storage " + storageId + " --instance " + mount.instanceId + " --namespace " + namespace
                + " --uid 9999 --gid 9999 " + journalLimitArguments();
        requireSuccess(exec(mount, "0:0", deadline, "/bin/sh", "-eu", "-c", daemon + " > " + CONTROL + "/daemon.log 2>&1 < /dev/null &"));
        while (true) {
            Exec result = exec(mount, "0:0", deadline, HELPER + "/checkpoint-fuse", "--ctl", SOCKET,
                    "STATUS " + identity(mount) + " 0");
            if (result.exitCode == 0) {
                JsonNode receipt = object(result.stdout);
                validateIdentity(receipt, mount);
                mount.instance = new Instance(mount.logicalName, mount.container.classLoadProcess(),
                        mount.container.getContainerId(), mount.instanceId, mount.role, positive(receipt, "daemonPid"));
                mount.status = status(receipt);
                if (mount.status.state() != State.HEALTHY || !mount.status.journalComplete())
                    throw new IllegalStateException("FUSE initial state is not healthy");
                break;
            }
            pause(deadline);
        }
        verifyMountAndAccess(mount, deadline);
        String role = mount.role == FlinkComponentRole.JOB_MANAGER ? "jobmanager" : "taskmanager";
        requireSuccess(exec(mount, "9999:9999", deadline, "/bin/sh", "-eu", "-c",
                "/bin/bash " + FlinkProcessConfiguration.LAUNCHER_PATH + " " + role + " > " + mount.logPath() + " 2>&1 < /dev/null &"));
    }

    static String verifiedMountLine(String mountInfo) {
        var matching = mountInfo.lines().filter(line -> {
            String[] halves = line.split(" - ", -1);
            if (halves.length != 2) return false;
            String[] before = halves[0].split(" "), after = halves[1].split(" ");
            return before.length >= 6 && before[4].equals(VIEW) && after.length == 3
                    && after[0].equals("fuse") && after[1].equals("/dev/fuse")
                    && Set.of(after[2].split(",")).containsAll(Set.of("default_permissions", "allow_other", "user_id=0", "group_id=0"));
        }).toList();
        if (matching.size() != 1) throw new IllegalStateException("One exact daemon-owned FUSE mount was not observed");
        return matching.getFirst();
    }

    private void verifyMountAndAccess(Mount mount, ContainerOperationDeadline deadline) {
        Exec info = exec(mount, "0:0", deadline, "/bin/cat", "/proc/self/mountinfo");
        requireSuccess(info);
        String mountLine = verifiedMountLine(info.stdout);
        String input = "printf '1\\thello\\n2\\traw-read\\traw-sentinel\\n3\\traw-write\\tforbidden\\teA==\\n4\\tquit\\n' | "
                + "java -jar " + HELPER + "/probe.jar --root " + VIEW + "/" + namespace
                + " --raw-root " + RAW + " --evidence-root " + EVIDENCE + " --timeout-seconds 10";
        Exec result = exec(mount, "9999:9999", deadline, "/bin/sh", "-eu", "-c", input);
        requireSuccess(result);
        List<JsonNode> receipts = result.stdout.lines().map(FuseCheckpointStorageRuntime::object).toList();
        if (receipts.size() != 4 || !"ok".equals(text(receipts.get(0), "status")))
            throw new IllegalStateException("FUSE permission probe is incomplete");
        JsonNode hello = receipts.get(0);
        List<Long> uid = numbers(hello, "uid"), gid = numbers(hello, "gid");
        String capabilities = text(hello, "capEff");
        boolean readDenied = denied(receipts.get(1), "raw-read"), writeDenied = denied(receipts.get(2), "raw-write");
        if (!uid.equals(List.of(9999L, 9999L, 9999L, 9999L)) || !gid.equals(uid)
                || !capabilities.matches("0+") || !readDenied || !writeDenied)
            throw new IllegalStateException("Unprivileged JVM/raw-backing isolation is not confirmed");
        ImageIdentityEvidence identity = mount.container.imageIdentityEvidence(mount.container.getContainerId());
        mount.verification = new Verification(identity.containerImageId(), target.files().get("checkpoint-fuse"),
                target.files().get("probe.jar"), mountLine, uid, gid, capabilities, 0, "", readDenied, writeDenied, false, List.of())
                .withImageIdentityEvidence(identity);
        capture(mount, false, deadline);
        if (mount.operations.stream().noneMatch(op -> op.operation().equals("READY") && CACHE_POLICY.equals(op.reason())))
            throw new IllegalStateException("FUSE effective cache policy is unconfirmed");
    }

    void verifyFlinkProcess(Mount mount) {
        var deadline = deadline(Duration.ofSeconds(10));
        operation(deadline, "verify Flink storage process", () -> { verifyFlinkProcess(mount, deadline); return null; });
    }

    private void verifyFlinkProcess(Mount mount, ContainerOperationDeadline deadline) {
        String main = mount.role == FlinkComponentRole.JOB_MANAGER ? "StandaloneSessionClusterEntrypoint" : "TaskManagerRunner";
        Exec result = exec(mount, "0:0", deadline, "/bin/sh", "-eu", "-c",
                "for p in /proc/[0-9]*; do [ -r \"$p/comm\" ] || continue; [ \"$(cat \"$p/comm\")\" = java ] || continue; "
                + "cmd=$(tr '\\000' ' ' < \"$p/cmdline\"); case \"$cmd\" in *" + main + "*) printf '%s\\n%s\\n' \"${p##*/}\" \"$cmd\"; "
                + "cat \"$p/status\";; esac; done");
        requireSuccess(result);
        List<String> lines = result.stdout.lines().toList();
        if (lines.size() < 3 || lines.stream().filter(line -> line.startsWith("Name:")).count() != 1)
            throw new IllegalStateException("One actual Flink JVM was not observed");
        long pid = Long.parseLong(lines.get(0));
        List<Long> uid = linuxIds(lines, "Uid:"), gid = linuxIds(lines, "Gid:");
        String cap = lines.stream().filter(line -> line.startsWith("CapEff:")).findFirst().orElseThrow().substring(7).strip();
        if (!uid.equals(List.of(9999L, 9999L, 9999L, 9999L)) || !gid.equals(uid) || !cap.matches("0+"))
            throw new IllegalStateException("Actual Flink JVM has unexpected UID/GID/capabilities");
        Verification previous = mount.verification;
        mount.verification = new Verification(previous.imageId(), previous.helperSha256(), previous.probeSha256(),
                previous.mountInfo(), uid, gid, cap, pid, lines.get(1), previous.rawReadDenied(), previous.rawWriteDenied(), true, List.of(),
                previous.imageIdentityEvidence());
    }

    @Override public Snapshot storageSnapshot(Duration timeout) {
        ContainerOperationDeadline deadline = deadline(timeout);
        try {
            return operation(deadline, "capture storage evidence", () -> {
                captureAll(deadline);
                return snapshot();
            });
        } catch (RuntimeException failure) {
            // A busy/abandoned worker cannot prevent the caller retaining known evidence.
            // Its result is explicitly incomplete and cannot certify a physical write fence.
            Snapshot previous = lastSnapshot;
            List<String> diagnostics = new ArrayList<>(previous.diagnostics());
            diagnostics.add("Storage capture unavailable: " + failure.getMessage());
            return new Snapshot(attemptId, storageId, namespace, VIEW_URI(), target.manifestSha256(),
                    previous.instances(), previous.materializedFiles(), false, false, diagnostics);
        }
    }

    private void captureAll(ContainerOperationDeadline deadline) {
        for (Mount mount : mounts) {
            if (mount.instance == null) continue;
            if (!mount.dead) {
                try { command(mount, "STATUS", mount.status.revision(), null, 0, deadline); capture(mount, false, deadline); }
                catch (RuntimeException failure) { mount.diagnostics.add("Storage capture failed: " + failure.getMessage()); }
            }
        }
    }

    /** Only the operation owner reads mutable mount evidence; publication itself does no I/O. */
    private Snapshot snapshot() {
        List<String> diagnostics = new ArrayList<>();
        if (closed) diagnostics.add("Storage lifecycle is closed");
        if (operationFailure.get() != null) diagnostics.add(operationFailure.get());
        for (Mount mount : mounts) if (mount.instance == null) diagnostics.add("Unverified storage incarnation: " + mount.logicalName);
        List<InstanceEvidence> evidence = mounts.stream().filter(m -> m.instance != null).map(Mount::evidence).toList();
        boolean complete = diagnostics.isEmpty() && !evidence.isEmpty() && evidence.stream().allMatch(e ->
                e.verification() != null && e.verification().complete() && e.journalComplete() && e.diagnostics().isEmpty());
        return new Snapshot(attemptId, storageId, namespace, VIEW_URI(), target.manifestSha256(), evidence,
                files, complete, complete && evidence.stream().allMatch(InstanceEvidence::processDead), diagnostics);
    }

    @Override public Transition armStorage(String faultId, long revision, Duration lease, Duration timeout) {
        if (lease == null || lease.isZero() || lease.isNegative() || lease.toMillis() < 1 || lease.toMillis() > 120000)
            throw new IllegalArgumentException("Storage lease must be in 1..120000 milliseconds");
        return transition("ARM", faultId, revision, lease.toMillis(), timeout);
    }
    @Override public Transition healStorage(String faultId, long revision, Duration timeout) {
        return transition("HEAL", faultId, revision, 0, timeout);
    }
    @Override public Transition abortStorage(String faultId, long revision, Duration timeout) {
        return transition("ABORT", faultId, revision, 0, timeout);
    }

    private Transition transition(String operation, String fault, long revision, long lease, Duration timeout) {
        if (fault == null || !fault.matches("[A-Za-z0-9_-]{1,64}") || revision < 1)
            throw new IllegalArgumentException("Invalid storage fault identity/revision");
        long start = System.nanoTime();
        ContainerOperationDeadline deadline = deadline(timeout);
        return operation(deadline, operation + " storage", () -> transition(operation, fault, revision, lease, start, deadline));
    }

    private Transition transition(String operation, String fault, long revision, long lease, long start,
                                  ContainerOperationDeadline deadline) {
        List<Command> receipts = new ArrayList<>(); List<Instance> dead = new ArrayList<>(); List<String> diagnostics = new ArrayList<>();
        for (Mount mount : mounts) {
            if (mount.instance == null) { diagnostics.add("Unverified mount cannot acknowledge transition"); continue; }
            if (mount.dead) { dead.add(mount.instance); if (operation.equals("ARM")) diagnostics.add("Cannot arm a dead mount"); continue; }
            try {
                Command receipt = command(mount, operation, revision, fault, lease, deadline);
                receipts.add(receipt);
                if (!receipt.confirmed()) diagnostics.add("Storage command rejected by " + mount.instance.instanceId());
            } catch (RuntimeException failure) { diagnostics.add("Storage command failed: " + failure.getMessage()); }
        }
        if (receipts.isEmpty()) diagnostics.add("No live storage acknowledgements");
        return new Transition(fault, revision, start, System.nanoTime(), receipts, dead, diagnostics.isEmpty(), diagnostics);
    }

    private Command command(Mount mount, String operation, long revision, String fault, long lease, ContainerOperationDeadline deadline) {
        if (mount.commands.size() >= 1024) throw new IllegalStateException("Storage control evidence command budget exceeded");
        long start = System.nanoTime();
        String value = operation + " " + identity(mount) + " " + revision;
        if (fault != null) value += " " + fault;
        if (operation.equals("ARM")) value += " " + lease + " create,write";
        Exec result = exec(mount, "0:0", deadline, HELPER + "/checkpoint-fuse", "--ctl", SOCKET, value);
        JsonNode node = object(result.stdout);
        validateIdentity(node, mount);
        Status observed = status(node);
        if (mount.status != null && !countersAdvance(mount.status.counters(), observed.counters()))
            throw new IllegalStateException("Storage status IO counters moved backwards or disappeared");
        boolean accepted = result.exitCode == 0 && bool(node, "ok") && observed.revision() == revision
                && observed.journalComplete() && (fault == null || fault.equals(observed.faultId()));
        if (operation.equals("ARM")) accepted &= observed.state() == State.ARMED && observed.leaseMs() == lease
                && node.path("allowedOperations").isArray() && node.path("allowedOperations").size() == 2
                && "create".equals(node.path("allowedOperations").get(0).asText())
                && "write".equals(node.path("allowedOperations").get(1).asText());
        if (operation.equals("HEAL")) accepted &= observed.state() == State.HEALTHY;
        if (operation.equals("ABORT")) accepted &= observed.state() == State.ABORTED;
        Command receipt = new Command(mount.instance, operation, start, System.nanoTime(), result.exitCode,
                observed, result.stdout, result.stderr, accepted);
        mount.commands.add(receipt); mount.status = observed;
        return receipt;
    }

    void processDead(Mount mount, FlinkHaControl.ProcessState proof, ContainerOperationDeadline deadline) {
        operation(deadline, "capture stopped storage process", () -> {
            if (mount.instance == null || proof.running() || !mount.instance.runtimeId().equals(proof.runtimeId()))
                throw new IllegalStateException("Storage process death lacks exact physical identity");
            mount.dead = true;
            capture(mount, true, deadline);
            return null;
        });
    }

    private void capture(Mount mount, boolean quiescent, ContainerOperationDeadline deadline) {
        byte[] raw = driver(deadline, "capture FUSE journal", () ->
                fileReader.read(mount.container, JOURNAL, MAX_JOURNAL_BYTES));
        try {
            String digest = Digests.sha256(raw);
            Path retained = captureRoot.resolve(mount.instanceId + "-" + digest + ".jsonl");
            if (!Files.exists(retained, LinkOption.NOFOLLOW_LINKS)) {
                if (retainedBytes + raw.length > 128L * 1024 * 1024) throw new IllegalStateException("Retained storage journal budget exceeded");
                try { Files.write(retained, raw, java.nio.file.StandardOpenOption.CREATE_NEW); }
                catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
                retainedBytes += raw.length;
            }
            mount.journalPath = retained.toString(); mount.journalSha256 = digest; mount.journalBytes = raw.length;
            List<Operation> parsed = parseJournal(raw, mount.instance, attemptId, storageId, namespace, quiescent);
            if (parsed.size() < mount.operations.size() || !parsed.subList(0, mount.operations.size()).equals(mount.operations))
                throw new IllegalStateException("FUSE journal prefix changed or disappeared");
            mount.operations = parsed;
            mount.journalComplete = !parsed.isEmpty() && parsed.stream().noneMatch(op -> op.state() == State.FATAL);
        } catch (RuntimeException failure) { mount.journalComplete = false; mount.diagnostics.add(failure.getMessage()); throw failure; }
    }

    static List<Operation> parseJournal(byte[] raw, Instance instance, String attempt, String storage, String namespace, boolean quiescent) {
        if (raw.length > MAX_JOURNAL_BYTES) throw new IllegalStateException("FUSE journal exceeds byte budget");
        String text = utf8(raw);
        if (quiescent && !text.endsWith("\n")) throw new IllegalStateException("Quiescent FUSE journal has a partial final record");
        int end = text.lastIndexOf('\n');
        if (end < 0) throw new IllegalStateException("FUSE journal has no complete record");
        List<Operation> records = new ArrayList<>();
        Map<String, IoCounter> previousCounters = Map.of();
        for (String line : text.substring(0, end).split("\n", -1)) {
            if (records.size() == MAX_JOURNAL_RECORDS || line.length() > 8192) throw new IllegalStateException("FUSE journal exceeds record budget");
            JsonNode node = object(line);
            if (number(node, "version") != 1 || number(node, "sequence") != records.size() + 1
                    || !attempt.equals(text(node, "attemptId")) || !storage.equals(text(node, "storageId"))
                    || !namespace.equals(text(node, "namespace")) || !instance.instanceId().equals(text(node, "instanceId"))
                    || instance.daemonPid() != number(node, "daemonPid") || !bool(node, "journalComplete"))
                throw new IllegalStateException("FUSE journal identity, sequence or completeness differs");
            JsonNode datasync = node.get("datasync");
            if (datasync == null || !(datasync.isNull() || datasync.isBoolean())) throw new IllegalStateException("Invalid datasync evidence");
            String operation = text(node, "operation");
            Map<String, IoCounter> counters = counters(node);
            if (operation.equals("SUMMARY") && counters.isEmpty()) throw new IllegalStateException("SUMMARY has no IO counters");
            if ((operation.equals("START") || operation.equals("READY")) && counters.values().stream().anyMatch(value -> value.count() != 0))
                throw new IllegalStateException("Startup record has nonzero IO counters");
            if (!previousCounters.isEmpty() && !operation.isEmpty() && Character.isUpperCase(operation.charAt(0)) && counters.isEmpty())
                throw new IllegalStateException("Lifecycle record lost IO counters");
            if (!counters.isEmpty()) {
                if (!countersAdvance(previousCounters, counters)) throw new IllegalStateException("Native IO counters moved backwards");
                if (operation.equals("SUMMARY") && counters.equals(previousCounters))
                    throw new IllegalStateException("Idle SUMMARY has no new IO counters");
                previousCounters = counters;
            }
            boolean pathTruncated = node.has("pathTruncated") && bool(node, "pathTruncated");
            if (pathTruncated && (number(node, "errno") != 36 || number(node, "result") != -1
                    || !text(node, "reason").equals("receipt-path-truncated")))
                throw new IllegalStateException("Invalid truncated-path receipt");
            records.add(new Operation(instance, number(node, "sequence"), number(node, "timestampMs"), number(node, "monotonicNs"),
                    number(node, "revision"), State.valueOf(text(node, "state")), nullableText(node, "faultId"), operation,
                    text(node, "path"), nullableText(node, "faultOperation"), number(node, "result"), Math.toIntExact(number(node, "errno")),
                    number(node, "requestedBytes"), number(node, "returnedBytes"), datasync.isNull() ? null : datasync.booleanValue(), text(node, "reason"),
                    counters, pathTruncated));
        }
        return List.copyOf(records);
    }

    @Override public Materialization inspectCheckpoint(String jobId, long checkpointId, Duration timeout) {
        if (jobId == null || !jobId.matches("[0-9a-f]{32}") || checkpointId < 1) throw new IllegalArgumentException("Invalid checkpoint identity");
        ContainerOperationDeadline deadline = deadline(timeout);
        return operation(deadline, "inspect checkpoint state", () -> inspectCheckpoint(jobId, checkpointId, deadline));
    }

    private Materialization inspectCheckpoint(String jobId, long checkpointId, ContainerOperationDeadline deadline) {
        Mount mount = mounts.stream().filter(m -> !m.dead && m.instance != null && m.role == FlinkComponentRole.JOB_MANAGER).findFirst().orElseThrow();
        Exec receipt = exec(mount, "9999:9999", deadline, "java", "-Xmx128m", "-cp", HELPER + "/probe.jar:/opt/flink/lib/*",
                "org.savonitar.flink.stability.testcontainers.storage.CheckpointStateInspector", "--root", VIEW + "/" + namespace,
                "--job-id", jobId, "--checkpoint-id", Long.toString(checkpointId));
        requireSuccess(receipt);
        JsonNode node = object(receipt.stdout);
        if (number(node, "version") != 1 || !jobId.equals(text(node, "jobId")) || checkpointId != number(node, "checkpointId")
                || !node.path("files").isArray() || node.path("files").isEmpty())
            throw new IllegalStateException("Checkpoint inspector returned incomplete identity/files");
        String prefix = VIEW + "/" + namespace + "/" + jobId + "/chk-" + checkpointId + "/";
        FileEvidence metadata = file(node.path("metadata"), prefix);
        if (!metadata.path().equals(prefix + "_metadata")) throw new IllegalStateException("Unexpected checkpoint metadata path");
        List<FileEvidence> state = new ArrayList<>();
        for (JsonNode entry : node.path("files")) {
            FileEvidence observed = file(entry, prefix);
            if (observed.path().endsWith("/_metadata") || state.stream().anyMatch(item -> item.path().equals(observed.path())))
                throw new IllegalStateException("Duplicate or metadata state handle");
            state.add(observed);
        }
        if (state.size() > 128) throw new IllegalStateException("Checkpoint state file count exceeds bound");
        files.add(metadata); files.addAll(state);
        capture(mount, false, deadline);
        return new Materialization(mount.instance, jobId, checkpointId, text(node, "operatorUid"), metadata, state, true, List.of());
    }

    private static FileEvidence file(JsonNode node, String prefix) {
        String path = text(node, "path");
        if (!path.startsWith(prefix) || !Path.of(path).normalize().toString().equals(path)) throw new IllegalStateException("Inspector path outside checkpoint");
        long size = positive(node, "size"); String digest = Checks.requireSha256(text(node, "sha256"), "checkpoint file");
        return new FileEvidence(path, size, true, digest, System.nanoTime());
    }

    @Override public void close() { close(Duration.ofSeconds(15)); }

    void close(Duration timeout) {
        ContainerOperationDeadline deadline = deadline(timeout);
        // Do not wait for the evidence lock: cancel publication/startup first, then attempt
        // cleanup independently. The dedicated volume lock preserves create/remove ordering
        // even when a timed-out create ignores interruption and acknowledges much later.
        closed = true;
        if (!volumeCreated) return;
        // Docker refuses removal if any container still owns the volume; never force-remove it.
        try { ContainerDriverCallBoundary.run(deadline, "remove checkpoint backing volume",
                () -> {
                    synchronized (volumeDriverLock) {
                        try { docker.removeVolumeCmd(volume).exec(); }
                        catch (com.github.dockerjava.api.exception.NotFoundException absent) { /* Confirmed absent. */ }
                    }
                });
        } catch (RuntimeException failure) {
            fail("Storage cleanup failed", failure);
            throw failure;
        }
    }

    private String VIEW_URI() { return "file://" + VIEW + "/" + namespace; }
    private String identity(Mount mount) { return attemptId + " " + storageId + " " + mount.instanceId; }
    private void validateIdentity(JsonNode node, Mount mount) {
        if (number(node, "version") != 1 || !attemptId.equals(text(node, "attemptId")) || !storageId.equals(text(node, "storageId"))
                || !mount.instanceId.equals(text(node, "instanceId")) || !namespace.equals(text(node, "namespace"))
                || (mount.instance != null && mount.instance.daemonPid() != number(node, "daemonPid")))
            throw new IllegalStateException("Foreign storage control receipt");
    }
    private static Status status(JsonNode node) {
        return new Status(State.valueOf(text(node, "state")), nullableText(node, "faultId"), number(node, "revision"), number(node, "sequence"),
                number(node, "leaseMs"), number(node, "leaseDeadlineMonotonicNs"), bool(node, "journalComplete"), bool(node, "stopping"), counters(node));
    }
    private static Map<String, IoCounter> counters(JsonNode node) {
        JsonNode value = node.get("counters");
        if (value == null) return Map.of();
        if (!value.isObject() || value.size() != 6) throw new IllegalStateException("Invalid native IO counter inventory");
        Map<String, IoCounter> counters = new TreeMap<>();
        for (String key : List.of("read", "getattr", "readdir", "access", "flush", "release")) {
            JsonNode counter = value.get(key);
            if (counter == null || !counter.isObject() || counter.size() != 2)
                throw new IllegalStateException("Missing native IO counter " + key);
            long count = number(counter, "count"), bytes = number(counter, "bytes");
            if (count < 0 || bytes < 0 || (count == 0 && bytes != 0) || (!key.equals("read") && bytes != 0))
                throw new IllegalStateException("Invalid native IO counter " + key);
            counters.put(key, new IoCounter(count, bytes));
        }
        return Map.copyOf(counters);
    }
    private static boolean countersAdvance(Map<String, IoCounter> previous, Map<String, IoCounter> current) {
        if (previous.isEmpty()) return true;
        return previous.keySet().equals(current.keySet()) && previous.entrySet().stream().allMatch(entry ->
                current.get(entry.getKey()).count() >= entry.getValue().count()
                        && current.get(entry.getKey()).bytes() >= entry.getValue().bytes());
    }
    private static boolean denied(JsonNode node, String operation) {
        return operation.equals(text(node, "operation")) && "io-exception".equals(text(node, "status"))
                && "java.nio.file.AccessDeniedException".equals(text(node, "exceptionClass"));
    }
    private static List<Long> linuxIds(List<String> lines, String prefix) {
        return Arrays.stream(lines.stream().filter(line -> line.startsWith(prefix)).findFirst().orElseThrow()
                .substring(prefix.length()).strip().split("\\s+")).map(Long::valueOf).toList();
    }
    private static List<Long> numbers(JsonNode node, String name) {
        if (!node.path(name).isArray()) throw new IllegalStateException("Missing numeric array " + name);
        List<Long> result = new ArrayList<>(); for (JsonNode item : node.path(name)) {
            if (!item.isIntegralNumber() || !item.canConvertToLong()) throw new IllegalStateException("Invalid numeric array"); result.add(item.longValue());
        } return List.copyOf(result);
    }
    private static JsonNode object(String value) {
        try (JsonParser parser = JSON.createParser(value)) {
            JsonNode node = JSON.readTree(parser);
            if (node == null || !node.isObject() || parser.nextToken() != null) throw new IllegalStateException("Invalid JSON object receipt");
            return node;
        } catch (java.io.IOException failure) { throw new IllegalStateException("Invalid JSON receipt", failure); }
    }
    private static String text(JsonNode node, String name) {
        JsonNode value = node.get(name); if (value == null || !value.isTextual()) throw new IllegalStateException("Missing textual field " + name); return value.textValue();
    }
    private static String nullableText(JsonNode node, String name) {
        JsonNode value = node.get(name); if (value == null) throw new IllegalStateException("Missing field " + name); return value.isNull() ? null : text(node, name);
    }
    private static long number(JsonNode node, String name) {
        JsonNode value = node.get(name); if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) throw new IllegalStateException("Invalid numeric field " + name); return value.longValue();
    }
    private static long positive(JsonNode node, String name) { long value = number(node, name); if (value < 1) throw new IllegalStateException("Nonpositive " + name); return value; }
    private static boolean bool(JsonNode node, String name) {
        JsonNode value = node.get(name); if (value == null || !value.isBoolean()) throw new IllegalStateException("Missing boolean field " + name); return value.booleanValue();
    }
    private static String utf8(byte[] bytes) {
        try { return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString(); }
        catch (java.nio.charset.CharacterCodingException failure) { throw new IllegalStateException("Malformed UTF-8 evidence", failure); }
    }

    private <T> T operation(ContainerOperationDeadline deadline, String description, Supplier<T> action) {
        boolean locked = false;
        try {
            checkActive(deadline);
            locked = operationLock.tryLock(deadline.remaining("acquire storage evidence lock").toNanos(), TimeUnit.NANOSECONDS);
            if (!locked) throw deadline.timedOut("acquire storage evidence lock", null);
            checkActive(deadline);
            T result = action.get();
            checkActive(deadline);
            return result;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            var failure = new IllegalStateException("Interrupted while " + description, interrupted);
            fail(description, failure);
            throw failure;
        } catch (RuntimeException failure) {
            fail(description, failure);
            throw failure;
        } finally {
            if (locked) {
                try { lastSnapshot = snapshot(); }
                finally { operationLock.unlock(); }
            }
        }
    }

    private void fail(String operation, RuntimeException failure) {
        operationFailure.compareAndSet(null, operation + " failed: " + failure.getMessage());
    }

    private void checkActive(ContainerOperationDeadline deadline) {
        AtomicBoolean worker = DRIVER_ACTIVE.get();
        if (closed || Thread.currentThread().isInterrupted() || (worker != null && !worker.get()))
            throw new IllegalStateException("Storage operation is closed, interrupted or abandoned");
        deadline.remaining("checkpoint storage operation");
    }

    private <T> T driver(ContainerOperationDeadline deadline, String description, Callable<T> action) {
        AtomicBoolean active = new AtomicBoolean(true);
        try {
            T result = ContainerDriverCallBoundary.call(deadline, description, () -> {
                DRIVER_ACTIVE.set(active);
                try {
                    checkActive(deadline);
                    T value = action.call();
                    checkActive(deadline);
                    return value;
                } finally { DRIVER_ACTIVE.remove(); }
            });
            checkActive(deadline);
            return result;
        } finally {
            // Cancellation is independent of interrupt cooperation. A late worker may only
            // finish its current call; it cannot advance to another exec/start/inspect step.
            active.set(false);
        }
    }

    private static ContainerOperationDeadline deadline(Duration timeout) { return ContainerOperationDeadline.start("checkpoint storage", timeout, System::nanoTime); }
    private static void pause(ContainerOperationDeadline deadline) {
        deadline.remaining("await native mount readiness");
        try { Thread.sleep(50); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IllegalStateException("Interrupted starting mount", failure); }
    }
    private static void requireSuccess(Exec value) { if (value.exitCode != 0) throw new IllegalStateException("Container storage command exited " + value.exitCode + ": " + value.stderr); }

    /** Caps output while it arrives; the same monotonic budget covers create, attach, completion and inspect. */
    private Exec exec(Mount mount, String user, ContainerOperationDeadline deadline, String... args) {
        return driver(deadline, "execute owned storage command", () -> {
            var created = docker.execCreateCmd(mount.container.getContainerId()).withUser(user).withAttachStdout(true).withAttachStderr(true).withCmd(args).exec();
            checkActive(deadline);
            try (Capture callback = new Capture()) {
                docker.execStartCmd(created.getId()).exec(callback);
                checkActive(deadline);
                if (!callback.awaitCompletion(deadline.remaining("await storage exec").toNanos(), TimeUnit.NANOSECONDS))
                    throw deadline.timedOut("await storage exec", null);
                callback.requireComplete();
                checkActive(deadline);
                var inspected = docker.inspectExecCmd(created.getId()).exec();
                if (Boolean.TRUE.equals(inspected.isRunning()) || inspected.getExitCodeLong() == null) throw new IllegalStateException("Storage exec completion is unconfirmed");
                return new Exec(Math.toIntExact(inspected.getExitCodeLong()), utf8(callback.stdout.toByteArray()), utf8(callback.stderr.toByteArray()));
            }
        });
    }
    private record Exec(int exitCode, String stdout, String stderr) {}
    private static final class Capture extends ResultCallback.Adapter<Frame> {
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream(), stderr = new ByteArrayOutputStream();
        volatile RuntimeException failure;
        @Override public synchronized void onNext(Frame frame) {
            if (failure != null) return;
            if (stdout.size() + stderr.size() + frame.getPayload().length > 256 * 1024) {
                failure = new IllegalStateException("Storage exec output exceeds 256 KiB"); onError(failure); return;
            }
            (frame.getStreamType() == StreamType.STDERR ? stderr : stdout).writeBytes(frame.getPayload());
        }
        void requireComplete() { if (failure != null) throw failure; }
    }

    final class Mount {
        final VerifiedFlinkContainer container; final String logicalName; final FlinkComponentRole role;
        final String instanceId = "instance-" + UUID.randomUUID();
        Instance instance; Status status; Verification verification; boolean dead; boolean journalComplete;
        String journalPath, journalSha256; long journalBytes;
        List<Operation> operations = List.of(); final List<Command> commands = new ArrayList<>(); final List<String> diagnostics = new ArrayList<>();
        Mount(VerifiedFlinkContainer container, String logicalName, FlinkComponentRole role) { this.container = container; this.logicalName = logicalName; this.role = role; }
        String logPath() { return EVIDENCE + "/flink-" + instanceId + ".log"; }
        InstanceEvidence evidence() { return new InstanceEvidence(instance, verification, status, dead, journalComplete && status != null && status.journalComplete(), journalPath, journalSha256, journalBytes, operations.size(), operations, commands, diagnostics); }
    }
}
