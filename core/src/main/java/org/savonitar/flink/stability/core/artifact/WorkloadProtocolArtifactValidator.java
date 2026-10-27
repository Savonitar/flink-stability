package org.savonitar.flink.stability.core.artifact;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Validates the byte-level workload protocol declaration in a staged workload JAR. */
public final class WorkloadProtocolArtifactValidator {
    public static final String ATTRIBUTE = "Flink-Stability-Workload-Protocol";
    public static final String VERSION = "v1";
    public static final String CAPABILITIES_ATTRIBUTE = "Flink-Stability-Workload-Capabilities";
    public static final String KEYED_PREFIX_V1 = "keyed-prefix-v1";
    private static final String MAIN_CLASS_ATTRIBUTE = "Main-Class";

    private static final int MAX_MANIFEST_BYTES = 1024 * 1024;

    private WorkloadProtocolArtifactValidator() {
    }

    /**
     * Requires exactly one nonblank {@value #ATTRIBUTE} main-manifest attribute whose value is
     * exactly {@value #VERSION}. The raw main section is scanned because {@link
     * java.util.jar.Manifest} collapses duplicate attributes.
     */
    public static void validate(Path jar) throws ValidationException {
        validate(jar, Set.of());
    }

    /** Requires optional, comma-separated capability tokens only for opted-in behaviors. */
    public static void validate(Path jar, Set<String> requiredCapabilities) throws ValidationException {
        Objects.requireNonNull(jar, "jar");
        requiredCapabilities = Set.copyOf(requiredCapabilities);
        try (JarFile jarFile = new JarFile(jar.toFile(), false)) {
            JarEntry manifest = jarFile.getJarEntry(JarFile.MANIFEST_NAME);
            if (manifest == null) {
                throw failure(FailureKind.MISSING,
                        "Workload JAR has no main manifest protocol declaration");
            }
            if (manifest.getSize() > MAX_MANIFEST_BYTES) {
                throw failure(FailureKind.UNREADABLE,
                        "Workload JAR manifest exceeds " + MAX_MANIFEST_BYTES + " bytes");
            }
            MainSection mainSection;
            try (InputStream input = new BoundedInputStream(
                            jarFile.getInputStream(manifest), MAX_MANIFEST_BYTES);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(
                            input,
                            StandardCharsets.UTF_8.newDecoder()
                                    .onMalformedInput(CodingErrorAction.REPORT)
                                    .onUnmappableCharacter(CodingErrorAction.REPORT)))) {
                mainSection = mainSection(reader);
            }
            if (mainSection.mainClasses().isEmpty()
                    || mainSection.mainClasses().getFirst().isBlank()) {
                throw failure(FailureKind.ENTRYPOINT_MISSING,
                        "The workload JAR requires a non-empty Main-Class manifest entry");
            }
            if (mainSection.mainClasses().size() != 1) {
                throw failure(FailureKind.UNREADABLE,
                        "Workload JAR declares Main-Class more than once");
            }
            List<String> values = mainSection.protocolValues();
            if (values.isEmpty()) {
                throw failure(FailureKind.MISSING,
                        "Workload JAR must declare main-manifest attribute '"
                                + ATTRIBUTE + ": " + VERSION + "'");
            }
            if (values.size() != 1) {
                throw failure(FailureKind.DUPLICATE,
                        "Workload JAR declares main-manifest attribute '"
                                + ATTRIBUTE + "' more than once");
            }
            String value = values.getFirst();
            if (value.isBlank()) {
                throw failure(FailureKind.MISSING,
                        "Workload JAR protocol marker must not be blank");
            }
            if (!VERSION.equals(value)) {
                throw failure(FailureKind.UNSUPPORTED,
                        "Workload protocol '" + value + "' is unsupported; expected '"
                                + VERSION + "'");
            }
            if (!requiredCapabilities.isEmpty()) {
                validateCapabilities(mainSection.capabilityValues(), requiredCapabilities);
            }
        } catch (ValidationException exception) {
            throw exception;
        } catch (IOException | RuntimeException exception) {
            throw new ValidationException(
                    FailureKind.UNREADABLE,
                    "Cannot inspect workload protocol marker: " + safeMessage(exception),
                    exception);
        }
    }

    private static void validateCapabilities(List<String> values, Set<String> required)
            throws ValidationException {
        if (values.isEmpty() || (values.size() == 1 && values.getFirst().isBlank())) {
            throw failure(FailureKind.CAPABILITY_MISSING,
                    "Workload JAR must declare " + CAPABILITIES_ATTRIBUTE + ": "
                            + String.join(",", required.stream().sorted().toList()));
        }
        if (values.size() != 1) {
            throw failure(FailureKind.CAPABILITY_INVALID,
                    "Workload JAR declares " + CAPABILITIES_ATTRIBUTE + " more than once");
        }
        Set<String> declared = new HashSet<>();
        for (String token : values.getFirst().split(",", -1)) {
            String capability = token.strip();
            if (!capability.matches("[a-z][a-z0-9]*(?:-[a-z0-9]+)*")
                    || !declared.add(capability)) {
                throw failure(FailureKind.CAPABILITY_INVALID,
                        "Workload JAR capability declaration contains an invalid or duplicate token");
            }
        }
        if (!declared.containsAll(required)) {
            throw failure(FailureKind.CAPABILITY_MISSING,
                    "Workload JAR lacks required capabilities: " + required.stream()
                            .filter(capability -> !declared.contains(capability)).sorted().toList());
        }
    }

    private static MainSection mainSection(BufferedReader reader)
            throws IOException, ValidationException {
        List<String> protocolValues = new ArrayList<>();
        List<String> mainClasses = new ArrayList<>();
        List<String> capabilityValues = new ArrayList<>();
        String currentName = null;
        StringBuilder currentValue = null;
        for (String line; (line = reader.readLine()) != null;) {
            if (line.isEmpty()) {
                addRecognized(protocolValues, mainClasses, capabilityValues, currentName, currentValue);
                return new MainSection(protocolValues, mainClasses, capabilityValues);
            }
            if (line.charAt(0) == ' ') {
                if (currentName == null) {
                    throw failure(FailureKind.UNREADABLE,
                            "Workload JAR manifest starts an orphan continuation line");
                }
                currentValue.append(line, 1, line.length());
                continue;
            }
            addRecognized(protocolValues, mainClasses, capabilityValues, currentName, currentValue);
            int separator = line.indexOf(':');
            if (separator <= 0) {
                throw failure(FailureKind.UNREADABLE,
                        "Workload JAR manifest contains a malformed main attribute");
            }
            currentName = line.substring(0, separator);
            int valueStart = separator + 1;
            if (valueStart < line.length() && line.charAt(valueStart) == ' ') {
                valueStart++;
            }
            currentValue = new StringBuilder(line.substring(valueStart));
        }
        addRecognized(protocolValues, mainClasses, capabilityValues, currentName, currentValue);
        return new MainSection(protocolValues, mainClasses, capabilityValues);
    }

    private static void addRecognized(
            List<String> protocolValues,
            List<String> mainClasses,
            List<String> capabilityValues,
            String name,
            StringBuilder value) {
        if (name != null && ATTRIBUTE.equalsIgnoreCase(name)) {
            protocolValues.add(value.toString());
        } else if (name != null && MAIN_CLASS_ATTRIBUTE.equalsIgnoreCase(name)) {
            mainClasses.add(value.toString());
        } else if (name != null && CAPABILITIES_ATTRIBUTE.equalsIgnoreCase(name)) {
            capabilityValues.add(value.toString());
        }
    }

    private static ValidationException failure(FailureKind kind, String message) {
        return new ValidationException(kind, message, null);
    }

    private static String safeMessage(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank()
                ? failure.getClass().getSimpleName()
                : message;
    }

    public enum FailureKind {
        ENTRYPOINT_MISSING("entrypoint-missing"),
        MISSING("missing"),
        DUPLICATE("duplicate"),
        UNSUPPORTED("unsupported"),
        CAPABILITY_MISSING("capability-missing"),
        CAPABILITY_INVALID("capability-invalid"),
        UNREADABLE("unreadable");

        private final String diagnosticSuffix;

        FailureKind(String diagnosticSuffix) {
            this.diagnosticSuffix = diagnosticSuffix;
        }

        public String diagnosticSuffix() {
            return diagnosticSuffix;
        }
    }

    private record MainSection(
            List<String> protocolValues, List<String> mainClasses, List<String> capabilityValues) {
        private MainSection {
            protocolValues = List.copyOf(protocolValues);
            mainClasses = List.copyOf(mainClasses);
            capabilityValues = List.copyOf(capabilityValues);
        }
    }

    public static final class ValidationException extends Exception {
        private final FailureKind kind;

        private ValidationException(FailureKind kind, String message, Throwable cause) {
            super(message, cause);
            this.kind = Objects.requireNonNull(kind, "kind");
        }

        public FailureKind kind() {
            return kind;
        }
    }

    private static final class BoundedInputStream extends InputStream {
        private final InputStream delegate;
        private final long limit;
        private long count;

        private BoundedInputStream(InputStream delegate, long limit) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.limit = limit;
        }

        @Override
        public int read() throws IOException {
            int value = delegate.read();
            if (value >= 0) {
                increment(1);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = delegate.read(bytes, offset, length);
            if (read > 0) {
                increment(read);
            }
            return read;
        }

        private void increment(long amount) throws IOException {
            count += amount;
            if (count > limit) {
                throw new IOException("Workload JAR manifest exceeds " + limit + " bytes");
            }
        }
    }
}
