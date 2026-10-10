package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.Cancellable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliShutdownLoggingTest {
    private static final String RESULT = "{\"status\":\"pass\"}";
    private static final String DIAGNOSTIC = "Unable to register Log4j shutdown hook because JVM is shutting down. Using SimpleLogger";
    private static final String ORDINARY_LOG = "ordinary diagnostic before shutdown";
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @TempDir
    Path temporaryDirectory;

    @Test
    void keepsJsonAndRealShutdownDiagnosticSeparateWithoutChangingExitStatus() throws Exception {
        Path configuration = Path.of(Main.class.getResource("/log4j2.properties").toURI());
        for (int exitCode : new int[]{0, 7}) {
            Invocation result = shutdown(configuration, exitCode);
            assertEquals(exitCode, result.exitCode(), result.stderr());
            assertEquals(RESULT + System.lineSeparator(), result.stdout());
            assertEquals("pass", JSON.readTree(result.stdout()).path("status").asText());
            assertEquals(1, occurrences(result.stderr(), DIAGNOSTIC), result.stderr());
            assertEquals(1, occurrences(result.stderr(), ORDINARY_LOG), result.stderr());
            assertFalse(result.stderr().contains(RESULT), result.stderr());
        }
    }

    @Test
    void originalStatusDestinationReproducesTheMalformedResultStream() throws Exception {
        Path configuration = Path.of(Main.class.getResource("/log4j2.properties").toURI());
        Path original = temporaryDirectory.resolve("original-log4j2.properties");
        Files.writeString(original, Files.readString(configuration).replace("dest = err\n", ""));
        Invocation result = shutdown(original, 0);
        assertEquals(0, result.exitCode(), result.stderr());
        assertTrue(result.stdout().startsWith(RESULT + System.lineSeparator()), result.stdout());
        assertEquals(1, occurrences(result.stdout(), RESULT));
        assertEquals(1, occurrences(result.stdout(), DIAGNOSTIC), result.stdout());
        assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class,
                () -> JSON.readTree(result.stdout()));
        assertFalse(result.stderr().contains(DIAGNOSTIC), result.stderr());
        assertEquals(1, occurrences(result.stderr(), ORDINARY_LOG), result.stderr());
    }

    @Test
    void actualMainRendersOneResolvedPlanFromFullyLocalArtifacts() throws Exception {
        Path catalog = Files.createDirectory(temporaryDirectory.resolve("catalog"));
        Path artifacts = Files.createDirectory(temporaryDirectory.resolve("artifacts"));
        ValidateSpecificationsCommandTest.createJar(artifacts.resolve("connector.jar"), false);
        ValidateSpecificationsCommandTest.createJar(artifacts.resolve("job.jar"), true);
        ValidateSpecificationsCommandTest.writePair(
                catalog, "subprocess-plan", "connector.jar", "job.jar", "");

        Invocation result = invoke("-cp", classpath(), Main.class.getName(),
                "validate", "--catalog-root", catalog.toString(), "--scenario", "subprocess-plan",
                "--artifact-root", artifacts.toString(), "--offline", "--show-plan");

        assertEquals(0, result.exitCode(), result.stderr());
        var plan = JSON.readTree(result.stdout());
        assertEquals("subprocess-plan", plan.path("scenario").asText());
        assertEquals("EXACTLY_ONCE",
                plan.at("/resolved/workload/jobs/0/sink/delivery_guarantee").asText());
        assertEquals(10, plan.at("/resolved/setup/kafka/clusters/main/topics/0/input_source/total").asInt());
    }

    private Invocation shutdown(Path configuration, int exitCode) throws Exception {
        return invoke("-Dlog4j.configurationFile=" + configuration.toUri(), "-cp", classpath(),
                ShutdownProcess.class.getName(), configuration.toUri().toString(), Integer.toString(exitCode));
    }

    private static String classpath() {
        return System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    }

    private Invocation invoke(String... arguments) throws Exception {
        Path directory = Files.createTempDirectory(temporaryDirectory, "process-");
        Path stdout = directory.resolve("stdout.txt");
        Path stderr = directory.resolve("stderr.txt");
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command)
                .directory(directory.toFile())
                .redirectOutput(stdout.toFile()).redirectError(stderr.toFile()).start();
        try {
            assertTrue(process.waitFor(20, TimeUnit.SECONDS), "CLI subprocess exceeded its bound");
            return new Invocation(process.exitValue(), Files.readString(stdout), Files.readString(stderr));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), "CLI subprocess was not reaped");
            }
        }
    }

    private static int occurrences(String text, String needle) {
        return (text.length() - text.replace(needle, "").length()) / needle.length();
    }

    private record Invocation(int exitCode, String stdout, String stderr) {}

    public static final class ShutdownProcess {
        // Log4j keeps callbacks through soft references; retain this one through exit.
        private static Cancellable shutdownCallback;

        public static void main(String[] args) {
            URI configuration = URI.create(args[0]);
            LogManager.getLogger(ShutdownProcess.class).warn(ORDINARY_LOG);
            var factory = (Log4jContextFactory) LogManager.getFactory();
            // The real JVM shutdown hook puts Log4j's registry into STOPPING before
            // invoking this callback. A new context then exercises the genuine
            // registration failure and LogManager's internal-status warning.
            shutdownCallback = factory.addShutdownCallback(() ->
                    LogManager.getContext(new ClassLoader(null) {}, false, configuration));
            System.out.println(RESULT);
            System.exit(Integer.parseInt(args[1]));
        }
    }
}
