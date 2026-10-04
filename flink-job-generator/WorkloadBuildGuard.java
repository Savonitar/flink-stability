import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

/** Invoked by the JDK source launcher before compilation; no additional build dependency. */
class WorkloadBuildGuard {
    public static void main(String[] arguments) throws IOException {
        if (arguments.length != 6) throw new IllegalArgumentException("Expected target directory and five workload build settings");
        Path target = Path.of(arguments[0]).toAbsolutePath().normalize();
        for (Path parent = target; parent != null; parent = parent.getParent()) {
            if (Files.isSymbolicLink(parent)) throw new IOException("Workload build output must not traverse a symlink");
        }
        String[] names = {"flink.version", "kafka.connector.groupId", "kafka.connector.version",
                "maven.compiler.release", "kafka.clients.version"};
        StringBuilder signature = new StringBuilder();
        for (int index = 0; index < names.length; index++) {
            String value = arguments[index + 1];
            if (value.isBlank() || value.chars().anyMatch(Character::isISOControl))
                throw new IllegalArgumentException("Invalid workload build setting: " + names[index]);
            signature.append(names[index]).append('=').append(value).append('\n');
        }
        Path version = target.resolve("flink.version");
        Path settings = target.resolve("workload-build.signature");
        for (Path marker : new Path[] {version, settings}) {
            if (Files.isSymbolicLink(marker)) throw new IOException("Workload build marker must not be a symlink");
        }
        boolean compiled = Files.exists(target.resolve("classes"), LinkOption.NOFOLLOW_LINKS)
                || Files.exists(target.resolve("flink-job-generator.jar"), LinkOption.NOFOLLOW_LINKS)
                || Files.exists(target.resolve("maven-status"), LinkOption.NOFOLLOW_LINKS);
        boolean versionExists = Files.exists(version, LinkOption.NOFOLLOW_LINKS);
        boolean settingsExist = Files.exists(settings, LinkOption.NOFOLLOW_LINKS);
        if ((compiled && (!versionExists || !settingsExist))
                || (versionExists && !Files.readString(version).equals(arguments[1] + "\n"))
                || (settingsExist && !Files.readString(settings).equals(signature.toString()))) {
            throw new IllegalStateException("Workload output belongs to another or unrecorded build configuration. "
                    + "Run mvn -pl flink-job-generator clean package with the requested Flink, connector and compiler-release settings.");
        }
        Files.createDirectories(target);
        if (!versionExists) Files.writeString(version, arguments[1] + "\n");
        if (!settingsExist) Files.writeString(settings, signature.toString());
    }
}
