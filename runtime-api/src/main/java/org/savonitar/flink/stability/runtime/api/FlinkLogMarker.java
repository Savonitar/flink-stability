package org.savonitar.flink.stability.runtime.api;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** An explicit, per-incarnation assertion against retained Flink process output. */
public record FlinkLogMarker(String name, String regex, String scope, boolean required) {
    public FlinkLogMarker {
        if (name == null || !name.matches("[A-Za-z0-9][A-Za-z0-9._-]*"))
            throw new IllegalArgumentException("Log marker name must be a simple nonempty identifier");
        Checks.requireNonBlank(regex, "log marker regex");
        Pattern.compile(regex);
        if (scope == null || !List.of("taskmanager", "jobmanager").contains(scope))
            throw new IllegalArgumentException("Log marker scope must be taskmanager or jobmanager");
    }
    public FlinkComponentRole role() {
        return scope.equals("taskmanager") ? FlinkComponentRole.TASK_MANAGER : FlinkComponentRole.JOB_MANAGER;
    }
    public static List<FlinkLogMarker> validate(List<FlinkLogMarker> values) {
        var checked = List.copyOf(Objects.requireNonNull(values, "logMarkers"));
        var names = new HashSet<String>();
        for (var marker : checked) if (!names.add(marker.name()))
            throw new IllegalArgumentException("Duplicate Flink log marker name: " + marker.name());
        return checked;
    }
}
