package org.savonitar.flink.stability.testcontainers;

import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/** One namespace owner, exact traffic selectors, bounded counter evidence, unconditional scoped cleanup. */
final class PacketFaultBackend {
    static final String ROOT_HANDLE = "7f00:", NETEM_HANDLE = "7f30:";
    private static final Set<String> OWNERS = ConcurrentHashMap.newKeySet();
    interface Driver {
        PacketFaultControl.Binding resolve(KafkaBrokerControl.Target target, MonotonicDeadline deadline) throws Exception;
        void verify(PacketFaultControl.Binding binding, MonotonicDeadline deadline) throws Exception;
        void open(PacketFaultControl.Request request, PacketFaultControl.Binding binding, String chain, MonotonicDeadline deadline) throws Exception;
        String sidecarId(); String imageId();
        PacketFaultControl.Receipt exec(List<String> command, MonotonicDeadline deadline) throws Exception;
        void close(MonotonicDeadline deadline) throws Exception;
        default long nanoTime() { return System.nanoTime(); }
        default long clockMillis() { return System.currentTimeMillis(); }
        default void sleep(Duration duration) throws InterruptedException { Thread.sleep(duration.toMillis(), duration.toNanosPart() % 1_000_000); }
    }
    static PacketFaultControl.Evidence execute(PacketFaultControl.Request request, Driver driver) {
        var deadline = MonotonicDeadline.start(request.timeout(), driver::nanoTime);
        long started = driver.clockMillis(), held = 0;
        PacketFaultControl.Binding binding = null; PacketFaultControl.Counters before = null, after = null;
        String device = null, error = null, sidecar = null, image = null;
        String chain = "FSCHAOS_" + UUID.randomUUID().toString().replace("-", "").substring(0,12);
        List<PacketFaultControl.Receipt> receipts = new ArrayList<>();
        boolean locked = false, opened = false, attempted = false, healed = false, removed = false;
        try {
            binding = driver.resolve(request.broker(), deadline);
            if (!binding.selection().requested().equals(request.broker())) throw new IllegalStateException("Selector binding changed");
            locked = OWNERS.add(binding.taskManagerId());
            if (!locked) throw new IllegalStateException("TaskManager namespace already owns a packet fault");
            driver.verify(binding, deadline);
            opened = true; // Even an ambiguous create/start failure must enter cleanup.
            driver.open(request, binding, chain, deadline);
            sidecar = driver.sidecarId(); image = driver.imageId();
            run(driver, receipts, List.of("sh", "-ceu", "command -v ip; command -v tc; command -v iptables; tc -V; iptables --version"), deadline);
            var route = run(driver, receipts, List.of("ip", "-o", "-4", "route", "get", binding.brokerIpv4()), deadline).stdout();
            device = device(route);
            if (request.action() != PacketFaultControl.Action.BLACKHOLE) {
                var qdiscs = run(driver, receipts, List.of("tc", "qdisc", "show", "dev", device), deadline).stdout();
                if (!qdiscs.lines().allMatch(line -> line.isBlank() || line.matches("qdisc noqueue 0: root(?: .*)?")))
                    throw new IllegalStateException("Foreign qdisc: refusing to replace namespace state");
            }
            driver.verify(binding, deadline);
            if (deadline.remaining().compareTo(request.duration()) <= 0) throw new IllegalStateException("Insufficient packet hold budget");
            attempted = true;
            run(driver, receipts, List.of("sh", "-ceu", "printf %s \"$1\" > /run/flink-packet-owned", "sh", device), deadline);
            for (var command : install(request, binding, device, chain)) run(driver, receipts, command, deadline);
            before = counters(request.action(), run(driver, receipts, counterCommand(request.action(), device, chain), deadline).stdout());
            long heldAt = driver.nanoTime();
            if (deadline.remaining().compareTo(request.duration()) < 0) throw new IllegalStateException("Installation exhausted hold budget");
            driver.sleep(request.duration()); held = driver.nanoTime() - heldAt;
            driver.verify(binding, deadline);
            var receipt = run(driver, receipts, counterCommand(request.action(), device, chain), deadline);
            after = counters(request.action(), receipt.stdout());
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            error = failure.toString();
        } finally {
            boolean interrupted = Thread.interrupted();
            if (locked && opened) {
                var cleanup = MonotonicDeadline.start(Duration.ofSeconds(30), driver::nanoTime);
                try {
                    if (attempted) {
                        // Keep trying all scoped cleanup commands after an individual failure.
                        for (var command : heal(request.action(), binding, device, chain)) {
                            try { retain(receipts, driver.exec(command, cleanup)); }
                            catch (Exception failure) { error = append(error, "Heal command failed: " + failure); }
                        }
                        var state = run(driver, receipts, request.action() == PacketFaultControl.Action.BLACKHOLE
                                ? List.of("iptables", "-w", "2", "-S") : List.of("tc", "qdisc", "show", "dev", device), cleanup).stdout();
                        healed = request.action() == PacketFaultControl.Action.BLACKHOLE ? !state.contains(chain)
                                : !state.contains(ROOT_HANDLE) && !state.contains(NETEM_HANDLE);
                        if (!healed) error = append(error, "Owned packet rules remain after healing");
                    } else healed = true;
                    // Remove only after verified healing. Otherwise keep the watchdog alive.
                    if (healed) { driver.close(cleanup); removed = true; }
                } catch (Exception failure) { error = append(error, "Packet cleanup failed; watchdog retained: " + failure); }
            }
            if (locked && (!opened || healed && removed)) OWNERS.remove(binding.taskManagerId());
            if (interrupted) Thread.currentThread().interrupt();
        }
        return new PacketFaultControl.Evidence(request, binding, sidecar, image, device, started, driver.clockMillis(), held,
                before, after, healed, removed, receipts, error);
    }
    private static String append(String current, String error) { return current == null ? error : current + "; " + error; }
    private static PacketFaultControl.Receipt run(Driver driver, List<PacketFaultControl.Receipt> receipts,
            List<String> command, MonotonicDeadline deadline) throws Exception {
        if (deadline.remaining().isZero() || Thread.currentThread().isInterrupted()) throw new IllegalStateException("Packet deadline expired/interrupted");
        var result = driver.exec(command, deadline); retain(receipts, result);
        if (result.exitCode() != 0) throw new IllegalStateException("Packet command failed: " + command + ": " + result.stderr());
        if (deadline.remaining().isZero()) throw new IllegalStateException("Packet result arrived after deadline");
        return result;
    }
    private static void retain(List<PacketFaultControl.Receipt> receipts, PacketFaultControl.Receipt result) {
        if (result.stdout().length() + result.stderr().length() > 65_536) {
            receipts.add(new PacketFaultControl.Receipt(result.atMillis(), result.command(), result.exitCode(),
                    result.stdout().substring(0, Math.min(32_768, result.stdout().length())),
                    result.stderr().substring(0, Math.min(32_768, result.stderr().length()))));
            throw new IllegalStateException("Packet receipt exceeds evidence bound; retained a truncated receipt");
        }
        receipts.add(result);
    }
    static String device(String route) {
        var matcher = Pattern.compile("\\bdev ([a-zA-Z0-9_.-]{1,15})(?: |$)").matcher(route.strip());
        if (!matcher.find()) throw new IllegalArgumentException("No unambiguous IPv4 route device");
        String device = matcher.group(1);
        if (matcher.find() || route.strip().contains("\n") || device.equals("lo")) throw new IllegalArgumentException("Ambiguous/loopback route");
        return device;
    }
    static List<List<String>> install(PacketFaultControl.Request request, PacketFaultControl.Binding binding, String device, String chain) {
        if (request.action() == PacketFaultControl.Action.BLACKHOLE) return List.of(
                List.of("iptables","-w","2","-N",chain), List.of("iptables","-w","2","-A",chain,"-j","DROP"),
                jump("-I", "OUTPUT", binding, chain), jump("-I", "INPUT", binding, chain));
        var netem = new ArrayList<>(List.of("tc","qdisc","add","dev",device,"parent",ROOT_HANDLE+"3","handle",NETEM_HANDLE,"netem"));
        if (request.action() == PacketFaultControl.Action.LOSS) netem.addAll(List.of("loss",request.lossPercent()+"%"));
        else netem.addAll(List.of("delay",request.delayMillis()+"ms",request.jitterMillis()+"ms"));
        return List.of(List.of("tc","qdisc","add","dev",device,"root","handle",ROOT_HANDLE,"prio","bands","3","priomap",
                        "0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0"), netem,
                List.of("tc","filter","add","dev",device,"protocol","ip","parent",ROOT_HANDLE,"prio","1","u32",
                        "match","ip","dst",binding.brokerIpv4()+"/32","match","ip","protocol","6","0xff",
                        "match","ip","dport",Integer.toString(binding.brokerPort()),"0xffff","flowid",ROOT_HANDLE+"3"));
    }
    static List<List<String>> heal(PacketFaultControl.Action action, PacketFaultControl.Binding binding, String device, String chain) {
        if (action != PacketFaultControl.Action.BLACKHOLE) return List.of(List.of("tc","qdisc","del","dev",device,"root","handle",ROOT_HANDLE));
        return List.of(jump("-D","OUTPUT",binding,chain), jump("-D","INPUT",binding,chain),
                List.of("iptables","-w","2","-F",chain), List.of("iptables","-w","2","-X",chain));
    }
    private static List<String> jump(String operation, String direction, PacketFaultControl.Binding binding, String chain) {
        return List.of("iptables","-w","2",operation,direction,"-p","tcp",direction.equals("OUTPUT") ? "-d" : "-s",
                binding.brokerIpv4(),direction.equals("OUTPUT") ? "--dport" : "--sport",Integer.toString(binding.brokerPort()),"-j",chain);
    }
    static List<String> counterCommand(PacketFaultControl.Action action, String device, String chain) {
        return action == PacketFaultControl.Action.BLACKHOLE ? List.of("iptables","-w","2","-L",chain,"-n","-v","-x")
                : List.of("tc","-s","qdisc","show","dev",device);
    }
    static PacketFaultControl.Counters counters(PacketFaultControl.Action action, String text) {
        var pattern = action == PacketFaultControl.Action.BLACKHOLE
                ? Pattern.compile("(?m)^\\s*(\\d+)\\s+\\d+\\s+DROP\\b")
                : Pattern.compile("qdisc netem " + NETEM_HANDLE + "[^\\n]*\\n\\s*Sent \\d+ bytes (\\d+) pkt \\(dropped (\\d+),");
        var match = pattern.matcher(text);
        if (!match.find()) throw new IllegalArgumentException("Owned fault counters unavailable");
        long packets = Long.parseLong(match.group(1)), dropped = action == PacketFaultControl.Action.BLACKHOLE ? packets : Long.parseLong(match.group(2));
        if (match.find()) throw new IllegalArgumentException("Ambiguous owned fault counters");
        return new PacketFaultControl.Counters(packets, dropped);
    }
}
