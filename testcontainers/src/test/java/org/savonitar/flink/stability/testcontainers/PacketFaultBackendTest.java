package org.savonitar.flink.stability.testcontainers;

import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class PacketFaultBackendTest {
    static final String TEST_IMAGE = "docker.io/nicolaka/netshoot@sha256:" + "a".repeat(64); // Synthetic digest; never pulled.
    static final KafkaBrokerControl.Target TARGET = new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.PARTITION_LEADER,null,"output",0,null);
    static PacketFaultControl.Request request(PacketFaultControl.Action action) {
        return new PacketFaultControl.Request("taskmanager-1", TARGET, TEST_IMAGE, action,
                action == PacketFaultControl.Action.LOSS ? 25 : 0, action == PacketFaultControl.Action.DELAY ? 100 : 0,
                action == PacketFaultControl.Action.DELAY ? 20 : 0, Duration.ofMillis(10), Duration.ofMillis(100));
    }
    @Test void eachActionNeedsRealCountersAndConfirmedScopedHealing() {
        for (var action : PacketFaultControl.Action.values()) {
            var driver = new Fake(); var result = PacketFaultBackend.execute(request(action), driver);
            assertTrue(result.confirmed(), result.toString()); assertTrue(driver.closed); assertFalse(driver.active);
            assertEquals(10_000_000, result.heldNanos()); assertEquals("eth0", result.device());
            assertEquals(driver.binding, result.binding());
            assertEquals(new PacketFaultControl.Counters(0,0), result.before());
            assertTrue(result.after().packets() > 0);
            assertFalse(result.receipts().stream().anyMatch(r -> r.command().contains("flush") || r.command().contains("replace")));
            assertTrue(result.receipts().stream().filter(r -> r.command().contains("filter") || r.command().contains("OUTPUT") || r.command().contains("INPUT"))
                    .allMatch(r -> r.command().contains("172.18.0.5") || r.command().contains("172.18.0.5/32")));
            driver = new Fake(); driver.zeroCounters = true;
            assertFalse(PacketFaultBackend.execute(request(action), driver).confirmed());
        }
    }
    @Test void allBrokersShareOneQueueWithThreeExactFiltersAndIdentityChecks() {
        var targets = java.util.stream.IntStream.rangeClosed(1,3).mapToObj(i ->
                new KafkaBrokerControl.Target(KafkaBrokerControl.TargetKind.NAMED,"broker-"+i,null,-1,null)).toList();
        var single=request(PacketFaultControl.Action.DELAY);
        var request=new PacketFaultControl.Request(single.taskManager(), targets.getFirst(), single.image(), single.action(),
                0,100,20,single.duration(),single.timeout(),targets.subList(1,3));
        var driver = new Fake() {
            @Override public PacketFaultControl.Binding resolve(KafkaBrokerControl.Target target,MonotonicDeadline deadline) {
                int id=Integer.parseInt(target.name().substring(7));
                return new PacketFaultControl.Binding(binding.taskManagerId(),binding.taskManagerImageId(),String.format("%064x",100+id),
                        "broker-image",binding.networkId(),"172.18.0."+(10+id),19092,
                        new KafkaBrokerControl.Selection(target,target.name(),null,-1,-1,null,null,null,id));
            }
        };
        var result=PacketFaultBackend.execute(request,driver);
        assertTrue(result.confirmed(),result.toString()); assertEquals(2,result.additionalBindings().size());
        assertEquals(3,driver.commands.stream().filter(c -> c.contains("filter")).count());
        assertEquals(1,driver.commands.stream().filter(c -> c.contains("netem")).count());
        assertEquals(1,driver.commands.stream().filter(c -> c.contains("del")).count());
        assertEquals(9,driver.verifies);
    }
    @Test void foreignQdiscIsNotModifiedEvenByCleanupOrWatchdogActivation() {
        var driver = new Fake(); driver.foreign = true;
        var result = PacketFaultBackend.execute(request(PacketFaultControl.Action.LOSS),driver);
        assertFalse(result.confirmed()); assertTrue(driver.closed);
        assertTrue(driver.commands.stream().noneMatch(c -> c.contains("add") || c.contains("del") || c.toString().contains("flink-packet-owned")));
    }
    @Test void partialInstallIdentityChangeAndLateCommandAllHeal() {
        for (int mode = 0; mode < 3; mode++) {
            var driver = new Fake(); driver.failInstall = mode == 0; driver.changeIdentity = mode == 1; driver.late = mode == 2;
            var result = PacketFaultBackend.execute(request(PacketFaultControl.Action.DELAY), driver);
            assertFalse(result.confirmed()); assertTrue(result.healed()); assertTrue(driver.closed); assertFalse(driver.active);
            assertTrue(driver.commands.stream().anyMatch(c -> c.contains("del")));
        }
    }
    @Test void failedHealingKeepsWatchdogAndNamespaceLockInsteadOfClaimingCleanup() {
        var driver = new Fake(); driver.failHeal = true;
        var result = PacketFaultBackend.execute(request(PacketFaultControl.Action.BLACKHOLE), driver);
        assertFalse(result.confirmed()); assertFalse(result.healed()); assertFalse(driver.closed);
        int opens = driver.opens;
        result = PacketFaultBackend.execute(request(PacketFaultControl.Action.BLACKHOLE), driver);
        assertTrue(result.error().contains("already owns")); assertEquals(opens, driver.opens);
    }
    @Test void interruptionStillHealsBeforeRestoringInterruptFlag() {
        var driver = new Fake(); driver.interrupt = true;
        try {
            var result = PacketFaultBackend.execute(request(PacketFaultControl.Action.LOSS), driver);
            assertFalse(result.confirmed()); assertTrue(result.healed()); assertTrue(driver.closed);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally { Thread.interrupted(); }
    }
    @Test void historicalCountersAndUnknownRemovalCannotConfirm() {
        for (var action : PacketFaultControl.Action.values()) {
            var historical = new Fake(); historical.constantCounters = true;
            var result = PacketFaultBackend.execute(request(action), historical);
            assertFalse(result.confirmed()); assertEquals(result.before(), result.after());
            var removal = new Fake(); removal.failClose = true;
            result = PacketFaultBackend.execute(request(action), removal);
            assertFalse(result.confirmed()); assertTrue(result.healed()); assertFalse(result.sidecarRemoved());
        }
    }
    @Test void oversizedCleanupOutputIsBoundedAndCannotConfirm() {
        var driver = new Fake(); driver.hugeHeal = true;
        var result = PacketFaultBackend.execute(request(PacketFaultControl.Action.LOSS), driver);
        assertFalse(result.confirmed()); assertTrue(result.healed()); assertTrue(driver.closed);
        assertTrue(result.receipts().stream().allMatch(r -> r.stdout().length() + r.stderr().length() <= 65_536));
    }
    @Test void countersAndRoutesRejectAmbiguousOrMissingProof() {
        assertThrows(IllegalArgumentException.class, () -> PacketFaultBackend.device("172.18.0.5 dev lo"));
        assertThrows(IllegalArgumentException.class, () -> PacketFaultBackend.device("172.18.0.5 dev eth0\nother dev eth1"));
        assertThrows(IllegalArgumentException.class, () -> PacketFaultBackend.counters(PacketFaultControl.Action.LOSS,"qdisc pfifo 1: root\n Sent 1 bytes 1 pkt (dropped 1,"));
        assertThrows(IllegalArgumentException.class, () -> PacketFaultBackend.counters(PacketFaultControl.Action.BLACKHOLE,"0 0 ACCEPT all --"));
        assertThrows(IllegalArgumentException.class, () -> new PacketFaultControl.Request("taskmanager-1",TARGET,"nicolaka/netshoot:latest",
                PacketFaultControl.Action.LOSS,10,0,0,Duration.ofSeconds(1),Duration.ofSeconds(2)));
        assertThrows(IllegalArgumentException.class, () -> new PacketFaultControl.Request("taskmanager-1",TARGET,TEST_IMAGE,
                PacketFaultControl.Action.DELAY,0,100,101,Duration.ofSeconds(1),Duration.ofSeconds(2)));
    }
    static class Fake implements PacketFaultBackend.Driver {
        static AtomicLong sequence = new AtomicLong();
        final PacketFaultControl.Binding binding = new PacketFaultControl.Binding(String.format("%064x", sequence.incrementAndGet()),"tm-image","b".repeat(64),
                "broker-image","network","172.18.0.5",19092,new KafkaBrokerControl.Selection(TARGET,"broker-2","output",0,0,null,null,null,2));
        final List<List<String>> commands = new ArrayList<>();
        boolean active,closed,foreign,failInstall,changeIdentity,late,failHeal,interrupt,zeroCounters,constantCounters,failClose,hugeHeal; int verifies,opens; long time;
        String chain;
        public long nanoTime() { return time; } public long clockMillis() { return time / 1_000_000; }
        public PacketFaultControl.Binding resolve(KafkaBrokerControl.Target target, MonotonicDeadline d) { return binding; }
        public void verify(PacketFaultControl.Binding binding, MonotonicDeadline d) { if (++verifies == 3 && changeIdentity) throw new IllegalStateException("Identity changed"); }
        public void open(PacketFaultControl.Request r, PacketFaultControl.Binding b, String chain, MonotonicDeadline d) { opens++; this.chain=chain; }
        public String sidecarId() { return "sidecar"; } public String imageId() { return "sidecar-image"; }
        public void close(MonotonicDeadline d) { if (failClose) throw new IllegalStateException("Unknown removal"); closed=true; }
        public void sleep(Duration hold) throws InterruptedException { if (interrupt) throw new InterruptedException("test"); time+=hold.toNanos(); }
        public PacketFaultControl.Receipt exec(List<String> command, MonotonicDeadline d) {
            commands.add(command); String out=""; int code=0;
            if (command.getFirst().equals("ip")) out="172.18.0.5 dev eth0 src 172.18.0.8";
            if (command.contains("add") || command.contains("-N")) active=true;
            if (command.contains("netem") && failInstall) code=1;
            if (command.contains("netem") && late) { time=200_000_000; late=false; }
            if (command.contains("del") || command.contains("-X")) { if(failHeal)code=1;else active=false; }
            if (command.contains("show") && !command.contains("-s")) out=foreign ? "qdisc fq_codel 1: root" : active ? "qdisc prio 7f00: root" : "qdisc noqueue 0: root refcnt 2";
            if (command.contains("-S")) out=active ? "-N "+chain : "";
            if (command.getFirst().equals("tc") && command.contains("-s")) out="qdisc netem 7f30: parent 7f00:3 limit 1000\n Sent 200 bytes "+(zeroCounters || time == 0 && !constantCounters ? 0 : 10)+" pkt (dropped "+(zeroCounters || time == 0 && !constantCounters ? 0 : 2)+", overlimits 0 requeues 0)";
            if (command.contains("-L")) out=(zeroCounters || time == 0 && !constantCounters ? 0 : 5)+" 100 DROP all -- * * 0.0.0.0/0 0.0.0.0/0";
            if (hugeHeal && command.contains("del")) out="x".repeat(70_000);
            return new PacketFaultControl.Receipt(clockMillis(),command,code,out,code==0?"":"injected failure");
        }
    }
}
