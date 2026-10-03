package org.savonitar.flink.stability.testcontainers;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.*;
import org.junit.jupiter.api.Test;
import org.savonitar.flink.stability.runtime.api.PacketFaultControl;
import java.lang.reflect.Proxy;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class DockerPacketFaultSidecarTest {
    @Test void sidecarUsesOnlyTargetNamespaceAndNetAdminWithoutHostMounts() {
        var request=PacketFaultBackendTest.request(PacketFaultControl.Action.DELAY);
        var binding=new PacketFaultBackendTest.Fake().binding;
        var container=DockerPacketFaultSidecar.container(request,binding,"FSCHAOS_123456abcdef");
        assertEquals("container:"+binding.taskManagerId(),container.getNetworkMode());
        var host=new HostConfig(); List<String> entrypoint=new ArrayList<>();
        var command=(CreateContainerCmd)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{CreateContainerCmd.class},(proxy,method,args)->{
            if(method.getName().equals("getHostConfig"))return host;
            if(method.getName().equals("withEntrypoint")){entrypoint.addAll(Arrays.asList((String[])args[0]));return proxy;}
            throw new AssertionError(method.getName());
        });
        DockerPacketFaultSidecar.configure(command);
        assertArrayEquals(new Capability[]{Capability.ALL},host.getCapDrop());
        assertArrayEquals(new Capability[]{Capability.NET_ADMIN},host.getCapAdd());
        assertTrue(host.getReadonlyRootfs()); assertEquals(0, host.getBinds().length); assertNotEquals(Boolean.TRUE,host.getPrivileged());
        assertEquals(Set.of("/run"),host.getTmpFs().keySet()); assertEquals(List.of("/bin/sh","-c"),entrypoint);
    }
    @Test void watchdogNeedsActivationAndOnlyRemovesItsOwnRules() {
        for(var action:PacketFaultControl.Action.values()) {
            var script=DockerPacketFaultSidecar.watchdog(PacketFaultBackendTest.request(action),new PacketFaultBackendTest.Fake().binding,"FSCHAOS_123456abcdef");
            assertTrue(script.contains("[ -f /run/flink-packet-owned ]")); assertTrue(script.contains("trap cleanup EXIT"));
            assertTrue(script.contains("sleep 30")); assertFalse(script.contains("iptables -F;"));
            if(action==PacketFaultControl.Action.BLACKHOLE){assertTrue(script.contains("172.18.0.5"));assertTrue(script.contains("19092"));}
            else assertTrue(script.contains("root handle 7f00:"));
        }
    }
}
