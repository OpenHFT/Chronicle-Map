package net.openhft.chronicle.map;

import junit.framework.TestCase;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.nio.ByteBuffer;

public class NodeDiscoveryHostPortBroadcasterTest extends TestCase {

    @Test
    public void test() throws IOException, InterruptedException {

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), 1235);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);

        final NodeDiscoveryHostPortBroadcaster nodeDiscoveryHostPortBroadcaster
                = new NodeDiscoveryHostPortBroadcaster(udpConfig, 1024, remoteNodes);

        Thread.sleep(5000);
    }


}