package net.openhft.chronicle.map;

import junit.framework.TestCase;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class NodeDiscoveryHostPortBroadcasterTest extends TestCase {

    @Test
    public void test() throws IOException, InterruptedException {

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), 1235);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);

        BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes);


        final NodeDiscoveryHostPortBroadcaster nodeDiscoveryHostPortBroadcaster
                = new NodeDiscoveryHostPortBroadcaster(udpConfig, 1024, externalizable);

        externalizable.setModificationNotifier(nodeDiscoveryHostPortBroadcaster);


        Thread.sleep(1000);

        externalizable.sendBootStrap();

        externalizable.add(new InetSocketAddress("myhost", 8888));
        externalizable.add(new InetSocketAddress("myOtherHost", 8888));
        externalizable.add((byte) 2);
        externalizable.add((byte) 2);
        externalizable.add((byte) 3);
        externalizable.add((byte) 4);
        externalizable.add((byte) 1);
        externalizable.onChange();

        Thread.sleep(1000);
    }


    @Test
    public void testJustListen() throws IOException, InterruptedException {

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), 1235);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);

        BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes);


        final NodeDiscoveryHostPortBroadcaster nodeDiscoveryHostPortBroadcaster
                = new NodeDiscoveryHostPortBroadcaster(udpConfig, 1024, externalizable);

        externalizable.setModificationNotifier(nodeDiscoveryHostPortBroadcaster);


        Thread.sleep(10000000);


    }

}
