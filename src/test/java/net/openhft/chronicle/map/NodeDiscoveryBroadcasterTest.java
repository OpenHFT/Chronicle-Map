package net.openhft.chronicle.map;

import junit.framework.TestCase;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class NodeDiscoveryBroadcasterTest extends TestCase {

    @Test
    @Ignore
    public void test() throws IOException, InterruptedException {

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), 1235);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);

        BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes);


        final NodeDiscoveryBroadcaster nodeDiscoveryBroadcaster
                = new NodeDiscoveryBroadcaster(udpConfig, 1024, externalizable);

        externalizable.setModificationNotifier(nodeDiscoveryBroadcaster);


        Thread.sleep(1000);

        externalizable.sendBootStrap();

        externalizable.add(new InetSocketAddress("myhost", 8888));
        externalizable.add(new InetSocketAddress("myOtherHost", 8888));
         externalizable.add((byte) 2);
        externalizable.onChange();
        Thread.sleep(10000);
        externalizable.add((byte) 2);
        externalizable.onChange();
        Thread.sleep(10000);
        externalizable.add((byte) 3);
        externalizable.onChange();
        Thread.sleep(10000);
        externalizable.add((byte) 4);
        externalizable.onChange();
        Thread.sleep(10000);
        externalizable.add((byte) 1);
        externalizable.onChange();
        Thread.sleep(10000);
        externalizable.onChange();

        Thread.sleep(1000);
    }


    @Test
    @Ignore
    public void testJustListen() throws IOException, InterruptedException {

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), 1235);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);

        BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes);


        final NodeDiscoveryBroadcaster nodeDiscoveryBroadcaster
                = new NodeDiscoveryBroadcaster(udpConfig, 1024, externalizable);

        externalizable.setModificationNotifier(nodeDiscoveryBroadcaster);


        Thread.sleep(10000000);


    }

}
