package net.openhft.chronicle.map;

import junit.framework.TestCase;

public class NodeDiscoveryBroadcasterTest extends TestCase {

    public void test() {
        // test to stop it erroring with no test
    }


   /* @Test
    @Ignore
    public void test() throws IOException, InterruptedException {

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), 1235);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);

        UDPEventListener udpEventListener = new UDPEventListener() {

            @Override
            public void onRemoteNodeEvent(RemoteNodes allNodes, byte remoteProposedId) {

            }
        };

        BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes, udpEventListener);


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

        UDPEventListener udpEventListener = new UDPEventListener() {

            @Override
            public void onRemoteNodeEvent(RemoteNodes allNodes, byte remoteProposedId) {

            }
        };

        BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes, udpEventListener);


        final NodeDiscoveryBroadcaster nodeDiscoveryBroadcaster
                = new NodeDiscoveryBroadcaster(udpConfig, 1024, externalizable);

        externalizable.setModificationNotifier(nodeDiscoveryBroadcaster);


        Thread.sleep(10000000);


    }*/

}
