package net.openhft.chronicle.map;

import junit.framework.TestCase;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.junit.Ignore;
import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.net.Inet4Address;

import static java.nio.ByteBuffer.allocate;

public class NodeDiscoveryBroadcasterTest extends TestCase {

    public static final int SERVER2_IDENTIFER = 10;

    public void test() {
        // test to stop it erroring with no test
    }

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private volatile byte proposedIdentifier = 1;
    private volatile boolean useAnotherIdentifier = false;


    @Test
    @Ignore
    public void test2() throws IOException, InterruptedException {

        DiscoveryCluster discoveryCluster = new DiscoveryCluster();

        ChronicleMap<Integer, CharSequence> map = discoveryCluster.discoverMap(1025, 8087);

        Thread.sleep(10000);
    }


    @Test
    @Ignore
    public void test3() throws IOException, InterruptedException {

        // write broadcast our address on the bootstrap method
        byte[] server1Address = Inet4Address.getLocalHost().getAddress();
        AddressAndPort server1AddressAndPort = new AddressAndPort(server1Address, (short) 1234);


        // write broadcast our address on the bootstrap method
        byte[] server2Address = Inet4Address.getByName("129.168.0.2").getAddress();
        AddressAndPort server2AddressAndPort = new AddressAndPort(server1Address, (short) 1234);


        RemoteNodes server1RemoteNodes = new RemoteNodes();
        BytesExternalizableImpl server1BytesExternalizable = new BytesExternalizableImpl(server1RemoteNodes, null);

        // for the unit test we are using a ByteBufferBytes, but iltimately this data would have been send
        // and received via UDP
        ByteBufferBytes udpData = new ByteBufferBytes(allocate(1024));

        // SERER1
        // we will first send the boostrap along with our host and port
        {


            BytesExternalizableImpl.ProposedIdentifierWithHost proposedIdentifierWithHost = new BytesExternalizableImpl
                    .ProposedIdentifierWithHost(server1AddressAndPort, (byte) -1);

            server1BytesExternalizable.sendBootStrap(proposedIdentifierWithHost);

            server1BytesExternalizable.writeMarshallable(udpData);

        }

        RemoteNodes server2RemoteNodes = new RemoteNodes();
        server2RemoteNodes.add(server2AddressAndPort, (byte) SERVER2_IDENTIFER);
        BytesExternalizableImpl server2BytesExternalizable = new BytesExternalizableImpl(server2RemoteNodes, null);

        // add our identifier and host:port to the list of known identifiers


        // SERVER 2
        // read the bootstrap along with the host and port it came from
        {
            udpData.flip();
            server2BytesExternalizable.readMarshallable(udpData);

            Assert.assertTrue(server2BytesExternalizable.proposedIdentifiersWithHost.values().contains
                    (new BytesExternalizableImpl.ProposedIdentifierWithHost(server1AddressAndPort, (byte) -1)));
        }

        // SERVER 2
        // write/broadcast the the response to receiving a bootstrap message

        {
            //ByteBufferBytes bitSetBytes = new ByteBufferBytes(allocate(128 / 8));
            //      final DirectBitSet knownIdentifiers = new ATSDirectBitSet(bitSetBytes);

            //    RemoteNodes remoteNodes = new RemoteNodes(bitSetBytes);


            udpData.clear();

            // broadcasts in response to the bootstrap all the host:ports and identifiers that it knows about
            // in this case given that there is only this node in the grid and one node joining,
            // it will only be the node that has the bootstrap and this node, who's data will be sent
            server2BytesExternalizable.writeMarshallable(udpData);

        }


        // SERER1
        // receive

        // read the bootstrap along with the host and port it came from
        {
            udpData.flip();
            server1BytesExternalizable.readMarshallable(udpData);

            Assert.assertTrue(server1BytesExternalizable.getRemoteNodes().addressAndPorts().contains
                    (server2AddressAndPort));

            Assert.assertTrue(server1BytesExternalizable.getRemoteNodes().activeIdentifierBitSet().get
                    (SERVER2_IDENTIFER));

        }


        {


            RemoteNodes remoteNodes = new RemoteNodes();

            BytesExternalizableImpl bytesExternalizable = new BytesExternalizableImpl(remoteNodes, null);


            BytesExternalizableImpl.ProposedIdentifierWithHost proposedIdentifierWithHost = new BytesExternalizableImpl
                    .ProposedIdentifierWithHost(server1AddressAndPort, (byte) -1);

            bytesExternalizable.sendBootStrap(proposedIdentifierWithHost);

            bytesExternalizable.writeMarshallable(udpData);

        }


        {

            RemoteNodes remoteNodes = new RemoteNodes();


            UDPEventListener udpEventListener = new UDPEventListener() {

                @Override
                public void onRemoteNodeEvent(RemoteNodes remoteNode, ConcurrentExpiryMap<AddressAndPort, BytesExternalizableImpl.ProposedIdentifierWithHost> proposedIdentifiersWithHost) {
                    int i = 1;
                }
            };
            BytesMarshallable bytesExternalizable = new BytesExternalizableImpl(remoteNodes, udpEventListener);


            udpData.flip();
            bytesExternalizable.readMarshallable(udpData);
        }
    }


}
