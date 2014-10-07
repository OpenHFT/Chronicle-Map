package net.openhft.chronicle.map;

import junit.framework.TestCase;
import net.openhft.lang.io.ByteBufferBytes;
import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.ByteBuffer.allocate;

public class NodeDiscoveryBroadcasterTest extends TestCase {

    public static final int SERVER2_IDENTIFER = 10;
    public static final int PROPOSED_IDENTIFIER = 5;


    @Test
    public void testsTheSerializationOfTheNodeDiscovery() throws IOException, InterruptedException {


        AtomicReference ref = new AtomicReference();
        final AddressAndPort ourAddressAndPort1 = new AddressAndPort(InetAddress.getLocalHost()
                .getAddress(),
                (short) 1024);

        final AddressAndPort ourAddressAndPort2 = new AddressAndPort(InetAddress.getLocalHost()
                .getAddress(),
                (short) 1025);

        // write broadcast our address on the bootstrap method
        byte[] server1Address = Inet4Address.getLocalHost().getAddress();
        AddressAndPort server1AddressAndPort = new AddressAndPort(server1Address, (short) 1234);

        // write broadcast our address on the bootstrap method
        byte[] server2Address = Inet4Address.getByName("129.168.0.2").getAddress();
        AddressAndPort server2AddressAndPort = new AddressAndPort(server1Address, (short) 1234);

        KnownNodes server1KnownNodes = new KnownNodes();
        DiscoveryNodeBytesMarshallable server1BytesExternalizable = new DiscoveryNodeBytesMarshallable
                (server1KnownNodes, ref, ourAddressAndPort1);

        // for the unit test we are using a ByteBufferBytes, but iltimately this data would have been send
        // and received via UDP
        ByteBufferBytes udpData = new ByteBufferBytes(allocate(1024));

        // SERER1 - is the new node join the cluster
        // we will first send the bootstrap along with our host and port
        {
            final DiscoveryNodeBytesMarshallable.ProposedNodes proposedNodes = new
                    DiscoveryNodeBytesMarshallable.ProposedNodes(server1AddressAndPort, (byte) -1);
            server1BytesExternalizable.sendBootStrap(proposedNodes);
            server1BytesExternalizable.writeMarshallable(udpData);
        }

        KnownNodes server2KnownNodes = new KnownNodes();
        server2KnownNodes.add(server2AddressAndPort, (byte) SERVER2_IDENTIFER);
        DiscoveryNodeBytesMarshallable server2BytesExternalizable = new DiscoveryNodeBytesMarshallable
                (server2KnownNodes, ref, ourAddressAndPort2);

        // add our identifier and host:port to the list of known identifiers


        // SERVER 2 - is the node already in the cluster
        // read the bootstrap along with the host and port it came from
        {
            udpData.flip();
            server2BytesExternalizable.readMarshallable(udpData);

            Assert.assertTrue(server2BytesExternalizable.proposedIdentifiersWithHost.values().contains
                    (new DiscoveryNodeBytesMarshallable.ProposedNodes(server1AddressAndPort, (byte) -1)));
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

            Assert.assertTrue(server1BytesExternalizable.getRemoteNodes().identifiers().get
                    (SERVER2_IDENTIFER));

        }

        // SERVER 1
        // send/broadcast - propose an identifier to use, by choosing one that is not used already
        // for the purpose of this test we wont choose one at random ( as the code does ) but just pick
        // identifier=5

        {
            udpData.clear();

            KnownNodes knownNodes = new KnownNodes();

            DiscoveryNodeBytesMarshallable bytesExternalizable = new DiscoveryNodeBytesMarshallable
                    (knownNodes, ref, ourAddressAndPort1);

            DiscoveryNodeBytesMarshallable.ProposedNodes proposedNodes = new DiscoveryNodeBytesMarshallable.ProposedNodes(server1AddressAndPort, (byte) PROPOSED_IDENTIFIER);

            // send the bootstrap along with this newly proposed identifier
            bytesExternalizable.sendBootStrap(proposedNodes);

            bytesExternalizable.writeMarshallable(udpData);

        }

        // SERVER 2 - is the node already in the cluster
        // read the bootstrap along with the proposed identifier and host:port it came from
        {
            udpData.flip();
            server2BytesExternalizable.readMarshallable(udpData);

            Assert.assertTrue(server2BytesExternalizable.proposedIdentifiersWithHost.values().contains
                    (new DiscoveryNodeBytesMarshallable.ProposedNodes(server1AddressAndPort, (byte) PROPOSED_IDENTIFIER)));
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


        // SERVER 1
        // read the bootstrap along with all the proposed identifiers ( this will include our
        // PROPOSED_IDENTIFIER and any that come from other server
        {
            udpData.flip();
            server1BytesExternalizable.readMarshallable(udpData);

            Assert.assertTrue(server1BytesExternalizable.getRemoteNodes().addressAndPorts().contains
                    (server2AddressAndPort));

            Assert.assertTrue(server1BytesExternalizable.getRemoteNodes().identifiers().get
                    (SERVER2_IDENTIFER));

            Assert.assertTrue(server1BytesExternalizable.proposedIdentifiersWithHost.values().contains
                    (new DiscoveryNodeBytesMarshallable.ProposedNodes(server1AddressAndPort, (byte) PROPOSED_IDENTIFIER)));

        }


    }


}
