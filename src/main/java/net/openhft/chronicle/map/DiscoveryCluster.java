package net.openhft.chronicle.map;

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;

import java.io.IOException;
import java.net.Inet4Address;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;

/**
 * @author Rob Austin.
 */
public class DiscoveryCluster implements UDPEventListener {

    public static final int PORT = 1235;

    DirectBitSet bitSet = newBitSet(128);

    public void discover() throws IOException, InterruptedException {

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), PORT);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);

        BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes, this);

        final NodeDiscoveryBroadcaster nodeDiscoveryBroadcaster
                = new NodeDiscoveryBroadcaster(udpConfig, 1024, externalizable);

        externalizable.setModificationNotifier(nodeDiscoveryBroadcaster);
        externalizable.sendBootStrap();
        Thread.sleep(100);
        externalizable.sendBootStrap();
        // we have to wait a while, we have not way of knowing how long it will take for a UDP message to
        // arrive, but the more UDP messages we receive the better, is it will make our selection of a
        // unique identifier more reliable.
        Thread.sleep(100);


    }

    @Override
    public void onRemoteNodeEvent(RemoteNodes allNodes, byte remoteProposedId) {

        if (remoteProposedId == -1)
            bitSet.set(remoteProposedId);

        // propose and identifier
    }



    /**
     * creates a bit set based on a number of bits
     *
     * @param numberOfBits the number of bits the bit set should include
     * @return a new DirectBitSet backed by a byteBuffer
     */
    private static DirectBitSet newBitSet(int numberOfBits) {
        final ByteBufferBytes bytes = new ByteBufferBytes(wrap(new byte[(numberOfBits + 7) / 8]));
        return new ATSDirectBitSet(bytes);
    }

}
