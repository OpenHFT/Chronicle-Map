package net.openhft.chronicle.map;

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.map.NodeDiscoveryBroadcaster.BOOTSTRAP_BYTES;
import static net.openhft.chronicle.map.NodeDiscoveryBroadcaster.LOG;

/**
 * Broad cast the nodes host ports and identifiers over UDP, to make it easy to join a grid of remote nodes
 * just by name, this functionality requires UDP
 *
 * @author Rob Austin.
 */
public class NodeDiscoveryBroadcaster extends UdpChannelReplicator {

    public static final Logger LOG = LoggerFactory.getLogger(NodeDiscoveryBroadcaster.class.getName
            ());

    private static final byte UNUSED = (byte) -1;

    static final ByteBufferBytes BOOTSTRAP_BYTES;

    static {
        final String BOOTSTRAP = "BOOTSTRAP";
        BOOTSTRAP_BYTES = new ByteBufferBytes(ByteBuffer.allocate(2 + BOOTSTRAP.length()));
        BOOTSTRAP_BYTES.write((short) BOOTSTRAP.length());
        BOOTSTRAP_BYTES.append(BOOTSTRAP);
    }


    /**
     * @param replicationConfig
     * @param externalizable
     * @throws java.io.IOException
     */
    NodeDiscoveryBroadcaster(
            @NotNull final UdpReplicationConfig replicationConfig,
            final int serializedEntrySize,
            final BytesExternalizable externalizable)
            throws IOException {

        super(replicationConfig, serializedEntrySize, UNUSED);


        final UdpSocketChannelEntryWriter writer = new UdpSocketChannelEntryWriter(1024, externalizable,
                this);
        final UdpSocketChannelEntryReader reader = new UdpSocketChannelEntryReader(1024, externalizable
        );


        setReader(reader);

        setWriter(writer);

        start();
    }

    private static class UdpSocketChannelEntryReader implements EntryReader {


        private final ByteBuffer in;
        private final ByteBufferBytes out;
        private final BytesExternalizable externalizable;


        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         */
        public UdpSocketChannelEntryReader(final int serializedEntrySize,
                                           @NotNull final BytesExternalizable externalizable) {

            // we make the buffer twice as large just to give ourselves headroom
            in = ByteBuffer.allocateDirect(serializedEntrySize * 2);

            out = new ByteBufferBytes(in);
            out.limit(0);
            in.clear();
            this.externalizable = externalizable;
        }

        /**
         * reads entries from the socket till it is empty
         *
         * @param socketChannel the socketChannel that we will read from
         * @throws IOException
         * @throws InterruptedException
         */
        public void readAll(@NotNull final DatagramChannel socketChannel) throws IOException,
                InterruptedException {

            out.clear();
            in.clear();

            socketChannel.receive(in);

            final int bytesRead = in.position();

            if (bytesRead < SIZE_OF_SHORT + SIZE_OF_SHORT)
                return;

            out.limit(in.position());

            final short invertedSize = out.readShort();
            final int size = out.readUnsignedShort();

            // check the the first 4 bytes are the inverted len followed by the len
            // we do this to check that this is a valid start of entry, otherwise we throw it away
            if (((short) ~size) != invertedSize)
                return;

            if (out.remaining() != size)
                return;

            externalizable.readExternalBytes(out);
        }


    }

    public static class UdpSocketChannelEntryWriter implements EntryWriter {


        private final ByteBuffer out;
        private final ByteBufferBytes in;


        @NotNull
        private final BytesExternalizable externalizable;
        private UdpChannelReplicator udpReplicator;

        public UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                           @NotNull final BytesExternalizable externalizable,
                                           @NotNull final UdpChannelReplicator udpReplicator) {

            this.externalizable = externalizable;
            this.udpReplicator = udpReplicator;

            // we make the buffer twice as large just to give ourselves headroom
            out = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);


        }


        /**
         * writes all the entries that have changed, to the tcp socket
         *
         * @param socketChannel
         * @param modificationIterator
         * @throws InterruptedException
         * @throws java.io.IOException
         */

        /**
         * update that are throttled are rejected.
         *
         * @param socketChannel the socketChannel that we will write to
         * @throws InterruptedException
         * @throws IOException
         */
        public int writeAll(@NotNull final DatagramChannel socketChannel)
                throws InterruptedException, IOException {

            out.clear();
            in.clear();

            // skip the size inverted
            in.skip(SIZE_OF_SHORT);

            // skip the size
            in.skip(SIZE_OF_SHORT);

            long start = in.position();

            // writes the contents of
            externalizable.writeExternalBytes(in);

            long size = in.position() - start;


            // we'll write the size inverted at the start
            in.writeShort(0, ~((int) size));
            in.writeUnsignedShort(SIZE_OF_SHORT, (int) size);


            out.limit((int) in.position());

            udpReplicator.disableWrites();

            return socketChannel.write(out);


        }
    }

}


/**
 * supports reading and writing serialize entries
 */
interface BytesExternalizable {


    void writeExternalBytes(@NotNull Bytes destination);


    public void readExternalBytes(@NotNull Bytes source);

}

class RemoteNodes {


    private final Bytes activeIdentifiersBitSetBytes;
    private ConcurrentSkipListSet inetSocketAddresses;
    private final ATSDirectBitSet atsDirectBitSet;
    private byte ourProposedID = -1;
    private final ATSDirectBitSet allProposedID;

    /**
     * @param activeIdentifiersBitSetBytes byte sof a bitset containing the known identifiers
     */
    RemoteNodes(final Bytes activeIdentifiersBitSetBytes) {

        this.inetSocketAddresses = new ConcurrentSkipListSet<InetSocketAddress>(new Comparator<InetSocketAddress>() {

            @Override
            public int compare(InetSocketAddress o1, InetSocketAddress o2) {

                int result = Integer.compare(o1.getPort(), o2.getPort());
                if (result != 0)
                    return result;

                return o1.getHostName().compareTo(o2.getHostName());

            }
        });

        this.activeIdentifiersBitSetBytes = activeIdentifiersBitSetBytes;
        this.atsDirectBitSet = new ATSDirectBitSet(this.activeIdentifiersBitSetBytes);

        // this will only hold bits in it while we in the phase of searching for an id
        this.allProposedID = new ATSDirectBitSet(new ByteBufferBytes(ByteBuffer.allocate(128 / 8)));
    }


    public Set<InetSocketAddress> inetSocketAddresses() {
        return inetSocketAddresses;
    }

    public Bytes activeIdentifierBytes() {
        return activeIdentifiersBitSetBytes;
    }

    public void add(InetSocketAddress inetSocketAddress) {
        inetSocketAddresses.add(inetSocketAddress);
    }

    public DirectBitSet activeIdentifierBitSet() {
        return atsDirectBitSet;
    }


    public byte ourProposedID() {
        return ourProposedID;
    }

    public DirectBitSet allProposedID() {
        return allProposedID;
    }
}

class BytesExternalizableImpl implements BytesExternalizable {

    private final RemoteNodes allNodes;

    private final AtomicBoolean bootstrapRequired = new AtomicBoolean();
    private final UDPEventListener udpEventListener;

    public void setModificationNotifier(Replica.ModificationNotifier modificationNotifier) {
        this.modificationNotifier = modificationNotifier;

    }

    Replica.ModificationNotifier modificationNotifier;

    public BytesExternalizableImpl(final RemoteNodes allNodes, UDPEventListener udpEventListener) {
        this.allNodes = allNodes;
        this.udpEventListener = udpEventListener;
    }

    /**
     * this is used to tell nodes that are connecting to us which host and ports are in our grid, along with
     * all the identifiers.
     *
     * @param destination a buffer the entry will be written to, the segment may reject this operation and add
     *                    zeroBytes, if the identifier in the entry did not match the maps local
     */
    @Override
    public void writeExternalBytes(@NotNull Bytes destination) {


        if (bootstrapRequired.getAndSet(false)) {
            BOOTSTRAP_BYTES.clear();
            destination.write(BOOTSTRAP_BYTES);
            return;
        }

        final Set<InetSocketAddress> inetSocketAddresses = allNodes.inetSocketAddresses();

        // write the number of hosts and ports
        short count = (short) inetSocketAddresses.size();
        destination.writeUnsignedShort(count);

        // write all the host and ports
        for (InetSocketAddress inetSocketAddress : inetSocketAddresses) {
            destination.writeUTF(inetSocketAddress.getHostName());
            destination.writeInt(inetSocketAddress.getPort());
        }

        destination.write(allNodes.ourProposedID());

        Bytes bytes = allNodes.activeIdentifierBytes();
        //bytes.position(0);

        toString(allNodes.activeIdentifierBitSet(), "writing");

        // we will store the size of the bitset
        destination.writeUnsignedShort((int) bytes.remaining());

        // then write it out
        destination.write(bytes);
    }

    @Override
    public void readExternalBytes(@NotNull Bytes source) {

        if (isBootstrap(source)) {
            LOG.debug("received Bootstrap");

            // we've received a bootstrap message so will will now rebroadcast what we know
            onChange();

            return;
        }

        int count = source.readUnsignedShort();

        for (int i = 0; i < count; i++) {

            final String host = source.readUTF();
            final int port = source.readInt();

            LOG.debug("received InetSocketAddress(" + host + "," + port + ")");
            allNodes.add(new InetSocketAddress(host, port));
        }


        byte remoteProposedId = source.readByte();


        // the number of bytes in the bitset
        int sizeInBytes = source.readUnsignedShort();

        if (sizeInBytes == 0) {
            LOG.debug("received nothing..");
            return;
        }

        //todo we should not have to do this !  -  maybe a bug in BitSet of ByteBuffer
        Bytes sourceBytes = toNewBuffer(source);

        // merges the source into the destination and returns the destination
        orBitSets(new ATSDirectBitSet(sourceBytes), allNodes.activeIdentifierBitSet());


        udpEventListener.onRemoteNodeEvent(allNodes, remoteProposedId);
    }


    /**
     * we shoudl have to do this, but I think there is a bug in the bitset, so added it as a work around
     *
     * @param source
     * @return
     */
    private Bytes toNewBuffer(Bytes source) {

        ByteBufferBytes result = new ByteBufferBytes(ByteBuffer.allocate((int) source.remaining()));

        result.write(source);
        result.clear();
        return result;

    }

    private void toString(DirectBitSet bitSet, String type) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bitSet.size(); i++) {
            builder.append(bitSet.get(i) ? '1' : '0');
        }

        LOG.debug(type + "=" + "bitset =" + builder);
    }

    public void onChange() {
        modificationNotifier.onChange();
    }


    /**
     * @param source
     * @return returns true if the UDP message contains the text 'BOOTSTRAP'
     */
    private boolean isBootstrap(Bytes source) {

        final long start = source.position();

        try {

            long size = BOOTSTRAP_BYTES.limit();

            if (size < source.remaining())
                return false;

            if (source.remaining() < size)
                return false;

            for (int i = 0; i < size; i++) {

                final byte actualByte = source.readByte(start + i);
                final byte expectedByte = BOOTSTRAP_BYTES.readByte(i);

                if (!(expectedByte == actualByte))
                    return false;
            }

            return true;
        } finally {
            source.position(start);
        }
    }

    /**
     * bitwise OR's the two bit sets or put another way, merges the source bitset into the destination bitset
     * and returns the destination
     *
     * @param source
     * @param destination
     * @return
     */
    private DirectBitSet orBitSets(@NotNull final DirectBitSet source,
                                   @NotNull final DirectBitSet destination) {


        // merges the two bit-sets together
        for (int i = (int) source.nextSetBit(0); i > 0;
             i = (int) source.nextSetBit(i + 1)) {
            try {
                destination.set(i, true);
            } catch (IndexOutOfBoundsException e) {
                LOG.error("", e);
            }
        }

        return destination;
    }


    public void sendBootStrap() {
        bootstrapRequired.set(true);
    }

    public void add(InetSocketAddress interfaceAddress) {
        allNodes.add(interfaceAddress);
    }


    public void add(byte identifier) {
        allNodes.activeIdentifierBitSet().set(identifier);
    }

}

interface UDPEventListener {

    /**
     * called when we have received a UDP message, this is called after the message has been parsed
     *
     * @param allNodes
     * @param remoteProposedId a remote node has proposed using this as an id
     */

    void onRemoteNodeEvent(RemoteNodes allNodes, byte remoteProposedId);
}
