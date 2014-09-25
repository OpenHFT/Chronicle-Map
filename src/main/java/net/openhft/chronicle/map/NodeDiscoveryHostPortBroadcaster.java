package net.openhft.chronicle.map;

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Rob Austin.
 */
public class NodeDiscoveryHostPortBroadcaster extends UdpChannelReplicator {

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
     * @throws java.io.IOException
     */
    NodeDiscoveryHostPortBroadcaster(
            @NotNull final UdpReplicationConfig replicationConfig,
            final int serializedEntrySize,
            RemoteNodes remoteNodes)
            throws IOException {

        super(replicationConfig, serializedEntrySize, UNUSED);


        BytesExternalizable externalizable = new BytesExternalizableImpl(remoteNodes, this);

        setReader(new UdpSocketChannelEntryReader(serializedEntrySize, externalizable));

        setWriter(new UdpSocketChannelEntryWriter(serializedEntrySize, externalizable, this));

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
        UdpSocketChannelEntryReader(final int serializedEntrySize,
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

    private static class UdpSocketChannelEntryWriter implements EntryWriter {


        private final ByteBuffer out;
        private final ByteBufferBytes in;

        private final BytesExternalizable externalizable;
        private UdpChannelReplicator udpReplicator;

        UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final BytesExternalizable externalizable,
                                    @NotNull final UdpChannelReplicator udpReplicator) {
            this.udpReplicator = udpReplicator;

            // we make the buffer twice as large just to give ourselves headroom
            out = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);

            // write bootstrap to the 'in' buffer
            {
                // skip 2 bytes where we write the size
                in.skip(SIZE_OF_SHORT);

                in.write(BOOTSTRAP_BYTES);

                // now write the size at the start
                in.writeShort(0, (int) in.position() - SIZE_OF_SHORT);

            }

            this.externalizable = externalizable;
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
            in.skip(SIZE_OF_SHORT);

            // we'll write the size inverted at the start
            in.writeShort(0, ~(in.readUnsignedShort(SIZE_OF_SHORT)));
            out.limit((int) in.position());

            int start = out.position();

            // writes the contents of
            externalizable.writeExternalBytes(in);
            udpReplicator.disableWrites();
            // number of bytes written
            return out.position() - start;


        }
    }


}

/**
 * supports reading and writing serialize entries
 */
interface BytesExternalizable {

    /**
     * The map implements this method to save its contents.
     *
     * @param destination a buffer the entry will be written to, the segment may reject this operation and add
     *                    zeroBytes, if the identifier in the entry did not match the maps local
     */
    void writeExternalBytes(@NotNull Bytes destination);

    /**
     * The map implements this method to restore its contents. This method must read the values in the same
     * sequence and with the same types as were written by {@code writeExternalEntry()}. This method is
     * typically called when we receive a remote replication event, this event could originate from either a
     * remote {@code put(K key, V value)} or {@code remove(Object key)}
     *
     * @param source bytes to read an entry from
     */
    void readExternalBytes(@NotNull Bytes source);


}

class RemoteNodes {


    private final Bytes activeIdentifiersBitSetBytes;
    private Set<InetSocketAddress> inetSocketAddresses;
    private final ATSDirectBitSet atsDirectBitSet;


    /**
     * @param inetSocketAddresses          the host and ports which we should connect to for TCP replication
     * @param activeIdentifiersBitSetBytes byte sof a bitset containing the known identifiers
     */
    RemoteNodes(final Bytes activeIdentifiersBitSetBytes) {
        this.inetSocketAddresses = new HashSet<InetSocketAddress>();
        this.activeIdentifiersBitSetBytes = activeIdentifiersBitSetBytes;
        this.atsDirectBitSet = new ATSDirectBitSet(this.activeIdentifiersBitSetBytes);
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

    public ATSDirectBitSet activeIdentifierBitSet() {
        return atsDirectBitSet;
    }
}

class BytesExternalizableImpl implements BytesExternalizable {

    private final RemoteNodes allNodes;
    private final Replica.ModificationNotifier modificationNotifier;
    //private final InetSocketAddress localInetSocketAddress;

    public BytesExternalizableImpl(final RemoteNodes allNodes,
                                   final Replica.ModificationNotifier modificationNotifier) {
        this.allNodes = allNodes;
        this.modificationNotifier = modificationNotifier;
        // todo this should only be added once we have connected - we will add our host and port to allNodes
        //this.localInetSocketAddress = new InetSocketAddress(localHost, localPort);
        //this.allNodes.add(localInetSocketAddress);

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

        final Set<InetSocketAddress> inetSocketAddresses = allNodes.inetSocketAddresses();

        // write the number of hosts and ports
        destination.write((short) inetSocketAddresses.size());

        // write all the host and ports
        for (InetSocketAddress inetSocketAddress : inetSocketAddresses) {
            destination.writeUTF(inetSocketAddress.getHostName());
            destination.writeInt(inetSocketAddress.getPort());
        }

        destination.write(allNodes.activeIdentifierBytes());
    }

    @Override
    public void readExternalBytes(@NotNull Bytes source) {

        if (source.equals(NodeDiscoveryHostPortBroadcaster.BOOTSTRAP_BYTES)) {
            // used to wake up the OP_WRITE
            modificationNotifier.onChange();
            return;
        }

        // the number of host and ports
        int count = source.readShort();

        for (int i = 0; i < count; i++) {

            final String host = source.readUTF();
            final int port = source.readInt();

            allNodes.add(new InetSocketAddress(host, port));
        }

        int identifierBitSetBytes = source.readShort();
        source.limit(source.position() + identifierBitSetBytes);

        final ATSDirectBitSet sourceBitSet = new ATSDirectBitSet(source);

        final ATSDirectBitSet resultBitSet = allNodes.activeIdentifierBitSet();

        // merges the source into the destination and returns the destination
        orBitSets(sourceBitSet, resultBitSet);


    }

    /**
     * merges the source into the destination and returns the destination
     *
     * @param source
     * @param destination
     * @return
     */
    private ATSDirectBitSet orBitSets(ATSDirectBitSet source, ATSDirectBitSet destination) {
        // merge the two bit-sets together via 'or'

        long numberOfLongs = source.size() / 8;
        for (int longIndex = 1; longIndex < numberOfLongs; longIndex++) {
            destination.or(longIndex, source.getLong(longIndex));
        }

        return destination;
    }


}