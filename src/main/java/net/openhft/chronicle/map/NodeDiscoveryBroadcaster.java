package net.openhft.chronicle.map;

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.map.BytesExternalizableImpl.ProposedIdentifierWithHost;
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

        final UdpSocketChannelEntryWriter writer = new UdpSocketChannelEntryWriter(1024, externalizable, this);
        final UdpSocketChannelEntryReader reader = new UdpSocketChannelEntryReader(1024, externalizable);

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
    private final ConcurrentSkipListSet<AddressAndPort> addressAndPorts;
    private final ATSDirectBitSet atsDirectBitSet;


    /**
     * @param activeIdentifiersBitSetBytes byte sof a bitset containing the known identifiers
     */
    RemoteNodes(final Bytes activeIdentifiersBitSetBytes) {

        this.addressAndPorts = new ConcurrentSkipListSet<AddressAndPort>();

        this.activeIdentifiersBitSetBytes = activeIdentifiersBitSetBytes;
        this.atsDirectBitSet = new ATSDirectBitSet(this.activeIdentifiersBitSetBytes);
    }

    public Set<AddressAndPort> addressAndPorts() {
        return addressAndPorts;
    }

    public Bytes activeIdentifierBytes() {
        return activeIdentifiersBitSetBytes;
    }

    public void add(AddressAndPort inetSocketAddress) {
        addressAndPorts.add(inetSocketAddress);
    }

    public DirectBitSet activeIdentifierBitSet() {
        return atsDirectBitSet;
    }


}


class AddressAndPort implements Comparable<AddressAndPort> {
    private byte[] address;
    private short port;

    public AddressAndPort(byte[] address, short port) {
        if (address.length != 4)
            throw new IllegalStateException("address.length should equal '4' but is length=" +
                    address.length);
        this.address = address;
        this.port = port;
    }

    public byte[] getAddress() {
        return address;
    }

    public short getPort() {
        return port;
    }

    public byte[] address() {

        return address;
    }

    public short port() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddressAndPort that = (AddressAndPort) o;

        if (port != that.port) return false;
        if (!Arrays.equals(address, that.address)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(address);
        result = 31 * result + (int) port;
        return result;
    }


    @Override
    public int compareTo(AddressAndPort o) {
        int i = 0;
        for (byte b : address) {
            int compare = Byte.compare(b, o.address[i++]);
            if (compare != 0)
                return compare;

        }
        return Short.compare(port, o.port);
    }


    @Override
    public String toString() {
        return "AddressAndPort{" +
                "address=" + numericToTextFormat(address) +
                ", port=" + port +
                '}';
    }

    static String numericToTextFormat(byte[] src) {
        return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + (src[2] & 0xff) + "." + (src[3] & 0xff);
    }


}


class BytesExternalizableImpl implements BytesExternalizable {

    private final RemoteNodes allNodes;

    private final AtomicBoolean bootstrapRequired = new AtomicBoolean();
    private final UDPEventListener udpEventListener;

    private ProposedIdentifierWithHost ourProposedIdentifier;

    public ProposedIdentifierWithHost getOurProposedIdentifier() {
        return ourProposedIdentifier;
    }

    public void setOurProposedIdentifier(ProposedIdentifierWithHost ourProposedIdentifier) {
        this.ourProposedIdentifier = ourProposedIdentifier;
    }

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

            final ProposedIdentifierWithHost ourProposedIdentifier = getOurProposedIdentifier();
            if (ourProposedIdentifier == null || ourProposedIdentifier.addressAndPort == null)
                return;

            BOOTSTRAP_BYTES.clear();
            destination.write(BOOTSTRAP_BYTES);


            for (byte b : ourProposedIdentifier.addressAndPort.address()) {
                destination.writeByte(b);
            }

            destination.writeShort(ourProposedIdentifier.addressAndPort.port());
            destination.writeByte(ourProposedIdentifier.identifier());
            return;
        }

        final Set<AddressAndPort> addressAndPorts = allNodes.addressAndPorts();

        // write the number of hosts and ports
        short count = (short) addressAndPorts.size();
        destination.writeUnsignedShort(count);

        // write all the host and ports
        for (AddressAndPort inetSocketAddress : addressAndPorts) {
            for (byte b : inetSocketAddress.address()) {
                destination.writeByte(b);
            }

            destination.writeShort(inetSocketAddress.port());
        }

        //   destination.write(allNodes.ourProposedID());

        Bytes bytes = allNodes.activeIdentifierBytes();
        //bytes.position(0);

        toString(allNodes.activeIdentifierBitSet(), "writing");

        // we will store the size of the bitset
        destination.writeUnsignedShort((int) bytes.remaining());

        // we are no going to broadcast all the nodes that have been bootstaping in the last 200
        expireOldBootstrapStats(System.currentTimeMillis() - 200);

        // this is where we will store the count of the bootstrap data which is written below
        destination.skip(2);
        long countLocation = destination.position();
        int countBootstrapDataRepublished = 0;

        // rebroadcast all the host:port:identifier of the nodes that have sent a bootstrap in the last 200
        // milliseconds
        for (ProposedIdentifierWithHost proposedIdentifierWithHost : bootstrapStatsMap.values()) {

            for (byte b : proposedIdentifierWithHost.addressAndPort.address()) {
                destination.writeByte(b);
            }

            destination.writeShort(proposedIdentifierWithHost.addressAndPort.port());
            destination.writeByte(proposedIdentifierWithHost.identifier());
            countBootstrapDataRepublished++;
        }

        destination.writeInt(countLocation, countBootstrapDataRepublished);

    }

    final ConcurrentMap<AddressAndPort, ProposedIdentifierWithHost> bootstrapStatsMap = new
            ConcurrentHashMap<AddressAndPort, ProposedIdentifierWithHost>();

    final Queue<ProposedIdentifierWithHost> proposedIdentifierWithHostExpiryQueue = new ConcurrentLinkedQueue<ProposedIdentifierWithHost>();


    private void expireOldBootstrapStats(long timeOlderThan) {
        for (; ; ) {

            final ProposedIdentifierWithHost proposedIdentifierWithHost = proposedIdentifierWithHostExpiryQueue.peek();

            if (proposedIdentifierWithHost == null)
                break;

            if (proposedIdentifierWithHost.timestamp < timeOlderThan) {
                // only remote it if it has not changed
                bootstrapStatsMap.remove(proposedIdentifierWithHost.addressAndPort, proposedIdentifierWithHost);
            }

            proposedIdentifierWithHostExpiryQueue.poll();
        }
    }


    @Override
    public void readExternalBytes(@NotNull Bytes source) {

        final ProposedIdentifierWithHost bootstrap = readBootstrapMessage(source);
        if (bootstrap != null) {
            LOG.debug("received Bootstrap");

            bootstrapStatsMap.put(bootstrap.addressAndPort, bootstrap);
            proposedIdentifierWithHostExpiryQueue.add(bootstrap);


            try {

                // we've received a bootstrap message so will will now rebroadcast what we know,
                // after a random delay

                Thread.sleep((int) (Math.random() * 9.0));

                onChange();

            } catch (InterruptedException e) {
                LOG.error("", e);
            }

            return;
        }

        int count = source.readUnsignedShort();


        final Set<AddressAndPort> allNodeInTheGrid = new HashSet<AddressAndPort>();

        for (int i = 0; i < count; i++) {
            byte[] address = new byte[4];
            address[0] = source.readByte();
            address[1] = source.readByte();
            address[2] = source.readByte();
            address[3] = source.readByte();

            final short port = source.readShort();

            LOG.debug("received InetSocketAddress(" + address + "," + port + ")");
            allNodeInTheGrid.add(new AddressAndPort(address, port));
        }


        int countRebroadcast = source.readShort();

        final Set<ProposedIdentifierWithHost> proposedIdentifierWithHosts = new
                HashSet<ProposedIdentifierWithHost>();

        // rebroadcast all the host:port:identifier of the nodes that have sent a bootstrap in the last 200
        // milliseconds
        for (int i = 0; i < countRebroadcast; i++) {
            byte[] address = new byte[4];
            address[0] = source.readByte();
            address[1] = source.readByte();
            address[2] = source.readByte();
            address[3] = source.readByte();

            final short port = source.readShort();
            final byte identifier = source.readByte();

            final ProposedIdentifierWithHost proposed = new ProposedIdentifierWithHost(
                    address, port, identifier);

            proposedIdentifierWithHosts.add(proposed);
        }

        // the number of bytes in the bitset
        int sizeInBytes = source.readUnsignedShort();

        if (sizeInBytes == 0) {
            LOG.debug("received nothing..");
            return;
        }


        //todo we should not have to do this !  -  maybe a bug in BitSet of ByteBuffer
        Bytes usedIdentifiers = toNewBuffer(source);

        udpEventListener.onRemoteNodeEvent(allNodeInTheGrid,
                new ATSDirectBitSet(usedIdentifiers),
                proposedIdentifierWithHosts);

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


    public static class ProposedIdentifierWithHost {

        private final int identifier;
        private final long timestamp;
        private final AddressAndPort addressAndPort;

        public ProposedIdentifierWithHost(@NotNull final AddressAndPort addressAndPort,
                                          byte identifier) {
            this.addressAndPort = addressAndPort;
            this.identifier = identifier;
            this.timestamp = System.currentTimeMillis();
        }

        public ProposedIdentifierWithHost(byte[] address, short port, byte identifier) {
            this(new AddressAndPort(address, port), identifier);
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ProposedIdentifierWithHost that = (ProposedIdentifierWithHost) o;

            if (identifier != that.identifier) return false;
            if (timestamp != that.timestamp) return false;
            if (addressAndPort != null ? !addressAndPort.equals(that.addressAndPort) : that.addressAndPort != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = identifier;
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (addressAndPort != null ? addressAndPort.hashCode() : 0);
            return result;
        }

        public int identifier() {
            return identifier;

        }

        public AddressAndPort addressAndPort() {
            return addressAndPort;
        }
    }

    /**
     * @param source
     * @return returns true if the UDP message contains the text 'BOOTSTRAP'
     */
    private ProposedIdentifierWithHost readBootstrapMessage(Bytes source) {

        final long start = source.position();

        try {

            long size = BOOTSTRAP_BYTES.limit();

            if (size < source.remaining())
                return null;

            for (int i = 0; i < size; i++) {

                final byte byteRead = source.readByte(start + i);
                final byte expectedByte = BOOTSTRAP_BYTES.readByte(i);

                if (!(expectedByte == byteRead))
                    return null;
            }

            byte[] address = new byte[4];
            address[0] = source.readByte();
            address[1] = source.readByte();
            address[2] = source.readByte();
            address[3] = source.readByte();

            final short port = source.readShort();
            final byte identifier = source.readByte();

            return new ProposedIdentifierWithHost(address, port, identifier);

        } finally {
            source.position(start);
        }
    }


    public void sendBootStrap() {
        bootstrapRequired.set(true);
    }

    public void add(AddressAndPort addressAndPort) {
        allNodes.add(addressAndPort);
    }


    public void add(byte identifier) {
        allNodes.activeIdentifierBitSet().set(identifier);
    }

}

interface UDPEventListener {

    /**
     * called when we have received a UDP message, this is called after the message has been parsed
     */

    void onRemoteNodeEvent(Set<AddressAndPort> usedHostPorts,
                           ATSDirectBitSet usedIdentifers,
                           Set<ProposedIdentifierWithHost> proposedHostPortIdentifiers);
}
