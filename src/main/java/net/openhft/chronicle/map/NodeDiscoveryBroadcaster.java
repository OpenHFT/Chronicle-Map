package net.openhft.chronicle.map;

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.*;
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
     * we shoudl have to do this, but I think there is a bug in the bitset, so added it as a work around
     *
     * @param source
     * @return
     */
    static Bytes toNewBuffer(Bytes source) {

        ByteBufferBytes result = new ByteBufferBytes(ByteBuffer.allocate((int) source.remaining()));

        result.write(source);
        result.clear();
        return result;

    }

    static String toString(DirectBitSet bitSet) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bitSet.size(); i++) {
            builder.append(bitSet.get(i) ? '1' : '0');
        }
        return builder.toString();

        // LOG.debug(type + "=" + "bitset =" + builder);
    }

    /**
     * @param replicationConfig
     * @param externalizable
     * @throws java.io.IOException
     */
    NodeDiscoveryBroadcaster(
            @NotNull final UdpReplicationConfig replicationConfig,
            final int serializedEntrySize,
            final BytesMarshallable externalizable)
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
        private final BytesMarshallable externalizable;

        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         */
        public UdpSocketChannelEntryReader(final int serializedEntrySize,
                                           @NotNull final BytesMarshallable externalizable) {

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

            externalizable.readMarshallable(out);
        }


    }

    public static class UdpSocketChannelEntryWriter implements EntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;

        @NotNull
        private final BytesMarshallable externalizable;
        private UdpChannelReplicator udpReplicator;

        public UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                           @NotNull final BytesMarshallable externalizable,
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
            externalizable.writeMarshallable(in);

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


class RemoteNodes implements BytesMarshallable {

    private Bytes activeIdentifiersBitSetBytes;
    private ConcurrentSkipListSet<AddressAndPort> addressAndPorts = new ConcurrentSkipListSet<AddressAndPort>();
    private ATSDirectBitSet atsDirectBitSet;

    RemoteNodes() {

        this.activeIdentifiersBitSetBytes = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));
        this.addressAndPorts = new ConcurrentSkipListSet<AddressAndPort>();
        this.atsDirectBitSet = new ATSDirectBitSet(this.activeIdentifiersBitSetBytes);
    }


    public Set<AddressAndPort> addressAndPorts() {
        return addressAndPorts;
    }

    public void add(AddressAndPort inetSocketAddress, byte identifier) {
        activeIdentifierBitSet().set(identifier);
        addressAndPorts.add(inetSocketAddress);
    }

    public DirectBitSet activeIdentifierBitSet() {
        return atsDirectBitSet;
    }


    @Override
    public void readMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes in) throws IllegalStateException {
        short size = in.readShort();

        for (int i = 0; i < size; i++) {
            final AddressAndPort addressAndPort = new AddressAndPort();
            addressAndPort.readMarshallable(in);
            addressAndPorts.add(addressAndPort);
        }

        ByteBufferBytes activeIdentifiersBitSetBytes = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));
        activeIdentifiersBitSetBytes.readMarshallable(in);

        ATSDirectBitSet bitset = new ATSDirectBitSet(activeIdentifiersBitSetBytes);
        for (long next = bitset.nextSetBit(0); next > 0; next = bitset.nextSetBit(next + 1)) {
            atsDirectBitSet.set(next);
        }
    }

    @Override
    public void writeMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes out) {

        // make a safe copy
        final Set<AddressAndPort> addressAndPorts = new HashSet<AddressAndPort>(this.addressAndPorts);

        // write the size
        out.writeShort(addressAndPorts.size());

        for (AddressAndPort bytesMarshallable : addressAndPorts) {
            bytesMarshallable.writeMarshallable(out);
        }

        activeIdentifiersBitSetBytes.clear();
        activeIdentifiersBitSetBytes.writeMarshallable(out);

    }

    @Override
    public String toString() {
        return "RemoteNodes{" +
                " addressAndPorts=" + addressAndPorts +
                ", bitSet=" + NodeDiscoveryBroadcaster.toString(atsDirectBitSet) +
                '}';
    }
}


class AddressAndPort implements Comparable<AddressAndPort>, BytesMarshallable {
    private byte[] address;
    private short port;

    public AddressAndPort(byte[] address, short port) {
        this.address = address;
        this.port = port;
    }

    public AddressAndPort() {

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
        if (src.length == 4) {
            return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + (src[2] & 0xff) + "." + (src[3] & 0xff);
        }
        throw new UnsupportedOperationException();

    }


    @Override
    public void readMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes in) throws IllegalStateException {
        short len = in.readShort();
        address = new byte[len];

        for (int i = 0; i < len; i++) {
            address[i] = in.readByte();
        }
        port = in.readShort();

    }

    @Override
    public void writeMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes out) {
        out.writeShort(getAddress().length);
        for (byte address : getAddress()) {
            out.write(address);
        }
        out.writeShort(port);
    }
}


class BytesExternalizableImpl implements BytesMarshallable {

    private final RemoteNodes remoteNode;

    private final AtomicBoolean bootstrapRequired = new AtomicBoolean();
    private final UDPEventListener udpEventListener;

    private ProposedIdentifierWithHost ourProposedIdentifier;

    final ConcurrentExpiryMap<AddressAndPort, ProposedIdentifierWithHost> proposedIdentifiersWithHost = new
            ConcurrentExpiryMap<AddressAndPort, ProposedIdentifierWithHost>(AddressAndPort.class,
            ProposedIdentifierWithHost.class);

    public ProposedIdentifierWithHost getOurProposedIdentifier() {
        return ourProposedIdentifier;
    }

    private void setOurProposedIdentifier(ProposedIdentifierWithHost ourProposedIdentifier) {
        this.ourProposedIdentifier = ourProposedIdentifier;
    }

    public void setModificationNotifier(Replica.ModificationNotifier modificationNotifier) {
        this.modificationNotifier = modificationNotifier;

    }

    Replica.ModificationNotifier modificationNotifier;

    public BytesExternalizableImpl(final RemoteNodes remoteNode, UDPEventListener udpEventListener) {
        this.remoteNode = remoteNode;
        this.udpEventListener = udpEventListener;
    }

    public RemoteNodes getRemoteNodes() {
        return remoteNode;
    }

    /**
     * this is used to tell nodes that are connecting to us which host and ports are in our grid, along with
     * all the identifiers.
     */
    @Override
    public void writeMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes out) {

        if (bootstrapRequired.getAndSet(false)) {
            writeBootstrap(out);
            return;
        }

        remoteNode.writeMarshallable(out);

        // we are no going to broadcast all the nodes that have been bootstaping in the last 200
        proposedIdentifiersWithHost.expireEntries(System.currentTimeMillis() - 200);

        proposedIdentifiersWithHost.writeMarshallable(out);


    }

    private boolean writeBootstrap(Bytes out) {
        final ProposedIdentifierWithHost ourProposedIdentifier = getOurProposedIdentifier();
        if (ourProposedIdentifier == null || ourProposedIdentifier.addressAndPort == null)
            return false;

        BOOTSTRAP_BYTES.clear();
        out.write(BOOTSTRAP_BYTES);

        ourProposedIdentifier.writeMarshallable(out);
        return true;
    }

    /**
     * @param in
     * @return returns true if the UDP message contains the text 'BOOTSTRAP'
     */
    private ProposedIdentifierWithHost readBootstrapMessage(Bytes in) {

        final long start = in.position();

        try {

            long size = BOOTSTRAP_BYTES.limit();

            if (size > in.remaining())
                return null;

            // reads the text bootstrap
            for (int i = 0; i < size; i++) {

                final byte byteRead = in.readByte();
                final byte expectedByte = BOOTSTRAP_BYTES.readByte(i);

                if (!(expectedByte == byteRead))
                    return null;
            }

            final ProposedIdentifierWithHost result = new ProposedIdentifierWithHost();
            result.readMarshallable(in);

            return result;

        } finally {
            in.position(start);
        }
    }


    @Override
    public void readMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes in) throws IllegalStateException {

        final ProposedIdentifierWithHost bootstrap = readBootstrapMessage(in);
        if (bootstrap != null) {
            LOG.debug("received Bootstrap");

            proposedIdentifiersWithHost.put(bootstrap.addressAndPort, bootstrap);

            try {

                // we've received a bootstrap message so will will now rebroadcast what we know,
                // after a random delay

                Thread.sleep((int) (Math.random() * 9.0));

                // this is used to turn on the OP_WRITE, so that we can broadcast back the known host and
                // ports in the grid
                onChange();

            } catch (InterruptedException e) {
                LOG.error("", e);
            }

            return;
        }


        this.remoteNode.readMarshallable(in);


        // we are no going to broadcast all the nodes that have been bootstaping in the last 200
        proposedIdentifiersWithHost.expireEntries(System.currentTimeMillis() - 200);
        proposedIdentifiersWithHost.readMarshallable(in);

        if (udpEventListener != null)
            udpEventListener.onRemoteNodeEvent(remoteNode, proposedIdentifiersWithHost);


    }


    public void onChange() {
        if (modificationNotifier != null)
            modificationNotifier.onChange();
    }

    public static class ProposedIdentifierWithHost implements BytesMarshallable {

        private byte identifier;
        private long timestamp;
        private AddressAndPort addressAndPort;

        public ProposedIdentifierWithHost() {

        }

        public byte identifier() {
            return identifier;
        }

        public ProposedIdentifierWithHost(@NotNull final AddressAndPort addressAndPort,
                                          byte identifier) {
            this.addressAndPort = addressAndPort;
            this.identifier = identifier;
            this.timestamp = System.currentTimeMillis();
        }

        public ProposedIdentifierWithHost(byte[] address, short port, byte identifier) {
            this(new AddressAndPort(address, port), identifier);
        }


        public AddressAndPort addressAndPort() {
            return addressAndPort;
        }

        @Override
        public void readMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes in) throws IllegalStateException {
            addressAndPort = new AddressAndPort();
            addressAndPort.readMarshallable(in);
            timestamp = in.readLong();
            identifier = in.readByte();

        }

        @Override
        public void writeMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes out) {
            addressAndPort.writeMarshallable(out);
            out.writeLong(timestamp);
            out.writeByte(identifier);
        }

        @Override
        public String toString() {
            return "ProposedIdentifierWithHost{" +
                    "identifier=" + identifier +
                    ", timestamp=" + timestamp +
                    ", addressAndPort=" + addressAndPort +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ProposedIdentifierWithHost that = (ProposedIdentifierWithHost) o;

            if (identifier != that.identifier) return false;
            if (!addressAndPort.equals(that.addressAndPort)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) identifier;
            result = 31 * result + addressAndPort.hashCode();
            return result;
        }
    }


    /**
     * sends a bootstrap message to the other nodes in the grid, the bootstrap message contains the host:port
     * and perhaps even proposed identifier of the node that sent it.
     *
     * @param proposedIdentifierWithHost
     */
    public void sendBootStrap(ProposedIdentifierWithHost proposedIdentifierWithHost) {
        setOurProposedIdentifier(proposedIdentifierWithHost);
        bootstrapRequired.set(true);
    }


}

interface UDPEventListener {

    /**
     * called when we have received a UDP message, this is called after the message has been parsed
     */

    void onRemoteNodeEvent(RemoteNodes remoteNode, ConcurrentExpiryMap<AddressAndPort, ProposedIdentifierWithHost> proposedIdentifiersWithHost);
}

class ConcurrentExpiryMap<K extends BytesMarshallable, V extends BytesMarshallable> implements BytesMarshallable {

    final ConcurrentMap<K, V> map = new
            ConcurrentHashMap<K, V>();
    private final Class<K> kClass;
    private final Class<V> vClass;

    ConcurrentExpiryMap(Class<K> kClass, Class<V> vClass) {

        this.kClass = kClass;
        this.vClass = vClass;
    }

    @Override
    public void readMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes in) throws IllegalStateException {

        short size = in.readShort();
        try {
            for (int i = 0; i < size; i++) {

                final K k = kClass.newInstance();
                k.writeMarshallable(in);

                final V v = vClass.newInstance();
                v.readMarshallable(in);

                map.put(k, v);
            }
        } catch (Exception e) {
            LOG.error("", e);
        }

    }

    @Override
    public void writeMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes out) {
        final Map<K, V> safeCopy = new HashMap<K, V>(map);
        out.writeShort(safeCopy.size());
        for (Map.Entry<K, V> entry : safeCopy.entrySet()) {
            entry.getKey().writeMarshallable(out);
            entry.getValue().writeMarshallable(out);
        }
    }

    class W<V> {
        final long timestamp;
        final V v;

        public W(V v) {
            this.v = v;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public void put(final K k, final V v) {
        map.put(k, v);
        final W w = new W(v);
        queue.add(new Map.Entry<K, W<V>>() {

            @Override
            public K getKey() {
                return k;
            }

            @Override
            public W<V> getValue() {
                return w;
            }

            @Override
            public W<V> setValue(W<V> value) {
                throw new UnsupportedOperationException();
            }
        });
    }


    final Queue<Map.Entry<K, W<V>>> queue = new ConcurrentLinkedQueue<Map.Entry<K, W<V>>>();

    public java.util.Collection<V> values() {
        return map.values();
    }


    public void expireEntries(long timeOlderThan) {
        for (; ; ) {

            final Map.Entry<K, W<V>> e = this.queue.peek();

            if (e == null)
                break;

            if (e.getValue().timestamp < timeOlderThan) {
                // only remote it if it has not changed
                map.remove(e.getKey(), e.getValue().v);
            }

            this.queue.poll();
        }
    }

}