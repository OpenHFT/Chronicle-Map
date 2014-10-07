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
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteBuffer.wrap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.ConcurrentExpiryMap.getDefaultAddress;
import static net.openhft.chronicle.map.NodeDiscoveryBroadcaster.BOOTSTRAP_BYTES;
import static net.openhft.chronicle.map.NodeDiscoveryBroadcaster.LOG;
import static net.openhft.chronicle.map.Replicators.tcp;
import static net.openhft.chronicle.map.UdpReplicationConfig.simple;

/**
 * @author Rob Austin.
 *
 *         This code is in alpha, and still has to undergo refactoring
 */
public class NodeDiscovery {

    private final AddressAndPort ourAddressAndPort;
    private final UdpReplicationConfig udpConfig;
    private final DiscoveryNodeBytesMarshallable discoveryNodeBytesMarshallable;
    private final AtomicReference<NodeDiscoveryEventListener> nodeDiscoveryEventListenerAtomicReference =
            new AtomicReference<NodeDiscoveryEventListener>();
    private KnownNodes knownNodes;

    public NodeDiscovery() throws IOException {
        this((short) 8124, (short) 8123, getDefaultAddress(), Inet4Address.getByName("255.255.255.255"));
    }

    public NodeDiscovery(short udpBroadcastPort,
                         short tcpPort,
                         @NotNull InetAddress tcpAddress,
                         @NotNull InetAddress udpBroadcastAddress) throws IOException {
        this.ourAddressAndPort = new AddressAndPort(tcpAddress.getAddress(), tcpPort);
        this.udpConfig = simple(udpBroadcastAddress, udpBroadcastPort);
        knownNodes = new KnownNodes();

        discoveryNodeBytesMarshallable = new DiscoveryNodeBytesMarshallable
                (knownNodes, nodeDiscoveryEventListenerAtomicReference, ourAddressAndPort);

        final NodeDiscoveryBroadcaster nodeDiscoveryBroadcaster
                = new NodeDiscoveryBroadcaster(udpConfig, 1024, discoveryNodeBytesMarshallable);

        discoveryNodeBytesMarshallable.setModificationNotifier(nodeDiscoveryBroadcaster);
    }


    public synchronized ChronicleMap<Integer, CharSequence> discoverMap() throws
            IOException, InterruptedException {

        final AtomicInteger ourProposedIdentifier = new AtomicInteger();
        final AtomicBoolean useAnotherIdentifier = new AtomicBoolean();


        final Set<AddressAndPort> knownHostPorts = new ConcurrentSkipListSet<AddressAndPort>();
        final DirectBitSet knownAndProposedIdentifiers = new ATSDirectBitSet(new ByteBufferBytes(ByteBuffer.allocate
                (128 / 8)));

        final AtomicReference<CountDownLatch> countDownLatch = new AtomicReference<CountDownLatch>(new
                CountDownLatch(1));

        final NodeDiscoveryEventListener nodeDiscoveryEventListener = new NodeDiscoveryEventListener() {

            @Override
            public void onRemoteNodeEvent(KnownNodes remoteNodes,
                                          ConcurrentExpiryMap<AddressAndPort,
                                                  DiscoveryNodeBytesMarshallable.ProposedNodes> proposedIdentifiersWithHost) {

                LOG.info("onRemoteNodeEvent " + remoteNodes + ", proposedIdentifiersWithHost=" + proposedIdentifiersWithHost);

                knownHostPorts.addAll(remoteNodes.addressAndPorts());

                orBitSets(remoteNodes.identifiers(), knownAndProposedIdentifiers);

                for (DiscoveryNodeBytesMarshallable.ProposedNodes proposedIdentifierWithHost :
                        proposedIdentifiersWithHost.values()) {
                    if (!proposedIdentifierWithHost.addressAndPort().equals(ourAddressAndPort)) {

                        int proposedIdentifier = proposedIdentifierWithHost.identifier();
                        if (proposedIdentifier != -1) {
                            knownAndProposedIdentifiers.set(proposedIdentifier, true);
                        }

                        knownHostPorts.add(proposedIdentifierWithHost.addressAndPort());

                        if (proposedIdentifier == ourProposedIdentifier.get())
                            useAnotherIdentifier.set(true);


                    }
                }
                countDownLatch.get().countDown();
            }


        };

        nodeDiscoveryEventListenerAtomicReference.set(nodeDiscoveryEventListener);

        final DiscoveryNodeBytesMarshallable.ProposedNodes ourHostPort = new
                DiscoveryNodeBytesMarshallable.ProposedNodes(ourAddressAndPort, (byte) -1);

        // to start with we will send a bootstrap that just contains our hostname without and identifier

        for (int i = 0; i < 10; i++) {
            discoveryNodeBytesMarshallable.sendBootStrap(ourHostPort);

            // once the count down latch is trigger we know we go something back from one of the nodes
            if (countDownLatch.get().await(i * 20, TimeUnit.MILLISECONDS))
                break;
        }

        // we should now get back some identifiers
        // the identifiers will come back to the callback on the nio thread, the update arrives at the
        // onRemoteNodeEvent

        Thread.sleep(200);

        byte identifier;

        // now we are going to propose an identifier
        boolean isFistTime = true;

        OUTER:
        for (; ; ) {
            useAnotherIdentifier.set(false);
            identifier = proposeRandomUnusedIdentifier(knownAndProposedIdentifiers, isFistTime);
            ourProposedIdentifier.set(identifier);
            LOG.info("proposing to use identifier=" + identifier);

            isFistTime = false;

            final DiscoveryNodeBytesMarshallable.ProposedNodes proposedNodes = new
                    DiscoveryNodeBytesMarshallable.ProposedNodes(ourAddressAndPort, identifier);

            Thread.sleep(100);

            countDownLatch.set(new CountDownLatch(1));

            for (int i = 0; i < 20; i++) {
                discoveryNodeBytesMarshallable.sendBootStrap(proposedNodes);

                // once the count down latch is trigger we know we go something back from one of the nodes
                if (countDownLatch.get().await(i * 20, TimeUnit.MILLISECONDS)) {
                    if (useAnotherIdentifier.get()) {
                        // given that another node host proposed the same identifier, we will choose a different one.
                        LOG.info("Another node is using identifier=" + identifier + ", " +
                                "going to have to select another one, we will wait 500ms before continuing");
                        continue OUTER;
                    } else {
                        break OUTER;
                    }
                } else {
                    LOG.debug("timed-out getting a response from the server so sending another boot-strap  " +
                            "message");
                }

            }

            LOG.info("looks like we are the only node in the grid, so going to use identifier=" + identifier);

            break;
        }


        // we should make a local copy as this may change

        final IdentifierListener identifierListener = new IdentifierListener() {

            private final ConcurrentMap<Byte, SocketAddress> identifiers = new ConcurrentHashMap<Byte,
                    SocketAddress>();

            @Override
            public boolean isIdentifierUnique(byte remoteIdentifier, SocketAddress remoteAddress) {
                final SocketAddress lastKnownAddress = identifiers.putIfAbsent(remoteIdentifier, remoteAddress);
                if (remoteAddress.equals(lastKnownAddress))
                    return true;
                knownNodes.identifiers().set(remoteIdentifier);
                return lastKnownAddress == null;
            }
        };

        // add our identifier and host:port to the list of known identifiers
        knownNodes.add(ourAddressAndPort, identifier);

        final TcpReplicationConfig tcpConfig = TcpReplicationConfig
                .of(ourAddressAndPort.getPort(), toInetSocketArray(knownHostPorts))
                .heartBeatInterval(1, SECONDS).nonUniqueIdentifierListener(identifierListener);


        LOG.info("Using Remote identifier=" + identifier);
        nodeDiscoveryEventListenerAtomicReference.set(null);

        return ChronicleMapBuilder.of(Integer.class,
                CharSequence.class)
                .entries(20000L)
                .addReplicator(tcp(identifier, tcpConfig)).create();

    }

    private InetSocketAddress[] toInetSocketArray(Set<AddressAndPort> source) throws
            UnknownHostException {

        // make a safe copy
        final HashSet<AddressAndPort> addressAndPorts = new HashSet<AddressAndPort>(source);

        if (addressAndPorts.isEmpty())
            return new InetSocketAddress[0];

        final InetSocketAddress[] addresses = new InetSocketAddress[addressAndPorts.size()];

        int i = 0;

        for (final AddressAndPort addressAndPort : addressAndPorts) {
            addresses[i++] = new InetSocketAddress(InetAddress.getByAddress(addressAndPort.address())
                    .getHostAddress(), addressAndPort.port());
        }
        return addresses;
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


    static byte proposeRandomUnusedIdentifier(final DirectBitSet knownIdentifiers,
                                              boolean isFirstTime) throws UnknownHostException {
        byte possible;


        // the first time, rather than choosing a random number, we will choose the last value of the IP
        // address as our random number, ( or at least something that is based upon it)
        if (isFirstTime) {
            byte[] address = InetAddress.getLocalHost().getAddress();
            int lastAddress = address[address.length - 1];
            if (lastAddress > 127)
                lastAddress = lastAddress - 127;
            if (lastAddress > 127)
                lastAddress = lastAddress - 127;

            possible = (byte) lastAddress;
        } else
            possible = (byte) (Math.random() * 128);

        int count = 0;
        for (; ; ) {

            if (knownIdentifiers.setIfClear(possible)) {
                return possible;
            }

            count++;

            if (count == 128) {
                throw new IllegalStateException("The grid is full, its not possible for any more nodes to " +
                        "going the grid.");
            }

            if (possible == 128)
                possible = 0;
            else
                possible++;
        }
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

/**
 * Broadcast the nodes host ports and identifiers over UDP, to make it easy to join a grid of remote nodes
 * just by name, this functionality requires UDP
 *
 * @author Rob Austin.
 */
class NodeDiscoveryBroadcaster extends UdpChannelReplicator {

    public static final Logger LOG = LoggerFactory.getLogger(NodeDiscoveryBroadcaster.class.getName());

    private static final byte UNUSED = (byte) -1;

    static final ByteBufferBytes BOOTSTRAP_BYTES;

    static {
        final String BOOTSTRAP = "BOOTSTRAP";
        BOOTSTRAP_BYTES = new ByteBufferBytes(ByteBuffer.allocate(2 + BOOTSTRAP.length()));
        BOOTSTRAP_BYTES.write((short) BOOTSTRAP.length());
        BOOTSTRAP_BYTES.append(BOOTSTRAP);
    }

    static String toString(DirectBitSet bitSet) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bitSet.size(); i++) {
            builder.append(bitSet.get(i) ? '1' : '0');
        }
        return builder.toString();
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

    static class UdpSocketChannelEntryReader implements EntryReader {

        private final ByteBuffer in;
        private final ByteBufferBytes out;
        private final BytesMarshallable externalizable;

        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         */
        UdpSocketChannelEntryReader(final int serializedEntrySize,
                                    @NotNull final BytesMarshallable externalizable) {

            // we make the buffer twice as large just to give ourselves headroom
            in = allocateDirect(serializedEntrySize * 2);

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

    static class UdpSocketChannelEntryWriter implements EntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;

        @NotNull
        private final BytesMarshallable externalizable;
        private UdpChannelReplicator udpReplicator;

        UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final BytesMarshallable externalizable,
                                    @NotNull final UdpChannelReplicator udpReplicator) {

            this.externalizable = externalizable;
            this.udpReplicator = udpReplicator;

            // we make the buffer twice as large just to give ourselves headroom
            out = allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);
        }


        /**
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


class KnownNodes implements BytesMarshallable {

    private Bytes activeIdentifiersBitSetBytes;
    private ConcurrentSkipListSet<AddressAndPort> addressAndPorts = new ConcurrentSkipListSet<AddressAndPort>();
    private ATSDirectBitSet atsDirectBitSet;

    KnownNodes() {

        this.activeIdentifiersBitSetBytes = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));
        this.addressAndPorts = new ConcurrentSkipListSet<AddressAndPort>();
        this.atsDirectBitSet = new ATSDirectBitSet(this.activeIdentifiersBitSetBytes);
    }


    public Set<AddressAndPort> addressAndPorts() {
        return addressAndPorts;
    }

    public void add(AddressAndPort inetSocketAddress, byte identifier) {
        identifiers().set(identifier);
        addressAndPorts.add(inetSocketAddress);
    }

    /**
     * all the known identifiers
     *
     * @return
     */
    public DirectBitSet identifiers() {
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

        final ByteBufferBytes activeIdentifiersBitSetBytes = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));
        activeIdentifiersBitSetBytes.readMarshallable(in);

        final ATSDirectBitSet bitset = new ATSDirectBitSet(activeIdentifiersBitSetBytes);
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

    AddressAndPort(byte[] address, short port) {
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
        return "{" +
                "address=" + numericToTextFormat(address) +
                ", port=" + port +
                '}';
    }

    final static int INADDRSZ = 16;
    final static int INT16SZ = 2;

    static String numericToTextFormat(byte[] src) {
        if (src.length == 4) {
            return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + (src[2] & 0xff) + "." + (src[3] & 0xff);
        }

        StringBuffer sb = new StringBuffer(39);
        for (int i = 0; i < (INADDRSZ / INT16SZ); i++) {
            sb.append(Integer.toHexString(((src[i << 1] << 8) & 0xff00)
                    | (src[(i << 1) + 1] & 0xff)));
            if (i < (INADDRSZ / INT16SZ) - 1) {
                sb.append(":");
            }
        }

        return sb.toString();


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


class DiscoveryNodeBytesMarshallable implements BytesMarshallable {

    private AddressAndPort sourceAddressAndPort = new AddressAndPort();
    private final KnownNodes remoteNode;

    private final AtomicBoolean bootstrapRequired = new AtomicBoolean();
    private final AtomicReference<NodeDiscoveryEventListener> nodeDiscoveryEventListener;

    private ProposedNodes ourProposedIdentifier;

    final ConcurrentExpiryMap<AddressAndPort, ProposedNodes> proposedIdentifiersWithHost = new
            ConcurrentExpiryMap<AddressAndPort, ProposedNodes>(AddressAndPort.class,
            ProposedNodes.class);

    private AddressAndPort ourAddressAndPort;

    public ProposedNodes getOurProposedIdentifier() {
        return ourProposedIdentifier;
    }

    private void setOurProposedIdentifier(ProposedNodes ourProposedIdentifier) {
        this.ourProposedIdentifier = ourProposedIdentifier;
    }

    public void setModificationNotifier(Replica.ModificationNotifier modificationNotifier) {
        this.modificationNotifier = modificationNotifier;
    }

    Replica.ModificationNotifier modificationNotifier;

    public DiscoveryNodeBytesMarshallable(@NotNull final KnownNodes remoteNode,
                                          @NotNull final AtomicReference<NodeDiscoveryEventListener>
                                                  nodeDiscoveryEventListener,
                                          @NotNull final AddressAndPort ourAddressAndPort) {
        this.remoteNode = remoteNode;
        this.nodeDiscoveryEventListener = nodeDiscoveryEventListener;
        this.ourAddressAndPort = ourAddressAndPort;
    }

    public KnownNodes getRemoteNodes() {
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

        // the host and port the message came from
        this.ourAddressAndPort.writeMarshallable(out);

        remoteNode.writeMarshallable(out);

        // we are no going to broadcast all the nodes that have been bootstaping in the last second
        proposedIdentifiersWithHost.expireEntries(System.currentTimeMillis() - SECONDS.toMillis(1));

        proposedIdentifiersWithHost.writeMarshallable(out);


    }

    private boolean writeBootstrap(Bytes out) {
        final ProposedNodes ourProposedIdentifier = getOurProposedIdentifier();
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
    private ProposedNodes readBootstrapMessage(Bytes in) {

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

            final ProposedNodes result = new ProposedNodes();
            result.readMarshallable(in);

            return result;

        } finally {
            in.position(start);
        }
    }


    @Override
    public void readMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes in) throws IllegalStateException {

        final ProposedNodes bootstrap = readBootstrapMessage(in);
        if (bootstrap != null) {

            if (bootstrap.addressAndPort().equals(this.ourAddressAndPort))
                return;

            LOG.info("Received Bootstrap from " + bootstrap);

            proposedIdentifiersWithHost.put(bootstrap.addressAndPort, bootstrap);

            // we've received a bootstrap message so will will now rebroadcast what we know,
            // after a random delay
            //  Thread.sleep((int) (Math.random() * 9.0));

            // this is used to turn on the OP_WRITE, so that we can broadcast back the known host and
            // ports in the grid
            onChange();
            return;
        }


        // the host and port the message came from
        this.sourceAddressAndPort.readMarshallable(in);
        if (sourceAddressAndPort.equals(ourAddressAndPort))
            return;

        LOG.info("Received Proposal");

        this.remoteNode.readMarshallable(in);
        this.proposedIdentifiersWithHost.readMarshallable(in);

        NodeDiscoveryEventListener listener = nodeDiscoveryEventListener.get();
        if (listener != null)
            listener.onRemoteNodeEvent(remoteNode, proposedIdentifiersWithHost);
    }


    public void onChange() {
        if (modificationNotifier != null)
            modificationNotifier.onChange();
    }

    static class ProposedNodes implements BytesMarshallable {

        private byte identifier;
        private long timestamp;
        private AddressAndPort addressAndPort;

        public ProposedNodes() {

        }

        public byte identifier() {
            return identifier;
        }

        ProposedNodes(@NotNull final AddressAndPort addressAndPort,
                      byte identifier) {
            this.addressAndPort = addressAndPort;
            this.identifier = identifier;
            this.timestamp = System.currentTimeMillis();
        }


        AddressAndPort addressAndPort() {
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
            return "{" +
                    "identifier=" + identifier +
                    ", timestamp=" + timestamp +
                    ", addressAndPort=" + addressAndPort +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ProposedNodes that = (ProposedNodes) o;

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
     * @param proposedNodes
     */
    public void sendBootStrap(ProposedNodes proposedNodes) {
        setOurProposedIdentifier(proposedNodes);
        bootstrapRequired.set(true);
        onChange();
    }


}

interface NodeDiscoveryEventListener {
    /**
     * called when we have received a UDP message, this is called after the message has been parsed
     */
    void onRemoteNodeEvent(KnownNodes remoteNode, ConcurrentExpiryMap<AddressAndPort, DiscoveryNodeBytesMarshallable.ProposedNodes> proposedIdentifiersWithHost);
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
    public String toString() {
        return "ConcurrentExpiryMap{" + map + '}';
    }

    @Override
    public void readMarshallable(@net.openhft.lang.model.constraints.NotNull Bytes in) throws IllegalStateException {

        short size = in.readShort();
        try {
            for (int i = 0; i < size; i++) {

                final K k = kClass.newInstance();
                k.readMarshallable(in);

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

        W(V v) {
            this.v = v;
            this.timestamp = System.currentTimeMillis();
        }
    }

    void put(final K k, final V v) {
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


    // this is used for expiry
    private final Queue<Map.Entry<K, W<V>>> queue = new ConcurrentLinkedQueue<Map.Entry<K, W<V>>>();

    java.util.Collection<V> values() {
        return map.values();
    }


    void expireEntries(long timeOlderThan) {
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


    public static void main(String... args) {
        String ip;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                // filters out 127.0.0.1 and inactive interfaces
                if (iface.isLoopback() || !iface.isUp())
                    continue;

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    ip = addr.getHostAddress();
                    System.out.println(iface.getDisplayName() + " " + ip);
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * private NetworkInterface defaultNetworkInterface() throws SocketException { NetworkInterface
     * networkInterface = null
     *
     * for (String suggestedName : new String[]{"en0", "eth0"}) { networkInterface =
     * NetworkInterface.getByName(suggestedName); if (networkInterface != null) break; } return
     * networkInterface; }
     *
     * /**
     *
     * @return the default network interface, or
     * @throws SocketException
     */
    public static NetworkInterface defaultNetworkInterface() throws SocketException {
        NetworkInterface networkInterface = null;

        for (String suggestedName : new String[]{"en0", "eth0"}) {
            networkInterface = NetworkInterface.getByName(suggestedName);
            if (networkInterface != null)
                break;
        }

        if (networkInterface != null)
            return networkInterface;

        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        if (networkInterfaces == null || !networkInterfaces.hasMoreElements())
            return null;

        return networkInterfaces.nextElement();

    }

    public static InetAddress getDefaultAddress() throws SocketException {
        NetworkInterface networkInterface = ConcurrentExpiryMap.defaultNetworkInterface();
        Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
        InetAddress inetAddress = inetAddresses.nextElement();

        if (inetAddress == null)
            throw new IllegalStateException();

        return inetAddress;
    }
}
