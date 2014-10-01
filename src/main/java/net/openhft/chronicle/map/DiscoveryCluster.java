package net.openhft.chronicle.map;

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.ByteBuffer.wrap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.NodeDiscoveryBroadcaster.LOG;
import static net.openhft.chronicle.map.Replicators.tcp;

/**
 * @author Rob Austin.
 */
public class DiscoveryCluster {


    public ChronicleMap<Integer, CharSequence> discoverMap(int udpBroadcastPort, final int tcpPort) throws IOException, InterruptedException {

        final AtomicInteger proposedIdentifier = new AtomicInteger();
        final AtomicBoolean useAnotherIdentifier = new AtomicBoolean();

        final UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), udpBroadcastPort);

        final Bytes identifierBitSetBits = new ByteBufferBytes(ByteBuffer.allocate(128 / 8));

        final RemoteNodes remoteNodes = new RemoteNodes(identifierBitSetBits);
        final DirectBitSet knownIdentifiers = new ATSDirectBitSet(new ByteBufferBytes(ByteBuffer.allocate
                (128 / 8)));

        final AddressAndPort ourAddressAndPort = new AddressAndPort(InetAddress.getLocalHost()
                .getAddress(),
                (short) tcpPort);

        final Set<AddressAndPort> knownHostPorts = new ConcurrentSkipListSet<AddressAndPort>();

        final UDPEventListener udpEventListener = new UDPEventListener() {

            @Override
            public void onRemoteNodeEvent(@NotNull final Set<AddressAndPort> usedHostPorts,
                                          @NotNull final ATSDirectBitSet usedIdentifiers,
                                          @NotNull final Set<BytesExternalizableImpl.ProposedIdentifierWithHost> proposedHostPortIdentifiers) {

                knownHostPorts.addAll(usedHostPorts);

                orBitSets(usedIdentifiers, knownIdentifiers);

                for (BytesExternalizableImpl.ProposedIdentifierWithHost proposedIdentifierWithHost : proposedHostPortIdentifiers) {
                    if (!proposedIdentifierWithHost.addressAndPort().equals(ourAddressAndPort)) {

                        int remoteIdentifier = proposedIdentifierWithHost.identifier();
                        knownIdentifiers.set(remoteIdentifier, true);
                        knownHostPorts.add(proposedIdentifierWithHost.addressAndPort());

                        if (remoteIdentifier == proposedIdentifier.get())
                            useAnotherIdentifier.set(true);
                    }
                }
            }
        };

        final BytesExternalizableImpl externalizable = new BytesExternalizableImpl(remoteNodes, udpEventListener);

        final BytesExternalizableImpl.ProposedIdentifierWithHost ourHostPort = new
                BytesExternalizableImpl.ProposedIdentifierWithHost(ourAddressAndPort, (byte) -1);

        // to start with we will send a bootstrap that just contains our hostname without and identifier
        externalizable.setOurProposedIdentifier(ourHostPort);
        externalizable.sendBootStrap();
        Thread.sleep(10);

        // we should not get back some identifiers
        // the identifiers will come back to the callback on the nio thread, the update arrives at the
        // onRemoteNodeEvent

        externalizable.sendBootStrap();
        Thread.sleep(10);
        byte identifier;


        boolean isFistTime = true;

        for (; ; ) {

            identifier = proposeRandomUnusedIdentifier(knownIdentifiers, isFistTime);
            proposedIdentifier.set(identifier);

            isFistTime = false;

            final BytesExternalizableImpl.ProposedIdentifierWithHost proposedIdentifierWithHost = new
                    BytesExternalizableImpl.ProposedIdentifierWithHost(ourAddressAndPort, identifier);

            externalizable.setOurProposedIdentifier(proposedIdentifierWithHost);
            externalizable.sendBootStrap();

            Thread.sleep(10);

            for (int j = 0; j < 3; j++) {

                externalizable.sendBootStrap();
                Thread.sleep(10);

                if (useAnotherIdentifier.get()) {
                    // given that another node host proposed the same identifier, we will choose a different one.
                    continue;
                }
            }

            break;
        }


        final NodeDiscoveryBroadcaster nodeDiscoveryBroadcaster
                = new NodeDiscoveryBroadcaster(udpConfig, 1024, externalizable);

        externalizable.setModificationNotifier(nodeDiscoveryBroadcaster);

        // we should make a local copy as this may change

        final IdentifierListener identifierListener = new IdentifierListener() {

            final ConcurrentMap<Byte, SocketAddress> identifiers = new ConcurrentHashMap<Byte,
                    SocketAddress>();

            @Override
            public boolean isIdentifierUnique(byte remoteIdentifier, SocketAddress remoteAddress) {
                final SocketAddress socketAddress = identifiers.putIfAbsent(remoteIdentifier, remoteAddress);
                return socketAddress == null;
            }
        };


        final TcpReplicationConfig tcpConfig = TcpReplicationConfig
                .of(tcpPort, toInetSocketArray(knownHostPorts))
                .heartBeatInterval(1, SECONDS).nonUniqueIdentifierListener(identifierListener);

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


    private byte proposeRandomUnusedIdentifier(final DirectBitSet knownIdentifiers, boolean isFirstTime) throws UnknownHostException {
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
