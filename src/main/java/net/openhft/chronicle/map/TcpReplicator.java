/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.replication.ConnectionListener;
import net.openhft.chronicle.hash.replication.RemoteNodeValidator;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.ThrottlingConfig;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.nio.channels.SelectionKey.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.openhft.chronicle.hash.impl.util.BuildVersion.version;
import static net.openhft.lang.MemoryUnit.*;

interface Work {

    /**
     * @param in the buffer that we will fill up
     * @return true when all the work is complete
     */
    boolean doWork(@NotNull Bytes in);
}

/**
 * Used with a {@link net.openhft.chronicle.map.ReplicatedChronicleMap} to send data between the
 * maps using a socket connection {@link net.openhft.chronicle.map.TcpReplicator}
 *
 * @author Rob Austin.
 */
public final class TcpReplicator<K, V> extends AbstractChannelReplicator implements Closeable {

    public static final long TIMESTAMP_FACTOR = 10000;
    private static final int STATELESS_CLIENT = -127;
    private static final byte HEARTBEAT = 0;
    private static final byte NOT_SET = 0;//(byte) HEARTBEAT.ordinal();
    private static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1MB

    public static final long SPIN_LOOP_TIME_IN_NONOSECONDS = TimeUnit.MICROSECONDS.toNanos(500);
    private final SelectionKey[] selectionKeysStore = new SelectionKey[Byte.MAX_VALUE + 1];
    // used to instruct the selector thread to set OP_WRITE on a key correlated by the bit index
    // in the bitset
    private final KeyInterestUpdater opWriteUpdater =
            new KeyInterestUpdater(OP_WRITE, selectionKeysStore);
    private final BitSet activeKeys = new BitSet(selectionKeysStore.length);
    private final long heartBeatIntervalMillis;
    private final ConnectionListener connectionListener;
    private long largestEntrySoFar = 128;

    @NotNull
    private final Replica replica;
    private final byte localIdentifier;

    @NotNull
    private final Replica.EntryExternalizable externalizable;
    @NotNull
    private final TcpTransportAndNetworkConfig replicationConfig;


    private final
    @Nullable
    RemoteNodeValidator remoteNodeValidator;
    private final String name;

    private long selectorTimeout;


    enum State {
        CONNECTED, DISCONNECTED;
    }

    /**
     * @throws IOException on an io error.
     */
    public TcpReplicator(@NotNull final Replica replica,
                         @NotNull final Replica.EntryExternalizable externalizable,
                         @NotNull final TcpTransportAndNetworkConfig replicationConfig,
                         @Nullable final RemoteNodeValidator remoteNodeValidator,
                         String name,
                         @Nullable final ConnectionListener connectionListener)
            throws IOException {

        super("TcpSocketReplicator-" + replica.identifier(), replicationConfig.throttlingConfig());

        final ThrottlingConfig throttlingConfig = replicationConfig.throttlingConfig();
        long throttleBucketInterval = throttlingConfig.bucketInterval(MILLISECONDS);

        heartBeatIntervalMillis = replicationConfig.heartBeatInterval(MILLISECONDS);

        selectorTimeout = Math.min(heartBeatIntervalMillis / 4, throttleBucketInterval);

        this.replica = replica;
        this.localIdentifier = replica.identifier();

        this.externalizable = externalizable;
        this.replicationConfig = replicationConfig;

        this.remoteNodeValidator = remoteNodeValidator;
        this.name = name;

        this.connectionListener = (connectionListener == null) ? null : new ConnectionListener() {

            private final Map<InetAddress, State> listenerStateMap = new ConcurrentHashMap<>();

            @Override
            public void onConnect(InetAddress address, byte identifier, boolean isServer) {

                if (listenerStateMap.put(address, State.CONNECTED) == State.CONNECTED)
                    return;

                connectionListener.onConnect(address, identifier, isServer);
            }

            @Override
            public void onDisconnect(InetAddress address, byte identifier) {
                if (listenerStateMap.put(address, State.DISCONNECTED) == State.DISCONNECTED)
                    return;

                connectionListener.onDisconnect(address, identifier);
            }
        };

        start();
    }

    @Override
    void processEvent() throws IOException {
        try {
            final InetSocketAddress serverInetSocketAddress =
                    new InetSocketAddress(replicationConfig.serverPort());
            final Details serverDetails = new Details(serverInetSocketAddress, localIdentifier);
            new ServerConnector(serverDetails).connect();

            for (InetSocketAddress client : replicationConfig.endpoints()) {
                final Details clientDetails = new Details(client, localIdentifier);
                new ClientConnector(clientDetails).connect();
            }

            while (selector.isOpen()) {
                registerPendingRegistrations();

                final int nSelectedKeys = select();

                // its less resource intensive to set this less frequently and use an approximation
                final long approxTime = System.currentTimeMillis();

                checkThrottleInterval();

                // check that we have sent and received heartbeats
                heartBeatMonitor(approxTime);

                // set the OP_WRITE when data is ready to send
                opWriteUpdater.applyUpdates();

                if (useJavaNIOSelectionKeys) {
                    // use the standard java nio selector

                    if (nSelectedKeys == 0)
                        continue;    // go back and check pendingRegistrations

                    final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    for (final SelectionKey key : selectionKeys) {
                        processKey(approxTime, key);
                    }
                    selectionKeys.clear();
                } else {
                    // use the netty like selector

                    final SelectionKey[] keys = selectedKeys.flip();

                    try {
                        for (int i = 0; i < keys.length && keys[i] != null; i++) {
                            final SelectionKey key = keys[i];

                            try {
                                processKey(approxTime, key);
                            } catch (BufferUnderflowException e) {
                                if (!isClosed)
                                    LOG.error("", e);
                            }
                        }
                    } finally {
                        for (int i = 0; i < keys.length && keys[i] != null; i++) {
                            keys[i] = null;
                        }
                    }
                }
            }
        } catch (CancelledKeyException | ConnectException | ClosedChannelException |
                ClosedSelectorException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (Exception e) {
            LOG.error("", e);
        } catch (Throwable e) {
            LOG.error("", e);
            throw e;
        } finally {

            if (LOG.isDebugEnabled())
                LOG.debug("closing name=" + name);
            if (!isClosed) {
                closeResources();
            }
        }
    }

    private void processKey(long approxTime, @NotNull SelectionKey key) {
        try {

            if (!key.isValid())
                return;

            if (key.isAcceptable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onAccept - " + name);
                onAccept(key);
            }

            if (key.isConnectable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onConnect - " + name);
                onConnect(key);
            }

            if (key.isReadable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onRead - " + name);
                onRead(key, approxTime);
            }
            if (key.isWritable()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("onWrite - " + name);
                onWrite(key, approxTime);
            }
        } catch (InterruptedException e) {
            quietClose(key, e);
        } catch (BufferUnderflowException | IOException |
                ClosedSelectorException | CancelledKeyException e) {
            if (!isClosed)
                quietClose(key, e);
        } catch (Exception e) {
            LOG.info("", e);
            if (!isClosed)
                closeEarlyAndQuietly(key.channel());
        }
    }

    /**
     * spin loops 100000 times first before calling the selector with timeout
     *
     * @return The number of keys, possibly zero, whose ready-operation sets were updated
     * @throws IOException
     */
    private int select() throws IOException {

        long start = System.nanoTime();

        while (System.nanoTime() < start + SPIN_LOOP_TIME_IN_NONOSECONDS) {
            final int keys = selector.selectNow();
            if (keys != 0)
                return keys;
        }

        return selector.select(selectorTimeout);
    }

    /**
     * checks that we receive heartbeats and send out heart beats.
     *
     * @param approxTime the approximate time in milliseconds
     */

    void heartBeatMonitor(long approxTime) {
        for (int i = activeKeys.nextSetBit(0); i >= 0; i = activeKeys.nextSetBit(i + 1)) {
            try {
                final SelectionKey key = selectionKeysStore[i];
                if (!key.isValid() || !key.channel().isOpen()) {
                    activeKeys.clear(i);
                    continue;
                }

                final Attached attachment = (Attached) key.attachment();

                if (attachment == null)
                    continue;

                if (!attachment.hasRemoteHeartbeatInterval)
                    continue;

                try {
                    sendHeartbeatIfRequired(approxTime, key);
                } catch (Exception e) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                }

                try {
                    heartbeatCheckHasReceived(key, approxTime);
                } catch (Exception e) {

                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                }
            } catch (Exception e) {
                if (LOG.isDebugEnabled())
                    LOG.debug("", e);
            }
        }
    }


    /**
     * check to see if its time to send a heartbeat, and send one if required
     *
     * @param approxTime the current time ( approximately )
     * @param key        nio selection key
     */
    private void sendHeartbeatIfRequired(final long approxTime,
                                         @NotNull final SelectionKey key) {
        final Attached attachment = (Attached) key.attachment();

        if (attachment.isHandShakingComplete() && attachment.entryWriter.lastSentTime +
                heartBeatIntervalMillis < approxTime) {
            attachment.entryWriter.lastSentTime = approxTime;
            attachment.entryWriter.writeHeartbeatToBuffer();

            enableOpWrite(key);

            if (LOG.isDebugEnabled())
                LOG.debug("sending heartbeat");
        }
    }

    private void enableOpWrite(@NotNull SelectionKey key) {
        int ops = key.interestOps();
        if ((ops & (OP_CONNECT | OP_ACCEPT)) == 0)
            key.interestOps(ops | OP_WRITE);
    }

    /**
     * check to see if we have lost connection with the remote node and if we have attempts a
     * reconnect.
     *
     * @param key               the key relating to the heartbeat that we are checking
     * @param approxTimeOutTime the approximate time in milliseconds
     * @throws ConnectException
     */
    private void heartbeatCheckHasReceived(@NotNull final SelectionKey key,
                                           final long approxTimeOutTime) {

        final Attached attached = (Attached) key.attachment();

        if (!attached.isServer || !attached.isHandShakingComplete())
            return;

        final SocketChannel channel = (SocketChannel) key.channel();

        if (approxTimeOutTime >
                attached.entryReader.lastHeartBeatReceived + attached.remoteHeartbeatInterval) {
            if (LOG.isDebugEnabled())
                LOG.debug("lost connection, attempting to reconnect. " +
                        "missed heartbeat from identifier=" + attached.remoteIdentifier);
            activeKeys.clear(attached.remoteIdentifier);
            quietClose(key, null);

            closeables.closeQuietly(channel.socket());

            // when node discovery is used ( by nodes broadcasting out their host:port over UDP ),
            // when new or restarted nodes are started up. they attempt to find the nodes
            // on the grid by listening to the host and ports of the other nodes, so these nodes
            // will establish the connection when they come back up, hence under these
            // circumstances, polling a dropped node to attempt to reconnect is no-longer
            // required as the remote node will establish the connection its self on startup.
            if (replicationConfig.autoReconnectedUponDroppedConnection())
                attached.connector.connectLater();
        }

    }

    /**
     * closes and only logs the exception at debug
     *
     * @param key the SelectionKey
     * @param e   the Exception that caused the issue
     */
    private void quietClose(@NotNull final SelectionKey key, @Nullable final Exception e) {
        if (LOG.isDebugEnabled() && e != null)
            LOG.debug("", e);

        if (key.channel() != null && key.attachment() != null && connectionListener != null)
            connectionListener.onDisconnect(((SocketChannel) key.channel()).socket().getInetAddress(),
                    ((Attached) key.attachment()).remoteIdentifier);


        closeEarlyAndQuietly(key.channel());
    }

    /**
     * called when the selector receives a OP_CONNECT message
     */
    private void onConnect(@NotNull final SelectionKey key)
            throws IOException {

        SocketChannel channel = null;

        try {
            channel = (SocketChannel) key.channel();
        } finally {
            closeables.add(channel);
        }

        final Attached attached = (Attached) key.attachment();

        try {
            if (!channel.finishConnect()) {
                return;
            }
        } catch (SocketException e) {
            quietClose(key, e);

            // when node discovery is used ( by nodes broadcasting out their host:port over UDP ),
            // when new or restarted nodes are started up. they attempt to find the nodes
            // on the grid by listening to the host and ports of the other nodes,
            // so these nodes will establish the connection when they come back up,
            // hence under these circumstances, polling a dropped node to attempt to reconnect
            // is no-longer required as the remote node will establish the connection its self
            // on startup.

            attached.connector.connectLater();

            throw e;
        }

        attached.connector.setSuccessfullyConnected();

        if (LOG.isDebugEnabled())
            LOG.debug("successfully connected to {}, local-id={}",
                    channel.socket().getInetAddress(), localIdentifier);

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        attached.entryReader = new TcpSocketChannelEntryReader();
        attached.entryWriter = new TcpSocketChannelEntryWriter();

        key.interestOps(OP_WRITE | OP_READ);

        throttle(channel);

        // register it with the selector and store the ModificationIterator for this key
        attached.entryWriter.identifierToBuffer(localIdentifier);
    }

    /**
     * called when the selector receives a OP_ACCEPT message
     */
    private void onAccept(@NotNull final SelectionKey key) throws IOException {
        ServerSocketChannel server = null;

        try {
            server = (ServerSocketChannel) key.channel();
        } finally {
            if (server != null)
                closeables.add(server);
        }

        SocketChannel channel = null;

        try {
            channel = server.accept();
        } finally {
            if (channel != null)
                closeables.add(channel);
        }

        channel.configureBlocking(false);
        channel.socket().setReuseAddress(true);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        final Attached attached = new Attached();
        attached.entryReader = new TcpSocketChannelEntryReader();
        attached.entryWriter = new TcpSocketChannelEntryWriter();

        attached.entryWriter.identifierToBuffer(localIdentifier);
        attached.isServer = true;

        channel.register(selector, OP_READ, attached);

        throttle(channel);
    }


    /**
     * check that the version number is valid text,
     *
     * @param versionNumber the version number to check
     * @return true, if the version number looks correct
     */
    boolean isValidVersionNumber(String versionNumber) {

        if (versionNumber.length() <= 2)
            return false;

        for (char c : versionNumber.toCharArray()) {

            if (c >= '0' && c <= '9')
                continue;

            if (c == '.' || c == '-' || c == '_')
                continue;

            if (c >= 'A' && c <= 'Z')
                continue;

            if (c >= 'a' && c <= 'z')
                continue;

            return false;

        }

        return true;
    }

    private void checkVersions(final Attached<K, V> attached) {

        final String localVersion = BuildVersion.version();
        final String remoteVersion = attached.serverVersion;


        if (!remoteVersion.equals(localVersion)) {
            byte remoteIdentifier = attached.remoteIdentifier;
            LOG.warn("DIFFERENT CHRONICLE-MAP VERSIONS : " +
                            "local-map=" + localVersion +
                            ", remote-map-id-" + remoteIdentifier + "=" +
                            remoteVersion +

                            ", The Remote Chronicle Map with " +
                            "identifier=" + remoteIdentifier +
                            " and this Chronicle Map are on different " +
                            "versions, we suggest that you use the same version."

            );
        }
    }

    /**
     * used to exchange identifiers and timestamps and heartbeat intervals between the server and
     * client
     *
     * @param key           the SelectionKey relating to the this cha
     * @param socketChannel
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    private void doHandShaking(@NotNull final SelectionKey key,
                               @NotNull SocketChannel socketChannel)
            throws IOException {
        final Attached attached = (Attached) key.attachment();
        final TcpSocketChannelEntryWriter writer = attached.entryWriter;
        final TcpSocketChannelEntryReader reader = attached.entryReader;

        socketChannel.register(selector, OP_READ | OP_WRITE, attached);

        if (attached.remoteIdentifier == Byte.MIN_VALUE) {
            final byte remoteIdentifier = reader.identifierFromBuffer();

            if (remoteIdentifier == STATELESS_CLIENT) {
                attached.handShakingComplete = true;
                attached.hasRemoteHeartbeatInterval = false;
                return;
            }

            if (remoteIdentifier == Byte.MIN_VALUE)
                return;

            attached.remoteIdentifier = remoteIdentifier;

            final SocketChannel channel = (SocketChannel) key.channel();
            if (channel != null && channel.socket() != null && connectionListener != null) {
                connectionListener.onConnect(
                        channel.socket().getInetAddress(),
                        attached.remoteIdentifier,
                        attached.isServer);
            }

            // we use the as iterating the activeKeys via the bitset wont create and Objects
            // but if we use the selector.keys() this will.
            selectionKeysStore[remoteIdentifier] = key;
            activeKeys.set(remoteIdentifier);

            if (LOG.isDebugEnabled()) {
                LOG.debug("server-connection id={}, remoteIdentifier={}", localIdentifier,
                        remoteIdentifier);
            }

            // this can occur sometimes when if 2 or more remote node attempt to use node discovery
            // at the same time

            final SocketAddress remoteAddress = socketChannel.getRemoteAddress();

            if ((remoteNodeValidator != null &&
                    !remoteNodeValidator.validate(remoteIdentifier, remoteAddress))
                    || remoteIdentifier == localIdentifier)

                // throwing this exception will cause us to disconnect, both the client and server
                // will be able to detect the the remote and local identifiers are the same,
                // as the identifier is send early on in the hand shaking via the connect(()
                // and accept() methods
                throw new IllegalStateException("dropping connection, " +
                        "as the remote-identifier is already being used, identifier=" +
                        remoteIdentifier);
            if (LOG.isDebugEnabled())
                LOG.debug("handshaking for localIdentifier=" + localIdentifier + "," +
                        "remoteIdentifier=" + remoteIdentifier);

            attached.remoteModificationIterator =
                    replica.acquireModificationIterator(remoteIdentifier);

            attached.remoteModificationIterator.setModificationNotifier(attached);

            writer.writeRemoteBootstrapTimestamp(replica.lastModificationTime(remoteIdentifier));

            writer.writeServerVersion();

            // tell the remote node, what are heartbeat interval is
            writer.writeRemoteHeartbeatInterval(heartBeatIntervalMillis);
        }

        if (attached.remoteBootstrapTimestamp == Long.MIN_VALUE) {
            attached.remoteBootstrapTimestamp = reader.remoteBootstrapTimestamp();
            if (attached.remoteBootstrapTimestamp == Long.MIN_VALUE)
                return;
        }

        if (attached.serverVersion == null) {
            try {
                attached.serverVersion = reader.readRemoteServerVersion();
            } catch (IllegalStateException e1) {
                socketChannel.close();
                return;
            }

            if (attached.serverVersion == null)
                return;

            if (!isValidVersionNumber(attached.serverVersion)) {
                LOG.warn("Closing the remote connection : Please check that you don't have " +
                        "a third party system incorrectly connecting to ChronicleMap, " +
                        "remoteAddress=" + socketChannel.getRemoteAddress() + ", " +
                        "so closing the remote connection as Chronicle can not make sense " +
                        "of the remote version number received from the external connection, " +
                        "version=" + attached.serverVersion + ", " +
                        "Chronicle is expecting the version number to only contain " +
                        "'.','-', ,A-Z,a-z,0-9");
                socketChannel.close();
                return;
            }

            checkVersions(attached);
        }

        if (!attached.hasRemoteHeartbeatInterval) {
            final long value = reader.readRemoteHeartbeatIntervalFromBuffer();

            if (value == Long.MIN_VALUE)
                return;

            if (value < 0) {
                LOG.error("value=" + value);
            }

            // we add a 10% safety margin to the timeout time due to latency fluctuations
            // on the network, in other words we wont consider a connection to have
            // timed out, unless the heartbeat interval has exceeded 25% of the expected time.
            attached.remoteHeartbeatInterval = (long) (value * 1.25);

            // we have to make our selector poll interval at least as short as the minimum selector
            // timeout
            selectorTimeout = Math.min(selectorTimeout, value);

            if (selectorTimeout < 0)
                LOG.info("");

            attached.hasRemoteHeartbeatInterval = true;

            // now we're finished we can get on with reading the entries
            attached.remoteModificationIterator.dirtyEntries(attached.remoteBootstrapTimestamp);
            try {
                reader.entriesFromBuffer(attached, key);
            } finally {
                attached.handShakingComplete = true;
            }
        }
    }

    /**
     * called when the selector receives a OP_WRITE message
     */
    private void onWrite(@NotNull final SelectionKey key,
                         final long approxTime) throws IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();
        if (attached == null) {
            LOG.info("Closing connection " + socketChannel + ", nothing attached");
            socketChannel.close();
            return;
        }

        TcpSocketChannelEntryWriter entryWriter = attached.entryWriter;

        if (entryWriter == null) throw
                new NullPointerException("No entryWriter");

        if (entryWriter.isWorkIncomplete()) {
            final boolean completed = entryWriter.doWork();

            if (completed)
                entryWriter.workCompleted();

        } else if (attached.remoteModificationIterator != null)
            entryWriter.entriesToBuffer(attached.remoteModificationIterator);

        try {
            final int len = entryWriter.writeBufferToSocket(socketChannel, approxTime);

            if (len == -1)
                socketChannel.close();

            if (len > 0)
                contemplateThrottleWrites(len);

            if (!entryWriter.hasBytesToWrite()
                    && !entryWriter.isWorkIncomplete()
                    && !hasNext(attached)
                    && attached.isHandShakingComplete()) {
                // if we have no more data to write to the socket then we will
                // un-register OP_WRITE on the selector, until more data becomes available
                // The OP_WRITE  will get added back in as soon as we have data to write

                if (LOG.isDebugEnabled())
                    LOG.debug("Disabling OP_WRITE to remoteIdentifier=" +
                            attached.remoteIdentifier +
                            ", localIdentifier=" + localIdentifier);
                key.interestOps(key.interestOps() & ~OP_WRITE);
            }
        } catch (IOException e) {
            quietClose(key, e);
            if (!attached.isServer)
                attached.connector.connectLater();
            throw e;
        }
    }

    private boolean hasNext(Attached attached) {
        return attached.remoteModificationIterator != null
                && attached.remoteModificationIterator.hasNext();
    }

    /**
     * called when the selector receives a OP_READ message
     */

    private void onRead(@NotNull final SelectionKey key,
                        final long approxTime) throws IOException, InterruptedException {

        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        if (attached == null) {
            LOG.info("Closing connection " + socketChannel + ", nothing attached");
            socketChannel.close();
            return;
        }

        try {

            int len = attached.entryReader.readSocketToBuffer(socketChannel);
            if (len == -1) {
                socketChannel.register(selector, 0, attached);
                if (replicationConfig.autoReconnectedUponDroppedConnection()) {
                    AbstractConnector connector = attached.connector;
                    if (connector != null)
                        connector.connectLater();
                } else
                    socketChannel.close();
                return;
            }

            if (len == 0)
                return;

            if (attached.entryWriter.isWorkIncomplete())
                return;
        } catch (IOException e) {
            if (!attached.isServer)
                attached.connector.connectLater();
            throw e;
        }

        if (LOG.isDebugEnabled())
            LOG.debug("heartbeat or data received.");

        attached.entryReader.lastHeartBeatReceived = approxTime;

        if (attached.isHandShakingComplete()) {
            attached.entryReader.entriesFromBuffer(attached, key);
        } else {
            doHandShaking(key, socketChannel);
        }
    }

    @Nullable
    private ServerSocketChannel openServerSocketChannel() throws IOException {
        ServerSocketChannel result = null;

        try {
            result = ServerSocketChannel.open();
        } finally {
            if (result != null)
                closeables.add(result);
        }
        return result;
    }

    /**
     * sets interestOps to "selector keys",The change to interestOps much be on the same thread as
     * the selector. This class, allows via {@link AbstractChannelReplicator
     * .KeyInterestUpdater#set(int)} to holds a pending change  in interestOps ( via a bitset ),
     * this change is processed later on the same thread as the selector
     */
    private static class KeyInterestUpdater {

        @NotNull
        private final DirectBitSet changeOfOpWriteRequired;
        @NotNull
        private final SelectionKey[] selectionKeys;
        private final int op;

        KeyInterestUpdater(int op, @NotNull final SelectionKey[] selectionKeys) {
            this.op = op;
            this.selectionKeys = selectionKeys;
            long bitSetSize = LONGS.align(BYTES.alignAndConvert(selectionKeys.length, BITS), BYTES);
            changeOfOpWriteRequired = new ATSDirectBitSet(DirectStore.allocate(bitSetSize).bytes());
        }

        public void applyUpdates() {
            for (long i = changeOfOpWriteRequired.clearNextSetBit(0L); i >= 0;
                 i = changeOfOpWriteRequired.clearNextSetBit(i + 1)) {
                final SelectionKey key = selectionKeys[((int) i)];
                try {
                    key.interestOps(key.interestOps() | op);
                } catch (Exception e) {
                    LOG.debug("", e);
                }
            }
        }

        /**
         * @param keyIndex the index of the key that has changed, the list of keys is provided by
         *                 the constructor {@link KeyInterestUpdater(int, SelectionKey[])}
         */
        public void set(int keyIndex) {
            changeOfOpWriteRequired.setIfClear(keyIndex);
        }
    }

    private class ServerConnector extends AbstractConnector {

        @NotNull
        private final Details details;

        private ServerConnector(@NotNull Details details) {
            super("TCP-ServerConnector-" + localIdentifier);
            this.details = details;
        }

        @NotNull
        @Override
        public String toString() {
            return "ServerConnector{" +
                    "" + details +
                    '}';
        }

        @Nullable
        SelectableChannel doConnect() throws
                IOException, InterruptedException {

            final ServerSocketChannel serverChannel = openServerSocketChannel();

            if (serverChannel == null)
                return null;

            serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);
            serverChannel.register(TcpReplicator.this.selector, 0);
            ServerSocket serverSocket = null;

            try {
                serverSocket = serverChannel.socket();
            } finally {
                if (serverSocket != null)
                    closeables.add(serverSocket);
            }

            if (serverSocket == null)
                return null;

            serverSocket.setReuseAddress(true);

            serverSocket.bind(details.address());

            // these can be run on this thread
            addPendingRegistration(() -> {
                final Attached attached = new Attached();
                attached.connector = ServerConnector.this;
                try {
                    serverChannel.register(TcpReplicator.this.selector, OP_ACCEPT, attached);
                } catch (ClosedChannelException e) {
                    LOG.debug("", e);
                }
            });

            selector.wakeup();

            return serverChannel;
        }
    }

    private class ClientConnector extends AbstractConnector {

        @NotNull
        private final Details details;

        private ClientConnector(@NotNull Details details) {
            super("TCP-ClientConnector-" + details.localIdentifier());
            this.details = details;
        }

        @NotNull
        @Override
        public String toString() {
            return "ClientConnector{" + details + '}';
        }

        /**
         * blocks until connected
         */
        @Override
        SelectableChannel doConnect() throws IOException, InterruptedException {
            boolean success = false;

            final SocketChannel socketChannel = openSocketChannel(TcpReplicator.this.closeables);

            try {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setReuseAddress(true);
                socketChannel.socket().setSoLinger(false, 0);
                socketChannel.socket().setSoTimeout(0);

                socketChannel.connect(details.address());

                // Under experiment, the concoction was found to be more successful if we
                // paused before registering the OP_CONNECT
                Thread.sleep(10);

                // the registration has be be run on the same thread as the selector
                addPendingRegistration(new Runnable() {
                    @Override
                    public void run() {
                        final Attached attached = new Attached();
                        attached.connector = ClientConnector.this;

                        try {
                            socketChannel.register(selector, OP_CONNECT, attached);
                        } catch (ClosedChannelException e) {
                            if (socketChannel.isOpen())
                                LOG.error("", e);
                        }
                    }
                });

                selector.wakeup();
                success = true;
                return socketChannel;
            } finally {
                if (!success) {
                    try {
                        try {
                            socketChannel.socket().close();
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                        socketChannel.close();
                    } catch (IOException e) {
                        LOG.error("", e);
                    }
                    this.connectLater();
                }
            }
        }
    }

    /**
     * Attached to the NIO selection key via methods such as {@link SelectionKey#attach(Object)}
     */
    class Attached<K, V> implements Replica.ModificationNotifier {

        public TcpSocketChannelEntryReader entryReader;
        public TcpSocketChannelEntryWriter entryWriter;

        @Nullable
        public Replica.ModificationIterator remoteModificationIterator;
        // used for connect later
        public AbstractConnector connector;
        public long remoteBootstrapTimestamp = Long.MIN_VALUE;
        public byte remoteIdentifier = Byte.MIN_VALUE;
        public boolean hasRemoteHeartbeatInterval;
        // true if its socket is a ServerSocket
        public boolean isServer;
        public boolean handShakingComplete;
        public String serverVersion;
        public long remoteHeartbeatInterval = heartBeatIntervalMillis;

        boolean isHandShakingComplete() {
            return handShakingComplete;
        }

        /**
         * called whenever there is a change to the modification iterator
         */
        @Override
        public void onChange() {
            if (remoteIdentifier != Byte.MIN_VALUE)
                TcpReplicator.this.opWriteUpdater.set(remoteIdentifier);
        }
    }

    /**
     * @author Rob Austin.
     */
    public class TcpSocketChannelEntryWriter {


        @NotNull
        private final EntryCallback entryCallback;
        // if uncompletedWork is set ( not null ) , this must be completed before any further work
        // is  carried out
        @Nullable
        public Work uncompletedWork;
        private long lastSentTime;

        private TcpSocketChannelEntryWriter() {
            entryCallback = new EntryCallback(externalizable, replicationConfig.tcpBufferSize());
        }

        public boolean isWorkIncomplete() {
            return uncompletedWork != null;
        }

        public void workCompleted() {
            uncompletedWork = null;
        }

        /**
         * writes the timestamp into the buffer
         *
         * @param localIdentifier the current nodes identifier
         */
        void identifierToBuffer(final byte localIdentifier) {
            in().writeByte(localIdentifier);
        }

        public void ensureBufferSize(long size) {
            if (in().remaining() < size) {
                size += entryCallback.in().position();
                if (size > Integer.MAX_VALUE)
                    throw new UnsupportedOperationException();
                entryCallback.resizeBuffer((int) size);
            }
        }

        void resizeToMessage(@NotNull IllegalStateException e) {

            String message = e.getMessage();
            if (message.startsWith("java.io.IOException: Not enough available space for writing ")) {
                String substring = message.substring("java.io.IOException: Not enough available space for writing ".length(), message.length());
                int i = substring.indexOf(' ');
                if (i != -1) {
                    int size = Integer.parseInt(substring.substring(0, i));

                    long requiresExtra = size - in().remaining();
                    ensureBufferSize((int) (in().capacity() + requiresExtra));
                } else
                    throw e;
            } else
                throw e;
        }

        public Bytes in() {
            return entryCallback.in();
        }

        private ByteBuffer out() {
            return entryCallback.out();
        }

        /**
         * sends the identity and timestamp of this node to a remote node
         *
         * @param timeStampOfLastMessage the last timestamp we received a message from that node
         */
        void writeRemoteBootstrapTimestamp(final long timeStampOfLastMessage) {
            in().writeLong(timeStampOfLastMessage);
        }

        void writeServerVersion() {
            in().write(String.format("%1$" + 64 + "s", version()).toCharArray());
        }

        /**
         * writes all the entries that have changed, to the buffer which will later be written to
         * TCP/IP
         *
         * @param modificationIterator a record of which entries have modification
         */
        void entriesToBuffer(@NotNull final Replica.ModificationIterator modificationIterator) {

            int entriesWritten = 0;
            try {
                for (; ; entriesWritten++) {

                    long start = in().position();

                    boolean success = modificationIterator.nextEntry(entryCallback, 0);

                    // if not success this is most likely due to the next entry not fitting into
                    // the buffer and the buffer can not be re-sized above Integer.max_value, in
                    // this case success will return false, so we return so that we can send to
                    // the socket what we have.
                    if (!success)
                        return;

                    long entrySize = in().position() - start;

                    if (entrySize > largestEntrySoFar)
                        largestEntrySoFar = entrySize;

                    // we've filled up the buffer lets give another channel a chance to send
                    // some data
                    if (in().remaining() <= largestEntrySoFar || in().position() >
                            replicationConfig.tcpBufferSize())
                        return;

                    // if we have space in the buffer to write more data and we just wrote data
                    // into the buffer then let try and write some more
                }
            } finally {
                if (LOG.isDebugEnabled())
                    LOG.debug("Entries written: {}", entriesWritten);
            }
        }

        /**
         * writes the contents of the buffer to the socket
         *
         * @param socketChannel the socket to publish the buffer to
         * @param approxTime    an approximation of the current time in millis
         * @throws IOException
         */
        private int writeBufferToSocket(@NotNull final SocketChannel socketChannel,
                                        final long approxTime) throws IOException {

            final Bytes in = in();
            final ByteBuffer out = out();

            if (in.position() == 0)
                return 0;

            // if we still have some unwritten writer from last time
            lastSentTime = approxTime;
            assert in.position() <= Integer.MAX_VALUE;
            int size = (int) in.position();

            out.limit(size);

            final int len = socketChannel.write(out);

            if (LOG.isDebugEnabled())
                LOG.debug("bytes-written=" + len);

            if (len == size) {
                out.clear();
                in.clear();
            } else {
                out.compact();
                in.position(out.position());
                in.limit(in.capacity());
                out.clear();
            }

            return len;
        }


        /**
         * used to send an single zero byte if we have not send any data for up to the
         * localHeartbeatInterval
         */
        private void writeHeartbeatToBuffer() {
            // denotes the state - 0 for a heartbeat
            in().writeByte(HEARTBEAT);

            // denotes the size in bytes
            in().writeInt(0);
        }

        private void writeRemoteHeartbeatInterval(long localHeartbeatInterval) {
            in().writeLong(localHeartbeatInterval);
        }


        public boolean doWork() {
            return uncompletedWork != null && uncompletedWork.doWork(in());
        }

        public boolean hasBytesToWrite() {
            return in().position() > 0;
        }
    }

    /**
     * Reads map entries from a socket, this could be a client or server socket
     */
    class TcpSocketChannelEntryReader {
        public static final int HEADROOM = 1024;
        public static final int SIZE_OF_BOOTSTRAP_TIMESTAMP = 8;
        public long lastHeartBeatReceived = System.currentTimeMillis();
        ByteBuffer in;
        ByteBufferBytes out;
        private long sizeInBytes;
        private byte state;

        private TcpSocketChannelEntryReader() {
            in = ByteBuffer.allocateDirect(replicationConfig.tcpBufferSize());
            out = new ByteBufferBytes(in.slice());
            out.limit(0);
            in.clear();
        }

        void resizeBuffer(long size) {
            assert size < Integer.MAX_VALUE;

            if (size < in.capacity())
                throw new IllegalStateException("it not possible to resize the buffer smaller");

            final ByteBuffer buffer = ByteBuffer.allocateDirect((int) size)
                    .order(ByteOrder.nativeOrder());

            final int inPosition = in.position();

            long outPosition = out.position();
            long outLimit = out.limit();

            out = new ByteBufferBytes(buffer.slice());

            // TODO why copy byte by byte?!
            in.position(0);
            for (int i = 0; i < inPosition; i++) {
                buffer.put(in.get());
            }

            in = buffer;
            in.limit(in.capacity());
            in.position(inPosition);

            out.limit(outLimit);
            out.position(outPosition);
        }

        /**
         * reads from the socket and writes them to the buffer
         *
         * @param socketChannel the  socketChannel to read from
         * @return the number of bytes read
         * @throws IOException
         */
        private int readSocketToBuffer(@NotNull final SocketChannel socketChannel)
                throws IOException {

            compactBuffer();
            final int len = socketChannel.read(in);
            out.limit(in.position());
            return len;
        }

        /**
         * reads entries from the buffer till empty
         *
         * @param attached
         * @param key
         * @throws InterruptedException
         */
        void entriesFromBuffer(@NotNull Attached attached, @NotNull SelectionKey key) {
            int entriesRead = 0;
            try {
                for (; ; entriesRead++) {
                    out.limit(in.position());

                    // its set to MIN_VALUE when it should be read again
                    if (state == NOT_SET) {
                        if (out.remaining() < SIZE_OF_SIZE + 1)
                            return;

                        // state is used for heartbeat
                        state = out.readByte();
                        sizeInBytes = out.readInt();

                        assert sizeInBytes >= 0;

                        // if the buffer is too small to read this payload we will have to grow the
                        // size of the buffer
                        long requiredSize = sizeInBytes + SIZE_OF_SIZE + 1 +
                                SIZE_OF_BOOTSTRAP_TIMESTAMP;
                        if (out.capacity() < requiredSize) {
                            attached.entryReader.resizeBuffer(requiredSize + HEADROOM);
                        }

                        // means this is heartbeat
                        if (state == NOT_SET)
                            continue;
                    }

                    if (out.remaining() < sizeInBytes) {
                        return;
                    }

                    final long nextEntryPos = out.position() + sizeInBytes;
                    assert nextEntryPos > 0;
                    final long limit = out.limit();
                    out.limit(nextEntryPos);

                    externalizable.readExternalEntry(out);

                    out.limit(limit);

                    // skip onto the next entry
                    out.position(nextEntryPos);

                    state = NOT_SET;
                    sizeInBytes = 0;
                }
            } finally {
                if (LOG.isDebugEnabled())
                    LOG.debug("Entries read: {}", entriesRead);
            }
        }

        /**
         * compacts the buffer and updates the {@code in} and {@code out} accordingly
         */
        private void compactBuffer() {
            // the maxEntrySizeBytes used here may not be the maximum size of the entry in its
            // serialized form however, its only use as an indication that the buffer is becoming
            // full and should be compacted the buffer can be compacted at any time
            if (in.position() == 0 || in.remaining() > largestEntrySoFar)
                return;

            in.limit(in.position());
            assert out.position() < Integer.MAX_VALUE;
            in.position((int) out.position());

            in.compact();
            out.position(0);
        }

        /**
         * @return the identifier or -1 if unsuccessful
         */
        byte identifierFromBuffer() {
            return (out.remaining() >= 1) ? out.readByte() : Byte.MIN_VALUE;
        }

        /**
         * @return the timestamp or -1 if unsuccessful
         */
        long remoteBootstrapTimestamp() {
            if (out.remaining() >= 8)
                return out.readLong();
            else
                return Long.MIN_VALUE;
        }

        /**
         * @return the timestamp or -1 if unsuccessful
         */
        String readRemoteServerVersion() {
            if (out.remaining() >= 64) {
                char[] chars = new char[64];
                out.readFully(chars, 0, chars.length);
                return new String(chars).trim();
            } else
                return null;
        }


        public long readRemoteHeartbeatIntervalFromBuffer() {
            return (out.remaining() >= 8) ? out.readLong() : Long.MIN_VALUE;
        }
    }
}

