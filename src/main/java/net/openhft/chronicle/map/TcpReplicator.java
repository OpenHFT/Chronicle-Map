/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.RemoteNodeValidator;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.ThrottlingConfig;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.channels.SelectionKey.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_SIZE;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_TRANSACTION_ID;
import static net.openhft.chronicle.map.StatelessChronicleMap.EventId.HEARTBEAT;

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
final class TcpReplicator extends AbstractChannelReplicator implements Closeable {

    public static final int STATELESS_CLIENT = -127;
    public static final byte NOT_SET = (byte) HEARTBEAT.ordinal();
    private static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1MB
    public static final int SPIN_LOOP_COUNT = 100000;
    private final SelectionKey[] selectionKeysStore = new SelectionKey[Byte.MAX_VALUE + 1];
    // used to instruct the selector thread to set OP_WRITE on a key correlated by the bit index in the
    // bitset
    private final KeyInterestUpdater opWriteUpdater = new KeyInterestUpdater(OP_WRITE, selectionKeysStore);
    private final BitSet activeKeys = new BitSet(selectionKeysStore.length);
    private final long heartBeatIntervalMillis;

    @NotNull
    private final Replica replica;
    private final byte localIdentifier;
    private final int maxEntrySizeBytes;
    @NotNull
    private final Replica.EntryExternalizable externalizable;
    @NotNull
    private final TcpTransportAndNetworkConfig replicationConfig;
    @Nullable
    private final StatelessServerConnector statelessServerConnector;
    private final
    @Nullable
    RemoteNodeValidator remoteNodeValidator;
    private final String name;

    private long selectorTimeout;

    /**
     * @param maxEntrySizeBytes        used to check that the last entry will fit into the buffer,
     *                                 it can not be smaller than the size of and entry, if it is
     *                                 set smaller the buffer will over flow, it can be larger then
     *                                 the entry, but setting it too large reduces the workable
     *                                 space in the buffer.
     * @param statelessServerConnector set to NULL if not required
     * @throws IOException on an io error.
     */
    public TcpReplicator(@NotNull final Replica replica,
                         @NotNull final Replica.EntryExternalizable externalizable,
                         @NotNull final TcpTransportAndNetworkConfig replicationConfig,
                         final int maxEntrySizeBytes,
                         @Nullable StatelessServerConnector statelessServerConnector,
                         @Nullable RemoteNodeValidator remoteNodeValidator) throws IOException {

        super("TcpSocketReplicator-" + replica.identifier(), replicationConfig.throttlingConfig(),
                maxEntrySizeBytes);
        this.statelessServerConnector = statelessServerConnector;

        final ThrottlingConfig throttlingConfig = replicationConfig.throttlingConfig();
        long throttleBucketInterval = throttlingConfig.bucketInterval(MILLISECONDS);

        heartBeatIntervalMillis = replicationConfig.heartBeatInterval(MILLISECONDS);

        selectorTimeout = Math.min(heartBeatIntervalMillis / 4, throttleBucketInterval);

        this.replica = replica;
        this.localIdentifier = replica.identifier();
        this.maxEntrySizeBytes = maxEntrySizeBytes;
        this.externalizable = externalizable;
        this.replicationConfig = replicationConfig;

        this.remoteNodeValidator = remoteNodeValidator;
        this.name = replicationConfig.name();
        start();
    }

    @Override
    void processEvent() throws IOException {
        try {
            final InetSocketAddress serverInetSocketAddress = new InetSocketAddress(replicationConfig
                    .serverPort());
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
        } catch (CancelledKeyException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (ClosedSelectorException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (ClosedChannelException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (ConnectException e) {
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
        } catch (BufferUnderflowException e) {
            if (!isClosed)
                quietClose(key, e);
        } catch (CancelledKeyException e) {
            if (!isClosed)
                quietClose(key, e);
        } catch (ClosedSelectorException e) {
            if (!isClosed)
                quietClose(key, e);
        } catch (IOException e) {
            if (!isClosed)
                quietClose(key, e);
        } catch (InterruptedException e) {
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
     * @return
     * @throws IOException
     */
    private int select() throws IOException {

        // spin loop 100000 times
        for (int i = 0; i < SPIN_LOOP_COUNT; i++) {
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
                                           final long approxTimeOutTime) throws ConnectException {

        final Attached attached = (Attached) key.attachment();

        // we wont attempt to reconnect the server socket
        if (attached.isServer || !attached.isHandShakingComplete())
            return;

        final SocketChannel channel = (SocketChannel) key.channel();

        if (approxTimeOutTime >
                attached.entryReader.lastHeartBeatReceived + attached.remoteHeartbeatInterval) {
            if (LOG.isDebugEnabled())
                LOG.debug("lost connection, attempting to reconnect. " +
                        "missed heartbeat from identifier=" + attached.remoteIdentifier);
            activeKeys.clear(attached.remoteIdentifier);
            closeables.closeQuietly(channel.socket());

            // when node discovery is used ( by nodes broadcasting out their host:port over UDP ),
            // when new or restarted nodes are started up. they attempt to find the nodes
            // on the grid by listening to the host and ports of the other nodes, so these nodes will establish the connection when they come back up,
            // hence under these circumstances, polling a dropped node to attempt to reconnect is no-longer
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
    private void quietClose(@NotNull final SelectionKey key, @NotNull final Exception e) {
        if (LOG.isDebugEnabled())
            LOG.debug("", e);
        closeEarlyAndQuietly(key.channel());
    }

    /**
     * called when the selector receives a OP_CONNECT message
     */
    private void onConnect(@NotNull final SelectionKey key)
            throws IOException, InterruptedException {

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
            // hence under these circumstances, polling a dropped node to attempt to reconnect is no-longer
            // required as the remote node will establish the connection its self on startup.
            if (replicationConfig.autoReconnectedUponDroppedConnection())
                attached.connector.connect();

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
     * this can be called when a new CHM is added to a cluster, we have to rebootstrap so will clear
     * all the old bootstrap information
     *
     * @param key the nio SelectionKey
     */
    private void clearHandshaking(@NotNull SelectionKey key) {
        final Attached attached = (Attached) key.attachment();
        activeKeys.clear(attached.remoteIdentifier);
        selectionKeysStore[attached.remoteIdentifier] = null;
        attached.clearHandShaking();
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
    private void doHandShaking(@NotNull final SelectionKey key, @NotNull SocketChannel socketChannel)
            throws IOException, InterruptedException {
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

            // we use the as iterating the activeKeys via the bitset wont create and Objects
            // but if we use the selector.keys() this will.
            selectionKeysStore[remoteIdentifier] = key;
            activeKeys.set(remoteIdentifier);

            if (LOG.isDebugEnabled()) {
                LOG.debug("server-connection id={}, remoteIdentifier={}", localIdentifier, remoteIdentifier);
            }

            // this can occur sometimes when if 2 or more remote node attempt to use node discovery at the
            // same time

            final SocketAddress remoteAddress = socketChannel.getRemoteAddress();

            if ((remoteNodeValidator != null &&
                    !remoteNodeValidator.validate(remoteIdentifier, remoteAddress))
                    || remoteIdentifier == localIdentifier)

                // throwing this exception will cause us to disconnect, both the client and server
                // will be able to detect the the remote and local identifiers are the same,
                // as the identifier is send early on in the hand shaking via the connect(() and accept()
                // methods
                throw new IllegalStateException("dropping connection, " +
                        "as the remote-identifier is already being used, identifier=" + remoteIdentifier);
            if (LOG.isDebugEnabled())
                LOG.debug("handshaking for localIdentifier=" + localIdentifier + "," +
                        "remoteIdentifier=" + remoteIdentifier);

            attached.remoteModificationIterator = replica.acquireModificationIterator(remoteIdentifier,
                    attached);

            writer.writeRemoteBootstrapTimestamp(replica.lastModificationTime(remoteIdentifier));

            // tell the remote node, what are heartbeat interval is
            writer.writeRemoteHeartbeatInterval(heartBeatIntervalMillis);
        }

        if (attached.remoteBootstrapTimestamp == Long.MIN_VALUE) {
            attached.remoteBootstrapTimestamp = reader.remoteBootstrapTimestamp();
            if (attached.remoteBootstrapTimestamp == Long.MIN_VALUE)
                return;
        }

        if (!attached.hasRemoteHeartbeatInterval) {
            final long value = reader.remoteHeartbeatIntervalFromBuffer();

            if (value == Long.MIN_VALUE)
                return;

            if (value < 0) {
                LOG.error("value=" + value);
            }

            // we add a 10% safety margin to the timeout time due to latency fluctuations on the network,
            // in other words we wont consider a connection to have
            // timed out, unless the heartbeat interval has exceeded 25% of the expected time.
            attached.remoteHeartbeatInterval = (long) (value * 1.25);

            // we have to make our selector poll interval at least as short as the minimum selector timeout
            selectorTimeout = Math.min(selectorTimeout, value);

            if (selectorTimeout < 0)
                LOG.info("");

            attached.hasRemoteHeartbeatInterval = true;

            // now we're finished we can get on with reading the entries
            attached.handShakingComplete = true;
            attached.remoteModificationIterator.dirtyEntries(attached.remoteBootstrapTimestamp);
            reader.entriesFromBuffer(attached, key);
        }
    }

    /**
     * called when the selector receives a OP_WRITE message
     */
    private void onWrite(@NotNull final SelectionKey key,
                         final long approxTime) throws InterruptedException, IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();
        if (attached == null) throw new NullPointerException("No attached");
        TcpSocketChannelEntryWriter entryWriter = attached.entryWriter;
        if (entryWriter == null) throw new NullPointerException("No entryWriter");
        if (entryWriter.isWorkIncomplete()) {
            final boolean completed = entryWriter.doWork();

            if (completed)
                entryWriter.workCompleted();
        } else if (attached.remoteModificationIterator != null)
            entryWriter.entriesToBuffer(attached.remoteModificationIterator, key);

        try {
            final int len = entryWriter.writeBufferToSocket(socketChannel,
                    approxTime);

            if (len == -1)
                socketChannel.close();

            if (len > 0)
                contemplateThrottleWrites(len);

            if (!entryWriter.hasBytesToWrite() && !entryWriter.isWorkIncomplete())
                // TURN OP_WRITE_OFF
                key.interestOps(key.interestOps() & ~OP_WRITE);
        } catch (IOException e) {
            quietClose(key, e);
            if (!attached.isServer)
                attached.connector.connectLater();
            throw e;
        }
    }

    /**
     * called when the selector receives a OP_READ message
     */

    private void onRead(@NotNull final SelectionKey key,
                        final long approxTime) throws IOException, InterruptedException {

        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        try {

            int len = attached.entryReader.readSocketToBuffer(socketChannel);
            if (len == -1) {
                socketChannel.register(selector, 0);
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
     * .KeyInterestUpdater#set(int)}  to holds a pending change  in interestOps ( via a bitset ),
     * this change is processed later on the same thread as the selector
     */
    private static class KeyInterestUpdater {

        private final AtomicBoolean wasChanged = new AtomicBoolean();
        @NotNull
        private final BitSet changeOfOpWriteRequired;
        @NotNull
        private final SelectionKey[] selectionKeys;
        private final int op;

        KeyInterestUpdater(int op, @NotNull final SelectionKey[] selectionKeys) {
            this.op = op;
            this.selectionKeys = selectionKeys;
            changeOfOpWriteRequired = new BitSet(selectionKeys.length);
        }

        public void applyUpdates() {
            if (wasChanged.getAndSet(false)) {
                for (int i = changeOfOpWriteRequired.nextSetBit(0); i >= 0;
                     i = changeOfOpWriteRequired.nextSetBit(i + 1)) {
                    changeOfOpWriteRequired.clear(i);
                    final SelectionKey key = selectionKeys[i];
                    try {
                        key.interestOps(key.interestOps() | op);
                    } catch (Exception e) {
                        LOG.debug("", e);
                    }
                }
            }
        }

        /**
         * @param keyIndex the index of the key that has changed, the list of keys is provided by
         *                 the constructor {@link KeyInterestUpdater(int, SelectionKey[])}
         */
        public void set(int keyIndex) {
            changeOfOpWriteRequired.set(keyIndex);
            wasChanged.lazySet(true);
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

            serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);
            ServerSocket serverSocket = null;

            try {
                serverSocket = serverChannel.socket();
            } finally {
                if (serverSocket != null)
                    closeables.add(serverSocket);
            }

            serverSocket.setReuseAddress(true);

            serverSocket.bind(details.address());

            // these can be run on this thread
            addPendingRegistration(new Runnable() {
                @Override
                public void run() {
                    final Attached attached = new Attached();
                    attached.connector = ServerConnector.this;
                    try {
                        serverChannel.register(TcpReplicator.this.selector, OP_ACCEPT, attached);
                    } catch (ClosedChannelException e) {
                        LOG.debug("", e);
                    }
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

                try {
                    socketChannel.connect(details.address());
                } catch (UnresolvedAddressException e) {
                    this.connectLater();
                }

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
                }
            }
        }
    }

    /**
     * Attached to the NIO selection key via methods such as {@link SelectionKey#attach(Object)}
     */
    class Attached implements Replica.ModificationNotifier {

        public TcpSocketChannelEntryReader entryReader;
        public TcpSocketChannelEntryWriter entryWriter;

        @Nullable
        public Replica.ModificationIterator remoteModificationIterator;
        public AbstractConnector connector;
        public long remoteBootstrapTimestamp = Long.MIN_VALUE;
        public byte remoteIdentifier = Byte.MIN_VALUE;
        public boolean hasRemoteHeartbeatInterval;
        // true if its socket is a ServerSocket
        public boolean isServer;        // the frequency the remote node will send a heartbeat
        public boolean handShakingComplete;
        public long remoteHeartbeatInterval = heartBeatIntervalMillis;

        boolean isHandShakingComplete() {
            return handShakingComplete;
        }

        void clearHandShaking() {
            handShakingComplete = false;

            remoteIdentifier = Byte.MIN_VALUE;
            remoteBootstrapTimestamp = Long.MIN_VALUE;
            remoteHeartbeatInterval = heartBeatIntervalMillis;
            hasRemoteHeartbeatInterval = false;
            remoteModificationIterator = null;
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
    private class TcpSocketChannelEntryWriter {

        @NotNull
        final ByteBufferBytes in;
        @NotNull
        private final ByteBuffer out;
        @NotNull
        private final EntryCallback entryCallback;
        // if uncompletedWork is set ( not null ) , this must be completed before any further work
        // is  carried out
        @Nullable
        public Work uncompletedWork;
        private long lastSentTime;

        private TcpSocketChannelEntryWriter() {
            out = ByteBuffer.allocateDirect(replicationConfig.packetSize() + maxEntrySizeBytes);
            in = new ByteBufferBytes(out);
            entryCallback = new EntryCallback(externalizable, in);
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
            in.writeByte(localIdentifier);
        }

        /**
         * sends the identity and timestamp of this node to a remote node
         *
         * @param timeStampOfLastMessage the last timestamp we received a message from that node
         */
        void writeRemoteBootstrapTimestamp(final long timeStampOfLastMessage) {
            in.writeLong(timeStampOfLastMessage);
        }

        /**
         * writes all the entries that have changed, to the buffer which will later be written to
         * TCP/IP
         *
         * @param modificationIterator a record of which entries have modification
         * @param selectionKey
         */
        void entriesToBuffer(@NotNull final Replica.ModificationIterator modificationIterator,
                             @NotNull final SelectionKey selectionKey)
                throws InterruptedException, IOException {

            final SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
            final Attached attached = (Attached) selectionKey.attachment();

            // this can occur when new map are added to a channel
            final boolean handShakingComplete = attached.isHandShakingComplete();

            for (; ; ) {
                final boolean wasDataRead = modificationIterator.nextEntry(entryCallback, 0);

                if (!wasDataRead) {
                    // if we have no more data to write to the socket then we will
                    // un-register OP_WRITE on the selector, until more data becomes available
                    if (in.position() == 0 && handShakingComplete)
                        disableWrite(socketChannel, attached);

                    return;
                }

                // we've filled up the buffer lets give another channel a chance to send some data
                if (in.remaining() <= maxEntrySizeBytes)
                    return;

                // if we have space in the buffer to write more data and we just wrote data into the
                // buffer then let try and write some more
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
            in.writeByte(HEARTBEAT.ordinal());

            // denotes the size in bytes
            in.writeInt(0);
        }

        private void writeRemoteHeartbeatInterval(long localHeartbeatInterval) {
            in.writeLong(localHeartbeatInterval);
        }

        /**
         * removes back in the OP_WRITE from the selector, otherwise it'll spin loop. The OP_WRITE
         * will get added back in as soon as we have data to write
         *
         * @param socketChannel the socketChannel we wish to stop writing to
         * @param attached      data associated with the socketChannels key
         */
        public synchronized void disableWrite(@NotNull final SocketChannel socketChannel,
                                              @NotNull final Attached attached) {
            try {
                SelectionKey key = socketChannel.keyFor(selector);
                if (key != null) {
                    if (attached.isHandShakingComplete() && selector.isOpen()) {
                        if (LOG.isDebugEnabled())
                            LOG.debug("Disabling OP_WRITE to remoteIdentifier=" +
                                    attached.remoteIdentifier +
                                    ", localIdentifier=" + localIdentifier);
                        key.interestOps(key.interestOps() & ~OP_WRITE);
                    }
                }
            } catch (Exception e) {
                LOG.error("", e);
            }
        }

        public boolean doWork() {
            return uncompletedWork.doWork(in);
        }

        public boolean hasBytesToWrite() {
            return in.position() > 0;
        }
    }

    /**
     * Reads map entries from a socket, this could be a client or server socket
     */
    class TcpSocketChannelEntryReader {
        public static final int HEADROOM = 1024;
        public long lastHeartBeatReceived = System.currentTimeMillis();
        ByteBuffer in;
        ByteBufferBytes out;
        private long sizeInBytes;
        private byte state;

        private TcpSocketChannelEntryReader() {
            in = ByteBuffer.allocateDirect(replicationConfig.packetSize() + maxEntrySizeBytes);
            out = new ByteBufferBytes(in.slice());
            out.limit(0);
            in.clear();
        }

        void resizeBuffer(long size) {
            assert size < Integer.MAX_VALUE;

            if (size < in.capacity())
                throw new IllegalStateException("it not possible to resize the buffer smaller");

            final ByteBuffer buffer = ByteBuffer.allocateDirect((int) size).order(ByteOrder.nativeOrder());

            final int inPosition = in.position();

            long outPosition = out.position();
            long outLimit = out.limit();

            out = new ByteBufferBytes(buffer.slice());

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
        void entriesFromBuffer(@NotNull Attached attached, @NotNull SelectionKey key) throws InterruptedException, IOException {
            for (; ; ) {
                out.limit(in.position());

                // its set to MIN_VALUE when it should be read again
                if (state == NOT_SET) {
                    if (out.remaining() < SIZE_OF_SIZE + 1)
                        return;

                    // state is used for both heartbeat and stateless
                    state = out.readByte();
                    sizeInBytes = out.readInt();

                    assert sizeInBytes >= 0;

                    // if the buffer is too small to read this payload we will have to grow the
                    // size of the buffer
                    long requiredSize = sizeInBytes + SIZE_OF_SIZE + 1;
                    if (out.capacity() < requiredSize) {
                        attached.entryReader.resizeBuffer(requiredSize + HEADROOM);
                    }

                    // this is the :
                    //  -- heartbeat if its 0
                    //  -- stateful update if its 1
                    //  -- the id of the stateful event
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

                boolean isStatelessClient = (state != 1);

                if (isStatelessClient) {
                    if (statelessServerConnector == null) {
                        LOG.error("", new IllegalArgumentException("received an event " +
                                "from a stateless map, stateless maps are not " +
                                "currently supported when using Chronicle Channels"));
                    } else {

                        final Work futureWork = statelessServerConnector.processStatelessEvent(state,
                                attached.entryWriter.in, attached.entryReader.out);

                        // turn the OP_WRITE on
                        key.interestOps(key.interestOps() | OP_WRITE);

                        // in some cases it may not be possible to send out all the data before we
                        // fill out the write buffer, so this data will be send when the buffer
                        // is no longer full, and as such is treated as future work
                        if (futureWork != null) {
                            try {  // we will complete what we can for now
                                boolean isComplete = futureWork.doWork(attached.entryWriter.in);
                                if (!isComplete)
                                    attached.entryWriter.uncompletedWork = futureWork;
                            } catch (Exception e) {
                                LOG.error("", e);
                            }
                        }
                    }
                } else
                    externalizable.readExternalEntry(copies, segmentState, out);

                out.limit(limit);

                // skip onto the next entry
                out.position(nextEntryPos);

                state = NOT_SET;
                sizeInBytes = 0;
            }
        }

        /**
         * compacts the buffer and updates the {@code in} and {@code out} accordingly
         */
        private void compactBuffer() {
            // the maxEntrySizeBytes used here may not be the maximum size of the entry in its serialized form
            // however, its only use as an indication that the buffer is becoming full and should be compacted
            // the buffer can be compacted at any time
            if (in.position() == 0 || in.remaining() > maxEntrySizeBytes)
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

        public long remoteHeartbeatIntervalFromBuffer() {
            return (out.remaining() >= 8) ? out.readLong() : Long.MIN_VALUE;
        }
    }
}

/**
 * @author Rob Austin.
 */
class StatelessServerConnector<K, V> {

    public static final int TRANSACTION_ID_OFFSET = SIZE_OF_SIZE + 1;
    public static final boolean MAP_SUPPORTS_BYTES = true;
    private static final Logger LOG = LoggerFactory.getLogger(StatelessServerConnector.class
            .getName());
    public static final StatelessChronicleMap.EventId[] VALUES
            = StatelessChronicleMap.EventId.values();
    public static final int SIZE_OF_IS_EXCEPTION = 1;
    public static final int HEADER_SIZE = SIZE_OF_SIZE + SIZE_OF_IS_EXCEPTION + SIZE_OF_TRANSACTION_ID;

    @NotNull
    private final ReaderWithSize<K> keyReaderWithSize;
    @NotNull
    private final WriterWithSize<K> keyWriterWithSize;
    @NotNull
    private final ReaderWithSize<V> valueReaderWithSize;
    @NotNull
    private final WriterWithSize<V> valueWriterWithSize;
    @NotNull
    private final VanillaChronicleMap<K, ?, ?, V, ?, ?> map;
    private double maxEntrySizeBytes;

    StatelessServerConnector(
            SerializationBuilder<K> keySerializationBuilder,
            SerializationBuilder<V> valueSerializationBuilder,
            @NotNull VanillaChronicleMap<K, ?, ?, V, ?, ?> map, int maxEntrySizeBytes) {
        keyReaderWithSize = new ReaderWithSize<>(keySerializationBuilder);
        keyWriterWithSize = new WriterWithSize<>(keySerializationBuilder);
        valueReaderWithSize = new ReaderWithSize<>(valueSerializationBuilder);
        valueWriterWithSize = new WriterWithSize<>(valueSerializationBuilder);
        this.map = map;
        this.maxEntrySizeBytes = maxEntrySizeBytes;
    }

    @Nullable
    Work processStatelessEvent(final byte eventId,
                               @NotNull final Bytes writer,
                               @NotNull final ByteBufferBytes reader) {
        final StatelessChronicleMap.EventId event = VALUES[eventId];

        // these methods don't return a result
        switch (event) {
            case KEY_SET:
                return keySet(reader, writer);

            case VALUES:
                return values(reader, writer);

            case ENTRY_SET:
                return entrySet(reader, writer);

            case PUT_WITHOUT_ACC:
                return put(reader);

            case PUT_ALL_WITHOUT_ACC:
                return putAll(reader);

            case REMOVE_WITHOUT_ACC:
                return remove(reader);
        }

        final long sizeLocation = reflectTransactionId(reader, writer);

        // these methods return a result

        switch (event) {
            case LONG_SIZE:
                return longSize(reader, writer, sizeLocation);

            case IS_EMPTY:
                return isEmpty(reader, writer, sizeLocation);

            case CONTAINS_KEY:
                return containsKey(reader, writer, sizeLocation);

            case CONTAINS_VALUE:
                return containsValue(reader, writer, sizeLocation);

            case GET:
                return get(reader, writer, sizeLocation);

            case PUT:
                return put(reader, writer, sizeLocation);

            case REMOVE:
                return remove(reader, writer, sizeLocation);

            case CLEAR:
                return clear(reader, writer, sizeLocation);

            case REPLACE:
                return replace(reader, writer, sizeLocation);

            case REPLACE_WITH_OLD_AND_NEW_VALUE:
                return replaceWithOldAndNew(reader, writer, sizeLocation);

            case PUT_IF_ABSENT:
                return putIfAbsent(reader, writer, sizeLocation);

            case REMOVE_WITH_VALUE:
                return removeWithValue(reader, writer, sizeLocation);

            case TO_STRING:
                return toString(reader, writer, sizeLocation);

            case PUT_ALL:
                return putAll(reader, writer, sizeLocation);

            case HASH_CODE:
                return hashCode(reader, writer, sizeLocation);

            case MAP_FOR_KEY:
                return mapForKey(reader, writer, sizeLocation);

            case UPDATE_FOR_KEY:
                return updateForKey(reader, writer, sizeLocation);

            default:
                throw new IllegalStateException("unsupported event=" + event);
        }
    }

    @Nullable
    public Work mapForKey(@NotNull ByteBufferBytes reader, @NotNull Bytes writer, long sizeLocation) {
        final K key = keyReaderWithSize.read(reader, null);
        final Function<V, ?> function = (Function<V, ?>) reader.readObject();
        try {
            Object result = map.mapForKey(key, function);
            writer.writeObject(result);
        } catch (Throwable e) {
            LOG.info("", e);
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    public Work updateForKey(@NotNull ByteBufferBytes reader, @NotNull Bytes writer, long sizeLocation) {
        final K key = keyReaderWithSize.read(reader, null);
        final Mutator<V, ?> mutator = (Mutator<V, ?>) reader.readObject();
        try {
            Object result = map.updateForKey(key, mutator);
            writer.writeObject(result);
        } catch (Throwable e) {
            LOG.info("", e);
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work removeWithValue(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                writer.writeBoolean(map.removeWithValue(reader));
            } else {
                ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
                copies = valueReaderWithSize.getCopies(copies);
                final K key = keyReaderWithSize.read(reader, copies);
                final V readValue = valueReaderWithSize.read(reader, copies);
                writer.writeBoolean(map.remove(key, readValue));
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work replaceWithOldAndNew(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                map.replaceWithOldAndNew(reader, writer);
            } else {
                ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
                copies = valueReaderWithSize.getCopies(copies);
                final K key = keyReaderWithSize.read(reader, copies);
                final V oldValue = valueReaderWithSize.read(reader, copies);
                final V newValue = valueReaderWithSize.read(reader, copies);
                writer.writeBoolean(map.replace(key, oldValue, newValue));
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work longSize(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            writer.writeLong(map.longSize());
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work hashCode(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            writer.writeInt(map.hashCode());
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work toString(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        final String str;

        final long remaining = writer.remaining();
        try {
            str = map.toString();
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        assert remaining > 4;

        // this stops the toString overflowing the buffer
        final String result = (str.length() < remaining) ?
                str :
                str.substring(0, (int) (remaining - 4)) + "...";

        writer.writeObject(result);
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work sendException(@NotNull Bytes writer, long sizeLocation, @NotNull Throwable e) {
        // move the position to ignore any bytes written so far
        writer.position(sizeLocation + HEADER_SIZE);

        writeException(writer, e);

        writeSizeAndFlags(sizeLocation + SIZE_OF_TRANSACTION_ID, true, writer);

        e.printStackTrace();
        return null;
    }

    @Nullable
    private Work isEmpty(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            writer.writeBoolean(map.isEmpty());
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work containsKey(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                writer.writeBoolean(map.containsKey(reader));
            } else {
                final K k = keyReaderWithSize.read(reader, null);
                writer.writeBoolean(map.containsKey(k));
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work containsValue(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        // todo optimize -- eliminate
        final V v = valueReaderWithSize.read(reader, null);

        try {
            writer.writeBoolean(map.containsValue(v));
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work get(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                map.get(reader, writer);
            } else {
                final ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
                final K k = keyReaderWithSize.read(reader, copies);
                valueWriterWithSize.writeNullable(writer, map.get(k), copies);
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work put(Bytes reader) {
        if (MAP_SUPPORTS_BYTES) {
            map.put(reader);
        } else {
            ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
            copies = valueReaderWithSize.getCopies(copies);
            final K k = keyReaderWithSize.read(reader, copies);
            final V v = valueReaderWithSize.read(reader, copies);
            map.put(k, v);
        }

        return null;
    }

    @Nullable
    private Work put(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                map.put(reader, writer);
            } else {
                ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
                copies = valueReaderWithSize.getCopies(copies);
                final K k = keyReaderWithSize.read(reader, copies);
                final V v = valueReaderWithSize.read(reader, copies);
                valueWriterWithSize.writeNullable(writer, map.put(k, v), copies);
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work remove(Bytes reader) {
        if (MAP_SUPPORTS_BYTES) {
            map.removeKeyAsBytes(reader);
        } else {
            final K key = keyReaderWithSize.read(reader, null);
            map.remove(key);
        }

        return null;
    }

    @Nullable
    private Work remove(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                map.remove(reader, writer);
            } else {
                ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
                final K key = keyReaderWithSize.read(reader, copies);
                if (LOG.isDebugEnabled())
                    LOG.debug("removing entry key=" + key);
                valueWriterWithSize.writeNullable(writer, map.remove(key), copies);
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work putAll(@NotNull Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                map.putAll(reader);
            } else {
                final Map<K, V> m = readEntries(reader);
                map.putAll(m);
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work putAll(@NotNull Bytes reader) {
        if (MAP_SUPPORTS_BYTES) {
            map.putAll(reader);
        } else {
            map.putAll(readEntries(reader));
        }

        return null;
    }

    @Nullable
    private Work clear(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            map.clear();
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work values(@NotNull Bytes reader, @NotNull Bytes writer) {
        final long transactionId = reader.readLong();

        Collection<V> values;

        try {
            values = map.values();
        } catch (Throwable e) {
            return sendException(reader, writer, e);
        }

        final Iterator<V> iterator = values.iterator();

        // this allows us to write more data than the buffer will allow
        return new Work() {
            @Override
            public boolean doWork(@NotNull Bytes out) {
                final long sizeLocation = header(out, transactionId);

                ThreadLocalCopies copies = valueWriterWithSize.getCopies(null);
                Object valueWriter = valueWriterWithSize.writerForLoop(copies);

                int count = 0;
                while (iterator.hasNext()) {
                    // we've filled up the buffer, so lets give another channel a chance to send
                    // some data, we don't know the max key size, we will use the entrySize instead
                    if (out.remaining() <= maxEntrySizeBytes) {
                        writeHeader(out, sizeLocation, count, true);
                        return false;
                    }

                    count++;

                    valueWriterWithSize.writeInLoop(out, iterator.next(), valueWriter, copies);
                }

                writeHeader(out, sizeLocation, count, false);
                return true;
            }
        };
    }

    @Nullable
    private Work keySet(@NotNull Bytes reader, @NotNull final Bytes writer) {
        final long transactionId = reader.readLong();

        Set<K> ks;

        try {
            ks = map.keySet();
        } catch (Throwable e) {
            return sendException(reader, writer, e);
        }

        final Iterator<K> iterator = ks.iterator();

        // this allows us to write more data than the buffer will allow
        return new Work() {
            @Override
            public boolean doWork(@NotNull Bytes out) {
                final long sizeLocation = header(out, transactionId);

                ThreadLocalCopies copies = keyWriterWithSize.getCopies(null);
                Object keyWriter = keyWriterWithSize.writerForLoop(copies);

                int count = 0;
                while (iterator.hasNext()) {
                    // we've filled up the buffer, so lets give another channel a chance to send
                    // some data, we don't know the max key size, we will use the entrySize instead
                    if (out.remaining() <= maxEntrySizeBytes) {
                        writeHeader(out, sizeLocation, count, true);
                        return false;
                    }

                    count++;
                    K key = iterator.next();
                    keyWriterWithSize.writeInLoop(out, key, keyWriter, copies);
                }

                writeHeader(out, sizeLocation, count, false);
                return true;
            }
        };
    }

    @Nullable
    private Work entrySet(@NotNull final Bytes reader, @NotNull Bytes writer) {
        final long transactionId = reader.readLong();

        final Set<Map.Entry<K, V>> entries;

        try {
            entries = map.entrySet();
        } catch (Throwable e) {
            return sendException(reader, writer, e);
        }

        final Iterator<Map.Entry<K, V>> iterator = entries.iterator();

        // this allows us to write more data than the buffer will allow
        return new Work() {
            @Override
            public boolean doWork(@NotNull Bytes out) {
                if (out.remaining() <= maxEntrySizeBytes)
                    return false;

                final long sizeLocation = header(out, transactionId);

                ThreadLocalCopies copies = keyWriterWithSize.getCopies(null);
                Object keyWriter = keyWriterWithSize.writerForLoop(copies);
                copies = valueWriterWithSize.getCopies(copies);
                Object valueWriter = valueWriterWithSize.writerForLoop(copies);

                int count = 0;
                while (iterator.hasNext()) {
                    // we've filled up the buffer, so lets give another channel a chance to send
                    // some data, we don't know the max key size, we will use the entrySize instead
                    if (out.remaining() <= maxEntrySizeBytes) {
                        writeHeader(out, sizeLocation, count, true);
                        return false;
                    }

                    count++;
                    final Map.Entry<K, V> next = iterator.next();
                    keyWriterWithSize.writeInLoop(out, next.getKey(), keyWriter, copies);
                    valueWriterWithSize.writeInLoop(out, next.getValue(), valueWriter, copies);
                }

                writeHeader(out, sizeLocation, count, false);
                return true;
            }
        };
    }

    @Nullable
    private Work putIfAbsent(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                map.putIfAbsent(reader, writer);
            } else {
                ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
                copies = valueReaderWithSize.getCopies(copies);
                final K key = keyReaderWithSize.read(reader, copies);
                final V v = valueReaderWithSize.read(reader, copies);
                valueWriterWithSize.writeNullable(writer, map.putIfAbsent(key, v), copies);
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    @Nullable
    private Work replace(Bytes reader, @NotNull Bytes writer, final long sizeLocation) {
        try {
            if (MAP_SUPPORTS_BYTES) {
                map.replaceKV(reader, writer);
            } else {
                ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
                copies = valueReaderWithSize.getCopies(copies);
                final K k = keyReaderWithSize.read(reader, copies);
                final V v = valueReaderWithSize.read(reader, copies);
                valueWriterWithSize.writeNullable(writer, map.replace(k, v), copies);
            }
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer);
        return null;
    }

    private long reflectTransactionId(@NotNull Bytes reader, @NotNull Bytes writer) {
        final long transactionId = reader.readLong();
        final long sizeLocation = writer.position();
        writer.skip(SIZE_OF_SIZE);
        assert transactionId != 0;
        writer.writeLong(transactionId);
        writer.writeBoolean(false); // isException
        return sizeLocation;
    }

    private void writeSizeAndFlags(long locationOfSize, boolean isException, @NotNull Bytes out) {
        final long size = out.position() - locationOfSize;

        out.writeInt(locationOfSize, (int) size); // size

        // write isException
        out.writeBoolean(locationOfSize + SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID, isException);

        long pos = out.position();
        long limit = out.limit();

        //   System.out.println("Sending with size=" + size);

        if (LOG.isDebugEnabled()) {
            out.position(locationOfSize);
            out.limit(pos);
            LOG.info("Sending to the stateless client, bytes=" + AbstractBytes.toHex(out) + "," +
                    "len=" + out.remaining());
            out.limit(limit);
            out.position(pos);
        }


    }

    private void writeException(@NotNull Bytes out, Throwable e) {
        out.writeObject(e);
    }

    @NotNull
    private Map<K, V> readEntries(@NotNull Bytes reader) {
        final long numberOfEntries = reader.readStopBit();
        final Map<K, V> result = new HashMap<K, V>();

        ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
        BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(copies);
        copies = valueReaderWithSize.getCopies(copies);
        BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(copies);

        for (long i = 0; i < numberOfEntries; i++) {
            K key = keyReaderWithSize.readInLoop(reader, keyReader);
            V value = valueReaderWithSize.readInLoop(reader, valueReader);
            result.put(key, value);
        }
        return result;
    }

    @Nullable
    private Work sendException(@NotNull Bytes reader, @NotNull Bytes writer, @NotNull Throwable e) {
        final long sizeLocation = reflectTransactionId(reader, writer);
        return sendException(writer, sizeLocation, e);
    }

    private long header(@NotNull Bytes writer, final long transactionId) {
        final long sizeLocation = writer.position();

        writer.skip(SIZE_OF_SIZE);
        writer.writeLong(transactionId);

        // exception
        writer.skip(1);

        //  hasAnotherChunk
        writer.skip(1);

        // count
        writer.skip(4);
        return sizeLocation;
    }

    private void writeHeader(@NotNull Bytes writer, long sizeLocation, int count,
                             final boolean hasAnotherChunk) {
        final long end = writer.position();
        final int size = (int) (end - sizeLocation);
        writer.position(sizeLocation);

        // size in bytes
        writer.writeInt(size);

        //transaction id;
        writer.skip(8);

        // is exception
        writer.writeBoolean(false);

        writer.writeBoolean(hasAnotherChunk);

        // count
        writer.writeInt(count);
        writer.position(end);
    }

}

