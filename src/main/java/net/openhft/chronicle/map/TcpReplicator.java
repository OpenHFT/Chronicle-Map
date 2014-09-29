/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.BitSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.channels.SelectionKey.*;

/**
 * Used with a {@see net.openhft.map.ReplicatedSharedHashMap} to send data between the maps using a socket
 * connection <p/> {@see net.openhft.map.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
class TcpReplicator extends AbstractChannelReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1MB

    private final SelectionKey[] selectionKeysStore = new SelectionKey[Byte.MAX_VALUE + 1];
    // used to instruct the selector thread to set OP_WRITE on a key correlated by the bit index in the
    // bitset
    private final KeyInterestUpdater opWriteUpdater = new KeyInterestUpdater(OP_WRITE, selectionKeysStore);
    private final BitSet activeKeys = new BitSet(selectionKeysStore.length);
    private final long heartBeatIntervalMillis;

    private final Replica replica;
    private final byte localIdentifier;
    private final int maxEntrySizeBytes;
    private final Replica.EntryExternalizable externalizable;
    private final TcpReplicationConfig replicationConfig;

    private long selectorTimeout;

    /**
     * @param maxEntrySizeBytes used to check that the last entry will fit into the buffer, it can not be
     *                          smaller than the size of and entry, if it is set smaller the buffer will over
     *                          flow, it can be larger then the entry, but setting it too large reduces the
     *                          workable space in the buffer.
     * @throws IOException
     */
    TcpReplicator(@NotNull final Replica replica,
                  @NotNull final Replica.EntryExternalizable externalizable,
                  @NotNull final TcpReplicationConfig replicationConfig,
                  final int maxEntrySizeBytes) throws IOException {

        super("TcpSocketReplicator-" + replica.identifier(), replicationConfig.throttlingConfig(),
                maxEntrySizeBytes);

        final ThrottlingConfig throttlingConfig = replicationConfig.throttlingConfig();
        long throttleBucketInterval = TimeUnit.MILLISECONDS.convert(throttlingConfig
                .bucketInterval(), throttlingConfig.bucketIntervalUnit());

        heartBeatIntervalMillis = TimeUnit.MILLISECONDS.convert(replicationConfig.heartBeatInterval(),
                replicationConfig.heartBeatIntervalUnit());

        selectorTimeout = Math.min(heartBeatIntervalMillis, throttleBucketInterval);


        this.replica = replica;
        this.localIdentifier = replica.identifier();
        this.maxEntrySizeBytes = maxEntrySizeBytes;
        this.externalizable = externalizable;
        this.replicationConfig = replicationConfig;

        start();
    }

    @Override
    void process() throws IOException {
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

                final int nSelectedKeys = selector.select(selectorTimeout);

                // its less resource intensive to set this less frequently and use an approximation
                final long approxTime = System.currentTimeMillis();

                checkThrottleInterval();

                // check that we have sent and received heartbeats
                heartBeatMonitor(approxTime);

                // set the OP_WRITE when data is ready to send
                opWriteUpdater.applyUpdates();

                if (nSelectedKeys == 0)
                    continue;    // go back and check pendingRegistrations

                final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                for (final SelectionKey key : selectionKeys) {
                    try {

                        if (!key.isValid())
                            continue;

                        if (key.isAcceptable())
                            onAccept(key);

                        if (key.isConnectable())
                            onConnect(key);

                        if (key.isReadable())
                            onRead(key, approxTime);

                        if (key.isWritable())
                            onWrite(key, approxTime);

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
                selectionKeys.clear();
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
        } finally {
            if (!isClosed) {
                close();
            }
        }
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

    private void enableOpWrite(SelectionKey key) {
        int ops = key.interestOps();
        if ((ops & (OP_CONNECT | OP_ACCEPT)) == 0)
            key.interestOps(ops | OP_WRITE);
    }

    /**
     * check to see if we have lost connection with the remote node and if we have attempts a reconnect.
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

        final SocketChannel channel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        try {
            if (!channel.finishConnect()) {
                return;
            }
        } catch (SocketException e) {
            quietClose(key, e);

            // when node discovery is used ( by nodes broadcasting out their host:port over UDP ),
            // when new or restarted nodes are started up. they attempt to find the nodes
            // on the grid by listening to the host and ports of the other nodes, so these nodes will establish the connection when they come back up,
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

        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel channel = server.accept();
        channel.configureBlocking(false);
        channel.socket().setReuseAddress(true);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        final Attached attached = new Attached();
        channel.register(selector, OP_WRITE | OP_READ, attached);

        throttle(channel);

        attached.entryReader = new TcpSocketChannelEntryReader();
        attached.entryWriter = new TcpSocketChannelEntryWriter();

        attached.isServer = true;
        attached.entryWriter.identifierToBuffer(localIdentifier);
    }

    /**
     * this can be called when a new CHM is added to a cluster, we have to rebootstrap so will clear all the
     * old bootstrap information
     *
     * @param key the nio SelectionKey
     */
    private void clearHandshaking(SelectionKey key) {
        final Attached attached = (Attached) key.attachment();
        activeKeys.clear(attached.remoteIdentifier);
        selectionKeysStore[attached.remoteIdentifier] = null;
        attached.clearHandShaking();
    }

    /**
     * used to exchange identifiers and timestamps and heartbeat intervals between the server and client
     *
     * @param key           the SelectionKey relating to the this cha
     * @param socketChannel
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    private void doHandShaking(@NotNull final SelectionKey key, SocketChannel socketChannel)
            throws IOException, InterruptedException {
        final Attached attached = (Attached) key.attachment();
        final TcpSocketChannelEntryWriter writer = attached.entryWriter;
        final TcpSocketChannelEntryReader reader = attached.entryReader;

        if (attached.remoteIdentifier == Byte.MIN_VALUE) {

            final byte remoteIdentifier = reader.identifierFromBuffer();

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
            if (remoteIdentifier == localIdentifier) {


                final NonUniqueIdentifierListener listener = replicationConfig.nonUniqueIdentifierListener();

                if (listener != null)
                    listener.onNonUniqueIdentifier(remoteIdentifier);

                // throwing this exception will cause us to disconnect, both the client and server
                // will be able to detect the the remote and local identifiers are the same,
                // as the identifier is send early on in the hand shaking via the connect(() and accept()
                // methods
                throw new IllegalStateException("dropping connection, as the remote and local-identifier " +
                        "are the same, identifier=" + localIdentifier);
            }

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

            long value = reader.remoteHeartbeatIntervalFromBuffer();

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
            reader.entriesFromBuffer();
        }
    }

    /**
     * called when the selector receives a OP_WRITE message
     */
    private void onWrite(@NotNull final SelectionKey key,
                         final long approxTime) throws InterruptedException, IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        if (attached.remoteModificationIterator != null)
            attached.entryWriter.entriesToBuffer(attached.remoteModificationIterator, key);

        try {
            int bytesJustWritten = attached.entryWriter.writeBufferToSocket(socketChannel,
                    approxTime);

            contemplateThrottleWrites(bytesJustWritten);

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
    private void onRead(final SelectionKey key,
                        final long approxTime) throws IOException, InterruptedException {

        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        try {
            if (attached.entryReader.readSocketToBuffer(socketChannel) <= 0)
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
            attached.entryReader.entriesFromBuffer();
        } else {
            doHandShaking(key, socketChannel);
        }
    }

    /**
     * sets interestOps to "selector keys",The change to interestOps much be on the same thread as the
     * selector. This  class, allows via {@link AbstractChannelReplicator .KeyInterestUpdater#set(int)}  to
     * holds a pending change  in interestOps ( via a bitset ), this change is processed later on the same
     * thread as the selector
     */
    private static class KeyInterestUpdater {

        private final AtomicBoolean wasChanged = new AtomicBoolean();
        private final BitSet changeOfOpWriteRequired;
        private final SelectionKey[] selectionKeys;
        private final int op;

        KeyInterestUpdater(int op, final SelectionKey[] selectionKeys) {
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
         * @param keyIndex the index of the key that has changed, the list of keys is provided by the
         *                 constructor {@link KeyInterestUpdater(int, SelectionKey[])}
         */
        public void set(int keyIndex) {
            changeOfOpWriteRequired.set(keyIndex);
            wasChanged.lazySet(true);
        }
    }

    private class ServerConnector extends AbstractConnector {

        private final Details details;

        private ServerConnector(@NotNull Details details) {
            super("TCP-ServerConnector-" + localIdentifier);
            this.details = details;
        }

        @Override
        public String toString() {
            return "ServerConnector{" +
                    "" + details +
                    '}';
        }

        SelectableChannel doConnect() throws
                IOException, InterruptedException {

            final ServerSocketChannel serverChannel = ServerSocketChannel.open();

            serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);
            final ServerSocket serverSocket = serverChannel.socket();
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
                        LOG.error("", e);
                    }

                }
            });

            selector.wakeup();

            return serverChannel;
        }
    }

    private class ClientConnector extends AbstractConnector {

        private final Details details;

        private ClientConnector(@NotNull Details details) {
            super("TCP-ClientConnector-" + details.localIdentifier());
            this.details = details;
        }


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
            final SocketChannel socketChannel = SocketChannel.open();
            try {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setReuseAddress(true);
                socketChannel.socket().setSoLinger(false, 0);
                socketChannel.socket().setSoTimeout(0);
                socketChannel.socket().setTcpNoDelay(true);

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

        public Replica.ModificationIterator remoteModificationIterator;
        public AbstractConnector connector;
        public long remoteBootstrapTimestamp = Long.MIN_VALUE;
        public byte remoteIdentifier = Byte.MIN_VALUE;
        public boolean hasRemoteHeartbeatInterval;
        // true if its socket is a ServerSocket
        public boolean isServer;        // the frequency the remote node will send a heartbeat
        public long remoteHeartbeatInterval = heartBeatIntervalMillis;
        public boolean handShakingComplete;

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

            selector.wakeup();
        }


    }

    /**
     * @author Rob Austin.
     */
    private class TcpSocketChannelEntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final EntryCallback entryCallback;
        private long lastSentTime;

        private TcpSocketChannelEntryWriter() {
            out = ByteBuffer.allocateDirect(replicationConfig.packetSize() + maxEntrySizeBytes);
            in = new ByteBufferBytes(out);
            entryCallback = new EntryCallback(externalizable, in);
        }


        /**
         * writes the timestamp into the buffer
         *
         * @param localIdentifier the current nodes identifier
         */
        void identifierToBuffer(final int localIdentifier) {
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
         * writes all the entries that have changed, to the buffer which will later be written to TCP/IP
         *
         * @param modificationIterator a record of which entries have modification
         * @param selectionKey
         */
        void entriesToBuffer(@NotNull final Replica.ModificationIterator modificationIterator,
                             @NotNull final SelectionKey selectionKey)
                throws InterruptedException, IOException {

            final SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
            final Attached attached = (Attached) selectionKey.attachment();

            // this can occur when new CHM's are added to a cluster
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
            out.limit((int) in.position());

            final int len = socketChannel.write(out);

            if (LOG.isDebugEnabled())
                LOG.debug("bytes-written=" + len);

            if (out.remaining() == 0) {
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
         * used to send an single zero byte if we have not send any data for up to the localHeartbeatInterval
         */
        private void writeHeartbeatToBuffer() {
            in.writeUnsignedShort(0);
        }

        private void writeRemoteHeartbeatInterval(long localHeartbeatInterval) {
            in.writeLong(localHeartbeatInterval);
        }


        /**
         * removes back in the OP_WRITE from the selector, otherwise it'll spin loop. The OP_WRITE will get
         * added back in as soon as we have data to write
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


    }

    /**
     * Reads map entries from a socket, this could be a client or server socket
     */
    private class TcpSocketChannelEntryReader {
        private final ByteBuffer in;
        private final ByteBufferBytes out;
        public long lastHeartBeatReceived = System.currentTimeMillis();
        // we use Integer.MIN_VALUE as N/A
        private int sizeOfNextEntry = Integer.MIN_VALUE;

        private TcpSocketChannelEntryReader() {
            in = ByteBuffer.allocateDirect(replicationConfig.packetSize() + maxEntrySizeBytes);
            out = new ByteBufferBytes(in);
            out.limit(0);
            in.clear();
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
         * @throws InterruptedException
         */
        private void entriesFromBuffer() throws InterruptedException, IOException {

            for (; ; ) {

                out.limit(in.position());

                // its set to MIN_VALUE when it should be read again
                if (sizeOfNextEntry == Integer.MIN_VALUE) {
                    if (out.remaining() < SIZE_OF_SHORT) {
                        return;
                    }

                    int value = out.readUnsignedShort();

                    // this is the heartbeat
                    if (value == 0)
                        continue;

                    sizeOfNextEntry = value;
                }


                if (out.remaining() < sizeOfNextEntry) {
                    return;
                }

                final long nextEntryPos = out.position() + sizeOfNextEntry;
                final long limit = out.limit();
                out.limit(nextEntryPos);
                externalizable.readExternalEntry(out);

                out.limit(limit);
                // skip onto the next entry
                out.position(nextEntryPos);

                // to allow the sizeOfNextEntry to be read the next time around
                sizeOfNextEntry = Integer.MIN_VALUE;
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
            return (out.remaining() >= 8) ? out.readLong() : Long.MIN_VALUE;
        }

        public long remoteHeartbeatIntervalFromBuffer() {
            return (out.remaining() >= 8) ? out.readLong() : Long.MIN_VALUE;
        }
    }
}

