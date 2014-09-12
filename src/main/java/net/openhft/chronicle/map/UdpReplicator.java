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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Set;

import static java.net.StandardProtocolFamily.INET;
import static java.net.StandardProtocolFamily.INET6;
import static java.net.StandardSocketOptions.IP_MULTICAST_IF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;


/**
 * The UdpReplicator attempts to read the data ( but it does not enforce or grantee delivery ),
 * typically, you should use the UdpReplicator if you have a large number of nodes,
 * and you wish to receive the data before it becomes available on TCP/IP.
 * In order to not miss data, UdpReplicator should be used in conjunction with the TCP Replicator.
 */
class UdpReplicator extends AbstractChannelReplicator implements Replica.ModificationNotifier, Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpReplicator.class.getName());

    private final byte localIdentifier;
    private final UdpSocketChannelEntryWriter writer;
    private final UdpSocketChannelEntryReader reader;
    private final Replica replica;
    private final InetAddress address;
    private final int port;
    private final NetworkInterface networkInterface;
    private final ServerConnector serverConnector;
    private Replica.ModificationIterator modificationIterator;
    private SelectableChannel writeChannel;
    private volatile boolean shouldEnableOpWrite;

    UdpReplicator(@NotNull final Replica replica,
                  @NotNull final Replica.EntryExternalizable entryExternalizable,
                  @NotNull final UdpReplicationConfig replicationConfig,
                  final int serializedEntrySize)
            throws IOException {
        super("UdpReplicator-" + replica.identifier(), replicationConfig.throttlingConfig(),
                serializedEntrySize);

        this.localIdentifier = replica.identifier();
        this.replica = replica;
        this.writer = new UdpSocketChannelEntryWriter(serializedEntrySize, entryExternalizable);
        this.reader = new UdpSocketChannelEntryReader(serializedEntrySize, entryExternalizable);

        address = replicationConfig.address();
        port = replicationConfig.port();
        networkInterface = replicationConfig.networkInterface();

        serverConnector = new ServerConnector();

        start();
    }

    @Override
    public void close() {
        writeChannel = null;
        super.close();
    }



    /**
     * binds to the server socket and process data This method will block until interrupted
     */
    @Override
    void process() throws IOException {

        connectClient().register(selector, OP_READ);
        serverConnector.connectLater();

        while (selector.isOpen()) {

            registerPendingRegistrations();

            // this may block for a long time, upon return the
            // selected set contains keys of the ready channels
            final int n = selector.select(100);

            if (shouldEnableOpWrite)
                enableWrites();

            checkThrottleInterval();

            if (n == 0) {
                continue;    // nothing to do
            }

            final Set<SelectionKey> selectionKeys = selector.selectedKeys();
            for (final SelectionKey key : selectionKeys) {

                try {

                    if (key.isReadable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        reader.readAll(socketChannel);
                    }

                    if (key.isWritable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        try {
                            int bytesJustWritten = writer.writeAll(socketChannel);
                            contemplateThrottleWrites(bytesJustWritten);
                        } catch (NotYetConnectedException e) {
                            if (LOG.isDebugEnabled())
                                LOG.debug("", e);
                            serverConnector.connectLater();
                        } catch (IOException e) {
                            if (LOG.isDebugEnabled())
                                LOG.debug("", e);
                            serverConnector.connectLater();
                        }
                    }

                } catch (Exception e) {
                    LOG.error("", e);
                    closeEarlyAndQuietly(key.channel());
                }
            }

            selectionKeys.clear();
        }


    }


    private DatagramChannel connectClient() throws IOException {
        final DatagramChannel client = address.isMulticastAddress() ?
                DatagramChannel.open(address.getAddress().length == 4 ? INET : INET6) :
                DatagramChannel.open();

        final InetSocketAddress hostAddress = new InetSocketAddress(port);
        client.configureBlocking(false);
        if (address.isMulticastAddress()) {
            client.setOption(IP_MULTICAST_IF, networkInterface);
            client.setOption(SO_REUSEADDR, true);
            client.bind(hostAddress);
            client.join(address, networkInterface);
            if (LOG.isDebugEnabled())
                LOG.debug("Connecting via multicast, group=" + address);
        } else {
            client.bind(hostAddress);
        }

        if (LOG.isDebugEnabled())
            LOG.debug("Listening on port " + port);
        closeables.add(client);
        return client;
    }

    /**
     * called whenever there is a change to the modification iterator
     */
    @Override
    public void onChange() {
        // the write have to be enabled on the same thread as the selector
        shouldEnableOpWrite = true;
        selector.wakeup();
    }

    private void enableWrites() {

        if (writeChannel == null)
            return;

        try {
            final SelectionKey selectionKey = writeChannel.keyFor(this.selector);
            if (selectionKey != null)
                selectionKey.interestOps(selectionKey.interestOps() | OP_WRITE);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    private void disableWrites() {

        if (writeChannel == null)
            return;

        try {
            final SelectionKey selectionKey = writeChannel.keyFor(this.selector);
            selectionKey.interestOps(selectionKey.interestOps() & ~OP_WRITE);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    private static class UdpSocketChannelEntryReader {

        private final Replica.EntryExternalizable externalizable;
        private final ByteBuffer in;
        private final ByteBufferBytes out;

        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         */
        UdpSocketChannelEntryReader(final int serializedEntrySize,
                                    @NotNull final Replica.EntryExternalizable externalizable) {
            // we make the buffer twice as large just to give ourselves headroom
            in = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            this.externalizable = externalizable;
            out = new ByteBufferBytes(in);
            out.limit(0);
            in.clear();
        }

        /**
         * reads entries from the socket till it is empty
         *
         * @param socketChannel the socketChannel that we will read from
         * @throws IOException
         * @throws InterruptedException
         */
        void readAll(@NotNull final DatagramChannel socketChannel) throws IOException, InterruptedException {

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

            externalizable.readExternalEntry(out);
        }

    }

    private class UdpSocketChannelEntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final EntryCallback entryCallback;

        UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final Replica.EntryExternalizable externalizable) {

            // we make the buffer twice as large just to give ourselves headroom
            out = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);
            entryCallback = new EntryCallback(externalizable, in);

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
        int writeAll(@NotNull final DatagramChannel socketChannel)
                throws InterruptedException, IOException {

            out.clear();
            in.clear();
            in.skip(SIZE_OF_SHORT);

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback, 0);

            if (!wasDataRead) {
                disableWrites();
                return 0;
            }

            // we'll write the size inverted at the start
            in.writeShort(0, ~(in.readUnsignedShort(SIZE_OF_SHORT)));
            out.limit((int) in.position());

            return socketChannel.write(out);

        }
    }

    private class ServerConnector extends AbstractChannelReplicator.AbstractConnector {
        private final InetSocketAddress socketAddress;

        private ServerConnector() {
            super("UDP-Connector");
            this.socketAddress = new InetSocketAddress(address, port);
        }

        SelectableChannel doConnect() throws
                IOException, InterruptedException {

            final DatagramChannel server = DatagramChannel.open();
            server.configureBlocking(false);

            // Kick off connection establishment
            try {
                // Create a non-blocking socket channel
                server.socket().setBroadcast(true);
                server.connect(socketAddress);
            } catch (IOException e) {
                if (LOG.isDebugEnabled())
                    LOG.debug("details=" + new Details(socketAddress, localIdentifier), e);
                connectLater();
                return null;
            }

            server.setOption(SO_REUSEADDR, true)
                    .setOption(StandardSocketOptions.IP_MULTICAST_LOOP, false)
                    .setOption(StandardSocketOptions.SO_BROADCAST, true)
                    .setOption(SO_REUSEADDR, true);

            // the registration has be be run on the same thread as the selector
            addPendingRegistration(new Runnable() {
                @Override
                public void run() {

                    try {
                        server.register(selector, OP_WRITE);
                        writeChannel = server;
                        throttle(server);
                    } catch (ClosedChannelException e) {
                        LOG.error("", e);
                    }

                }
            });

            return server;
        }


    }


}





