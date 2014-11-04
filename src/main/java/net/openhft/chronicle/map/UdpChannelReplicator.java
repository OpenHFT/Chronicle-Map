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

import net.openhft.chronicle.hash.UdpReplicationConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
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
 * typically, you should use the UdpReplicator if you have a large number of nodes, and you wish to
 * receive the data before it becomes available on TCP/IP. In order to not miss data, UdpReplicator
 * should be used in conjunction with the TCP Replicator.
 */
class UdpChannelReplicator extends AbstractChannelReplicator implements Replica.ModificationNotifier, Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpChannelReplicator.class.getName());

    private final byte localIdentifier;
    private EntryWriter writer;
    private EntryReader reader;

    private final InetAddress address;
    private final int port;
    private final NetworkInterface networkInterface;
    private final ServerConnector serverConnector;

    private SelectableChannel writeChannel;
    private volatile boolean shouldEnableOpWrite;

    /**
     * @param replicationConfig
     * @param maxPayloadSize    the maximum size of any serialized entry
     * @param localIdentifier   just used for logging
     * @throws IOException
     */
    UdpChannelReplicator(@NotNull final UdpReplicationConfig replicationConfig,
                         final int maxPayloadSize,
                         final byte localIdentifier)
            throws IOException {

        super("UdpReplicator-" + localIdentifier, replicationConfig.throttlingConfig(),
                maxPayloadSize);

        this.localIdentifier = localIdentifier;

        address = replicationConfig.address();
        port = replicationConfig.port();
        networkInterface = replicationConfig.networkInterface();
        serverConnector = new ServerConnector();
    }


    void setWriter(EntryWriter writer) {
        this.writer = writer;
    }

    void setReader(EntryReader reader) {
        this.reader = reader;
    }

    @Override
    public void close() {
        super.close();
        writeChannel = null;
    }

    /**
     * binds to the server socket and process data This method will block until interrupted
     */
    @Override
    void process() throws IOException {

        connectClient().register(selector, OP_READ);
        serverConnector.connectLater();

        try {
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
                        if (!isClosed)
                            closeEarlyAndQuietly(key.channel());
                    }
                }

                selectionKeys.clear();
            }
        } finally {
            if (!isClosed) {
                close();
            }
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

    }

    private void enableWrites() {

        if (writeChannel == null)
            return;

        try {
            final SelectionKey selectionKey = writeChannel.keyFor(this.selector);
            if (selectionKey != null)
                selectionKey.interestOps(selectionKey.interestOps() | OP_WRITE);
            // we have just enabled it, so don't have to do this again.
            shouldEnableOpWrite = false;
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    void disableWrites() {

        if (writeChannel == null)
            return;

        try {
            final SelectionKey selectionKey = writeChannel.keyFor(this.selector);
            selectionKey.interestOps(selectionKey.interestOps() & ~OP_WRITE);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }


    private class ServerConnector extends AbstractConnector {
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


interface EntryReader {
    void readAll(@NotNull final DatagramChannel socketChannel) throws IOException, InterruptedException;
}

interface EntryWriter {
    int writeAll(@NotNull final DatagramChannel socketChannel) throws InterruptedException, IOException;
}



