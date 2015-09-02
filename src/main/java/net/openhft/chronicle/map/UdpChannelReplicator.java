/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.UdpTransportConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.*;
import java.nio.BufferUnderflowException;
import java.nio.channels.*;
import java.util.Set;

import static java.net.StandardProtocolFamily.INET;
import static java.net.StandardProtocolFamily.INET6;
import static java.net.StandardSocketOptions.IP_MULTICAST_IF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

interface EntryReader {
    void readAll(@NotNull final DatagramChannel socketChannel) throws IOException;
}

interface EntryWriter {
    int writeAll(@NotNull final DatagramChannel socketChannel) throws InterruptedException, IOException;
}

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
    private final InetAddress address;
    private final int port;
    private final NetworkInterface networkInterface;
    private final ServerConnector serverConnector;
    private final String name;
    private EntryWriter writer;
    private EntryReader reader;
    private SelectableChannel writeChannel;
    private volatile boolean shouldEnableOpWrite;

    /**
     * @param replicationConfig
     * @param localIdentifier   just used for logging
     * @throws IOException
     */
    UdpChannelReplicator(@NotNull final UdpTransportConfig replicationConfig,
                         final byte localIdentifier) throws IOException {

        super("UdpReplicator-" + localIdentifier, replicationConfig.throttlingConfig());

        this.localIdentifier = localIdentifier;

        address = replicationConfig.address();
        port = replicationConfig.port();
        networkInterface = replicationConfig.networkInterface();
        name = replicationConfig.name();
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
    void processEvent() {
        try {

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

                if (useJavaNIOSelectionKeys) {
                    // use the standard java nio selector

                    final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    for (final SelectionKey key : selectionKeys) {
                        processKey(key);
                    }
                    selectionKeys.clear();
                } else {
                    // use the netty like selector

                    final SelectionKey[] keys = selectedKeys.flip();

                    try {
                        for (int i = 0; i < keys.length && keys[i] != null; i++) {
                            final SelectionKey key = keys[i];

                            try {
                                processKey(key);
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
                LOG.debug("closing name=" + this.name);
            if (!isClosed) {
                closeResources();
            }
        }
    }

    private void processKey(SelectionKey key) {

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

    private DatagramChannel connectClient() throws IOException {
        final DatagramChannel client = address.isMulticastAddress() ?
                DatagramChannel.open(address.getAddress().length == 4 ? INET : INET6) :
                DatagramChannel.open();

        final InetSocketAddress hostAddress = new InetSocketAddress(port);
        client.configureBlocking(false);
        if (address.isMulticastAddress()) {
            client.setOption(SO_REUSEADDR, true);
            client.bind(hostAddress);
            if (networkInterface != null) {
                // This is probably not needed, because client socket doesn't send datagrams,
                // but since EVERYBODY on the internet configures this for any channels, and
                // I don't see any harm this config could make, I leave it here
                client.setOption(IP_MULTICAST_IF, networkInterface);
                client.join(address, networkInterface);
            } else {
                client.join(address, NetworkInterface.getByInetAddress(hostAddress.getAddress()));
            }
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
            if (networkInterface != null)
                server.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

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

