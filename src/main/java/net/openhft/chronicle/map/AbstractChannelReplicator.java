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

import net.openhft.chronicle.hash.ThrottlingConfig;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;

import static java.lang.Math.round;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Rob Austin.
 */
abstract class AbstractChannelReplicator implements Closeable {

    public static final int BITS_IN_A_BYTE = 8;

    public static final int SIZE_OF_SIZE = 4;
    public static final int SIZE_OF_TRANSACTIONID = 8;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractChannelReplicator.class);
    final Selector selector;
    final CloseablesManager closeables = new CloseablesManager();
    private final ExecutorService executorService;
    private final Queue<Runnable> pendingRegistrations = new ConcurrentLinkedQueue<Runnable>();
    @Nullable
    private final Throttler throttler;
    volatile boolean isClosed = false;

    AbstractChannelReplicator(String name, ThrottlingConfig throttlingConfig,
                              int maxEntrySizeBytes)
            throws IOException {
        executorService = Executors.newSingleThreadExecutor(
                new NamedThreadFactory(name, true));
        selector = openSelector(closeables);

        throttler = throttlingConfig.throttling(DAYS) > 0 ?
                new Throttler(selector,
                        throttlingConfig.bucketInterval(MILLISECONDS),
                        maxEntrySizeBytes, throttlingConfig.throttling(DAYS)) : null;
    }

    static Selector openSelector(final CloseablesManager closeables) throws IOException {
        Selector result = null;
        try {
            result = Selector.open();
        } finally {
            if (result != null)
                closeables.add(result);
        }
        return result;
    }

    static SocketChannel openSocketChannel(final CloseablesManager closeables) throws IOException {
        SocketChannel result = null;

        try {
            result = SocketChannel.open();
        } finally {
            if (result != null)
                try {
                    closeables.add(result);
                } catch (IllegalStateException e) {
                    // already closed
                }
        }
        return result;
    }

    void addPendingRegistration(Runnable registration) {
        pendingRegistrations.add(registration);
    }

    void registerPendingRegistrations() throws ClosedChannelException {
        for (Runnable runnable = pendingRegistrations.poll(); runnable != null;
             runnable = pendingRegistrations.poll()) {
            try {
                runnable.run();
            } catch (Exception e) {
                LOG.info("", e);
            }
        }
    }

    abstract void process() throws IOException;

    final void start() {
        executorService.execute(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            process();
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                    }
                }
        );
    }


    @Override
    public void close() {
        isClosed = true;
        closeables.closeQuietly();
        executorService.shutdownNow();


        try {
            // we HAVE to be sure we have terminated before calling close() on the
            // ReplicatedChronicleMap
            // if this thread is still running and you close() the ReplicatedChronicleMap without
            // fulling terminating this, you can cause the ReplicatedChronicleMap to attempt to
            // process data on a map that has been closed, which would result in a
            // core dump.
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("", e);
        }
    }

    void closeEarlyAndQuietly(SelectableChannel channel) {
        if (throttler != null)
            throttler.remove(channel);
        closeables.closeQuietly(channel);
    }


    void checkThrottleInterval() throws ClosedChannelException {
        if (throttler != null)
            throttler.checkThrottleInterval();
    }

    void contemplateThrottleWrites(int bytesJustWritten) throws ClosedChannelException {
        if (throttler != null)
            throttler.contemplateThrottleWrites(bytesJustWritten);
    }

    void throttle(SelectableChannel channel) {
        if (throttler != null)
            throttler.add(channel);
    }

    /**
     * throttles 'writes' to ensure the network is not swamped, this is achieved by periodically
     * de-registering the write selector during periods of high volume.
     */
    static class Throttler {

        private final Selector selector;
        private final Set<SelectableChannel> channels = new CopyOnWriteArraySet<SelectableChannel>();
        private final long throttleInterval;
        private final long maxBytesInInterval;

        private long lastTime = System.currentTimeMillis();
        private long bytesWritten;

        Throttler(@NotNull Selector selector,
                  long throttleIntervalInMillis,
                  long serializedEntrySize,
                  long bitsPerDay) {

            this.selector = selector;
            this.throttleInterval = throttleIntervalInMillis;
            double bytesPerMs = ((double) bitsPerDay) / DAYS.toMillis(1) / BITS_IN_A_BYTE;
            this.maxBytesInInterval = round(bytesPerMs * throttleInterval) - serializedEntrySize;
        }

        public void add(SelectableChannel selectableChannel) {
            channels.add(selectableChannel);
        }

        public void remove(SelectableChannel socketChannel) {
            channels.remove(socketChannel);
        }

        /**
         * re register the 'write' on the selector if the throttleInterval has passed
         *
         * @throws java.nio.channels.ClosedChannelException
         */
        public void checkThrottleInterval() throws ClosedChannelException {
            final long time = System.currentTimeMillis();

            if (lastTime + throttleInterval >= time)
                return;

            lastTime = time;
            bytesWritten = 0;

            if (LOG.isDebugEnabled())
                LOG.debug("Restoring OP_WRITE on all channels");

            for (SelectableChannel selectableChannel : channels) {

                final SelectionKey selectionKey = selectableChannel.keyFor(selector);
                if (selectionKey != null)
                    selectionKey.interestOps(selectionKey.interestOps() | OP_WRITE);

            }
        }


        /**
         * checks the number of bytes written in this interval, if this number of bytes exceeds a
         * threshold, the selected will de-register the socket that is being written to, until the
         * interval is finished.
         *
         * @throws ClosedChannelException
         */
        public void contemplateThrottleWrites(int bytesJustWritten)
                throws ClosedChannelException {
            bytesWritten += bytesJustWritten;
            if (bytesWritten > maxBytesInInterval) {
                for (SelectableChannel channel : channels) {
                    final SelectionKey selectionKey = channel.keyFor(selector);
                    if (selectionKey != null) {
                        selectionKey.interestOps(selectionKey.interestOps() & ~OP_WRITE);
                    }
                    if (LOG.isDebugEnabled())
                        LOG.debug("Throttling UDP writes");
                }
            }
        }
    }

    /**
     * details about the socket connection
     */
    static class Details {

        private final InetSocketAddress address;
        private final byte localIdentifier;

        Details(@NotNull final InetSocketAddress address, final byte localIdentifier) {
            this.address = address;
            this.localIdentifier = localIdentifier;
        }

        public InetSocketAddress address() {
            return address;
        }

        public byte localIdentifier() {
            return localIdentifier;
        }

        @Override
        public String toString() {
            return "Details{" +
                    "address=" + address +
                    ", localIdentifier=" + localIdentifier +
                    '}';
        }
    }

    static class EntryCallback extends Replica.EntryCallback {

        private final Replica.EntryExternalizable externalizable;
        private final ByteBufferBytes in;

        EntryCallback(@NotNull final Replica.EntryExternalizable externalizable,
                      @NotNull final ByteBufferBytes in) {
            this.externalizable = externalizable;
            this.in = in;
        }

        @Override
        public boolean onEntry(final Bytes entry, final int chronicleId) {

            long pos0 = in.position();

            // used to denote that this is not a stateless map event
            in.writeByte(StatelessChronicleMap.EventId.STATEFUL_UPDATE.ordinal());

            long sizeLocation = in.position();

            // this is where we will store the size of the entry
            in.skip(SIZE_OF_SIZE);

            final long start = in.position();
            externalizable.writeExternalEntry(entry, in, chronicleId);

            if (in.position() == start) {
                in.position(pos0);
                return false;
            }

            // write the length of the entry, just before the start, so when we read it back
            // we read the length of the entry first and hence know how many preceding writer to read
            final long entrySize = (int) (in.position() - start);
            if (entrySize > Integer.MAX_VALUE)
                throw new IllegalStateException("entry too large, the entry size=" + entrySize + ", " +
                        "entries are limited to a size of " + Integer.MAX_VALUE);


            int entrySize1 = (int) entrySize;
            if (LOG.isDebugEnabled())
                LOG.debug("sending entry of entrySize=" + entrySize1);

            in.writeInt(sizeLocation, entrySize1);

            return true;
        }
    }

    abstract class AbstractConnector {

        private final String name;
        private int connectionAttempts = 0;
        private volatile SelectableChannel socketChannel;

        public AbstractConnector(String name) {
            this.name = name;
        }

        abstract SelectableChannel doConnect() throws IOException, InterruptedException;

        /**
         * connects or reconnects, but first waits a period of time proportional to the {@code
         * connectionAttempts}
         */
        public final void connectLater() {
            if (socketChannel != null) {
                closeables.closeQuietly(socketChannel);
                socketChannel = null;
            }

            final long reconnectionInterval = connectionAttempts * 100;
            if (connectionAttempts < 5)
                connectionAttempts++;
            doConnect(reconnectionInterval);
        }

        /**
         * connects or reconnects immediately
         */
        public void connect() {
            doConnect(0);
        }


        /**
         * @param reconnectionInterval the period to wait before connecting
         */
        private void doConnect(final long reconnectionInterval) {

            final Thread thread = new Thread(new Runnable() {

                public void run() {
                    SelectableChannel socketChannel = null;
                    try {
                        if (reconnectionInterval > 0)
                            Thread.sleep(reconnectionInterval);

                        socketChannel = doConnect();

                        try {
                            closeables.add(socketChannel);
                        } catch (IllegalStateException e) {

                            // close could have be called from another thread, while we were in Thread.sleep()
                            // which would cause a IllegalStateException
                            closeQuietly(socketChannel);
                            return;
                        }

                        AbstractConnector.this.socketChannel = socketChannel;


                    } catch (Exception e) {
                        closeQuietly(socketChannel);
                        LOG.debug("", e);
                    }
                }

                private void closeQuietly(SelectableChannel socketChannel) {
                    if (socketChannel == null)
                        return;
                    try {
                        socketChannel.close();
                    } catch (Exception e1) {
                        // do nothing
                    }
                }
            });

            thread.setName(name);
            thread.setDaemon(true);
            thread.start();


        }

        public void setSuccessfullyConnected() {
            connectionAttempts = 0;
        }
    }


}
