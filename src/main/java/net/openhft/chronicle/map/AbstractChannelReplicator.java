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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.CloseablesManager;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.hash.replication.ThrottlingConfig;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.security.AccessController;
import java.security.PrivilegedAction;
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

    static final int SIZE_OF_SIZE = 4;
    public static final int SIZE_OF_TRANSACTION_ID = 8;

    static final byte STATEFUL_UPDATE = 1;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractChannelReplicator.class);

    // currently this is not safe to use as it wont work with the stateless client
    static boolean useJavaNIOSelectionKeys = Boolean.valueOf(System.getProperty("useJavaNIOSelectionKeys"));

    final SelectedSelectionKeySet selectedKeys = new SelectedSelectionKeySet();

    final CloseablesManager closeables = new CloseablesManager();
    final Selector selector = openSelector(closeables);
    private final ExecutorService executorService;

    private final ExecutorService connectExecutorService = Executors.newCachedThreadPool(
            new NamedThreadFactory("TCP-connect", true) {
                @NotNull
                @Override
                public Thread newThread(@NotNull Runnable r) {
                    return lastThread = super.newThread(r);
                }
            });

    private volatile Thread lastThread;
    private Throwable startedHere;
    private Future<?> future;
    private final Queue<Runnable> pendingRegistrations = new ConcurrentLinkedQueue<>();
    @Nullable
    private final Throttler throttler;

    volatile boolean isClosed = false;

    AbstractChannelReplicator(String name, ThrottlingConfig throttlingConfig)
            throws IOException {
        executorService = Executors.newSingleThreadExecutor(
                new NamedThreadFactory(name, true) {
                    @NotNull
                    @Override
                    public Thread newThread(@NotNull Runnable r) {
                        return lastThread = super.newThread(r);
                    }
                });

        throttler = throttlingConfig.throttling(DAYS) > 0 ?
                new Throttler(selector,
                        throttlingConfig.bucketInterval(MILLISECONDS),
                        throttlingConfig.throttling(DAYS)) : null;

        startedHere = new Throwable("Started here");
    }

    Selector openSelector(final CloseablesManager closeables) throws IOException {

        Selector result = Selector.open();
        closeables.add(result);

        if (!useJavaNIOSelectionKeys) {
            closeables.add(new Closeable() {
                               @Override
                               public void close() throws IOException {
                                   {
                                       SelectionKey[] keys = selectedKeys.flip();
                                       for (int i = 0; i < keys.length && keys[i] != null; i++) {
                                           keys[i] = null;
                                       }
                                   }
                                   {
                                       SelectionKey[] keys = selectedKeys.flip();
                                       for (int i = 0; i < keys.length && keys[i] != null; i++) {
                                           keys[i] = null;
                                       }
                                   }
                               }
                           }
            );
            return openSelector(result, selectedKeys);
        }

        return result;
    }

    static SocketChannel openSocketChannel(final CloseablesManager closeables) throws IOException {
        SocketChannel result = null;

        try {
            result = SocketChannel.open();
            result.socket().setTcpNoDelay(true);
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

    /**
     * this is similar to the code in Netty
     *
     * @param selector
     * @return
     */
    private static Selector openSelector(@NotNull final Selector selector,
                                  @NotNull final SelectedSelectionKeySet selectedKeySet) {
        try {

            Class<?> selectorImplClass =
                    Class.forName("sun.nio.ch.SelectorImpl", false, getSystemClassLoader());

            // Ensure the current selector implementation is what we can instrument.
            if (!selectorImplClass.isAssignableFrom(selector.getClass())) {
                return selector;
            }

            Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
            Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

            selectedKeysField.setAccessible(true);
            publicSelectedKeysField.setAccessible(true);

            selectedKeysField.set(selector, selectedKeySet);
            publicSelectedKeysField.set(selector, selectedKeySet);

            //   logger.trace("Instrumented an optimized java.util.Set into: {}", selector);
        } catch (Exception e) {
            LOG.error("", e);
            //   logger.trace("Failed to instrument an optimized java.util.Set into: {}", selector, t);
        }

        return selector;
    }

    static ClassLoader getSystemClassLoader() {
        if (System.getSecurityManager() == null) {
            return ClassLoader.getSystemClassLoader();
        } else {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                @Override
                public ClassLoader run() {
                    return ClassLoader.getSystemClassLoader();
                }
            });
        }
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

    abstract void processEvent() throws IOException;

    final void start() {
        future = executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            processEvent();
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                    }
                }
        );
    }

    @Override
    public void close() {

        if (Thread.interrupted())
            LOG.warn("Already interrupted");
        long start = System.currentTimeMillis();
        isClosed = true;
        connectExecutorService.shutdown();
        try {
            if (!connectExecutorService.awaitTermination(5, TimeUnit.MILLISECONDS)) {
                connectExecutorService.shutdownNow();
                connectExecutorService.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            LOG.error("", e);
        }

        try {
            closeResources();
            if (!executorService.awaitTermination(5, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
                executorService.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            LOG.error("", e);
        }

        try {
            if (future != null)
                future.cancel(true);

            // we HAVE to be sure we have terminated before calling close() on the ReplicatedChronicleMap
            // if this thread is still running and you close() the ReplicatedChronicleMap without
            // fulling terminating this, you can cause the ReplicatedChronicleMap to attempt to
            // process data on a map that has been closed, which would result in a core dump.

            Thread thread = lastThread;
            if (thread != null && Thread.currentThread() != lastThread)
                for (int i = 0; i < 10; i++) {
                    if (thread.isAlive()) {
                        thread.join(1000);
                        dumpThreadStackTrace(start);
                    }
                }
        } catch (InterruptedException e) {
            dumpThreadStackTrace(start);
            LOG.error("", e);
            LOG.error("", startedHere);
        }
    }

    public void closeResources() {
        isClosed = true;
        executorService.shutdown();
        closeables.closeQuietly();

    }

    private void dumpThreadStackTrace(long start) {
        if (lastThread != null && lastThread.isAlive()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Replicator thread still running after ");
            sb.append((System.currentTimeMillis() - start) / 100 / 10.0);
            sb.append(" secs ");
            sb.append(lastThread);
            sb.append(" isAlive= ");
            sb.append(lastThread.isAlive());
            for (StackTraceElement ste : lastThread.getStackTrace()) {
                sb.append("\n\t").append(ste);
            }
            LOG.warn(sb.toString());
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
                  long bitsPerDay) {
            this.selector = selector;
            this.throttleInterval = throttleIntervalInMillis;
            double bytesPerMs = ((double) bitsPerDay) / DAYS.toMillis(1) / BITS_IN_A_BYTE;
            this.maxBytesInInterval = round(bytesPerMs * throttleInterval);
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

        /** For writing entries for replication from Ch Map memory */
        private Bytes<ByteBuffer> entryIn;

        EntryCallback(@NotNull final Replica.EntryExternalizable externalizable,
                      final int tcpBufferSize) {
            this.externalizable = externalizable;
            entryIn = Bytes.elasticByteBuffer(tcpBufferSize);
        }

        @NotNull
        public Bytes entryIn() {
            return entryIn;
        }

        @NotNull
        public ByteBuffer socketOut() {
            return entryIn.underlyingObject();
        }

        @Override
        public boolean shouldBeIgnored(final ReplicableEntry entry, final int chronicleId) {
            return !externalizable.identifierCheck(entry, chronicleId);
        }

        @Override
        public boolean onEntry(
                @NotNull final Bytes entry, final int chronicleId, final long bootstrapTime) {

            long pos0 = entryIn.writePosition();
            // used to denote that this is not a heartbeat
            entryIn.writeByte(STATEFUL_UPDATE);

            long sizeLocation = entryIn.writePosition();

            // this is where we will store the size of the entry
            entryIn.writeSkip(SIZE_OF_SIZE);

            long start = entryIn.writePosition();

            externalizable.writeExternalEntry(entry, entryIn, chronicleId, bootstrapTime);

            if (entryIn.writePosition() == start) {
                entryIn.writePosition(pos0);
                return false;
            }

            // write the length of the entry, just before the start, so when we read it back
            // we read the length of the entry first and hence know how many preceding writer
            // to read
            final long bytesWritten = (int) (entryIn.writePosition() - start);

            if (bytesWritten > Integer.MAX_VALUE)
                throw new IllegalStateException("entry size " + bytesWritten + " too large, " +
                        "entries are limited to a size of " + Integer.MAX_VALUE);

            if (LOG.isDebugEnabled())
                LOG.debug("sending entry of entrySize=" + (int) bytesWritten);

            entryIn.writeInt(sizeLocation, (int) bytesWritten);
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
            try {
                connectExecutorService.submit(new Runnable() {
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
                });
            } catch (Exception e) {
                if (!isClosed)
                    LOG.error("", e);
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


        public void setSuccessfullyConnected() {
            connectionAttempts = 0;
        }

    }

}
