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

import net.openhft.chronicle.hash.replication.ThrottlingConfig;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.thread.NamedThreadFactory;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
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

    public static final int SIZE_OF_SIZE = 4;
    public static final int SIZE_OF_TRANSACTION_ID = 8;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractChannelReplicator.class);

    // currently this is not safe to use as it wont work with the stateless client
    static boolean useJavaNIOSelectionKeys = Boolean.valueOf(System.getProperty("useJavaNIOSelectionKeys"));

    final SelectedSelectionKeySet selectedKeys = new SelectedSelectionKeySet();

    final CloseablesManager closeables = new CloseablesManager();
    final Selector selector = openSelector(closeables);
    private final ExecutorService executorService;

    private final ExecutorService connectExecutorService = Executors.newCachedThreadPool(
            new NamedThreadFactory("TCP-connect", true) {
                @Override
                public Thread newThread(@net.openhft.lang.model.constraints.NotNull Runnable r) {
                    return lastThread = super.newThread(r);
                }
            });

    private volatile Thread lastThread;
    private Throwable startedHere;
    private Future<?> future;
    private final Queue<Runnable> pendingRegistrations = new ConcurrentLinkedQueue<Runnable>();
    @Nullable
    private final Throttler throttler;

    volatile boolean isClosed = false;
    ThreadLocalCopies copies;
    VanillaChronicleMap.SegmentState segmentState;

    AbstractChannelReplicator(String name, ThrottlingConfig throttlingConfig)
            throws IOException {
        executorService = Executors.newSingleThreadExecutor(
                new NamedThreadFactory(name, true) {
                    @Override
                    public Thread newThread(@net.openhft.lang.model.constraints.NotNull Runnable r) {
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
    private Selector openSelector(@NotNull final Selector selector,
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
                            copies = VanillaChronicleMap.SegmentState.getCopies(null);
                            segmentState = VanillaChronicleMap.SegmentState.get(copies);
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
        if (segmentState != null)
            segmentState.close();
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

    static class EntryCallback extends Replica.EntryCallback implements BufferResizer {

        private final Replica.EntryExternalizable externalizable;

        @NotNull
        private ByteBufferBytes in;

        @NotNull
        private ByteBuffer out;

        EntryCallback(@NotNull final Replica.EntryExternalizable externalizable,
                      final int tcpBufferSize) {
            this.externalizable = externalizable;
            out = ByteBuffer.allocateDirect(tcpBufferSize);
            in = new ByteBufferBytes(out);
        }

        @NotNull
        public ByteBufferBytes in() {
            return in;
        }

        @NotNull
        public ByteBuffer out() {
            return out;
        }


        public Bytes resizeBuffer(int size) {

            if (LOG.isDebugEnabled())
                LOG.debug("resizing buffer to size=" + size);

            if (size < out.capacity())
                throw new IllegalStateException("it not possible to resize the buffer smaller");

            assert size < Integer.MAX_VALUE;

            final ByteBuffer result = ByteBuffer.allocate(size).order(ByteOrder.nativeOrder());
            final long bytesPosition = in.position();

            in = new ByteBufferBytes(result);

            out.position(0);
            out.limit((int) bytesPosition);
            in.write(out);
            out = result;

            assert out.capacity() == in.capacity();

            assert out.capacity() == size;
            assert out.capacity() == in.capacity();
            assert in.limit() == in.capacity();
            return in;
        }

        public boolean shouldBeIgnored(final Bytes entry, final int chronicleId) {
            return !externalizable.identifierCheck(entry, chronicleId);
        }


        @Override
        public boolean onEntry(final Bytes entry, final int chronicleId) {

            long startOfEntry = entry.position();
            long pos0 = in.position();
            long start = 0;
            try {
                // used to denote that this is not a stateless map event
                in.writeByte(StatelessChronicleMap.EventId.STATEFUL_UPDATE.ordinal());

                long sizeLocation = in.position();

                // this is where we will store the size of the entry
                in.skip(SIZE_OF_SIZE);

                start = in.position();

                externalizable.writeExternalEntry(entry, in, chronicleId);

                if (in.position() == start) {
                    in.position(pos0);
                    return false;
                }

                // write the length of the entry, just before the start, so when we read it back
                // we read the length of the entry first and hence know how many preceding writer to read
                final long bytesWritten = (int) (in.position() - start);

                if (bytesWritten > Integer.MAX_VALUE)
                    throw new IllegalStateException("entry too large, " +
                            "entries are limited to a size of " + Integer.MAX_VALUE);

                if (LOG.isDebugEnabled())
                    LOG.debug("sending entry of entrySize=" + (int) bytesWritten);

                in.writeInt(sizeLocation, (int) bytesWritten);

            } catch (IllegalArgumentException e) {

                // reset the entries position
                entry.position(startOfEntry);

                // reset the in-buffers position
                in.position(pos0);
                long remaining = in.remaining();
                int entrySize = externalizable.sizeOfEntry(entry, chronicleId);


                if (entrySize > remaining) {

                    long newSize = start + entrySize;

                    // This can occur when we pack a number of entries into the buffer and the
                    // last entry is very large.
                    if (newSize > Integer.MAX_VALUE)
                        return false;

                    resizeBuffer((int) newSize);

                    in.position(pos0);
                    entry.position(startOfEntry);
                    return onEntry(entry, chronicleId);
                } else
                    throw e;

            }
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
