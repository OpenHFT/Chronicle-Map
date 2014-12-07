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

import com.sun.jdi.connect.spi.ClosedConnectionException;
import net.openhft.chronicle.hash.RemoteCallTimeoutException;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.thread.NamedThreadFactory;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.ByteBuffer.allocateDirect;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_SIZE;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_TRANSACTION_ID;
import static net.openhft.chronicle.map.StatelessChronicleMap.EventId.*;

/**
 * @author Rob Austin.
 */
class StatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Closeable, Cloneable {

    public static final String START_OF = "Attempt to write ";
    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);
    public static final byte STATELESS_CLIENT_IDENTIFIER = (byte) -127;

    private final byte[] connectionByte = new byte[1];
    private final ByteBuffer connectionOutBuffer = ByteBuffer.wrap(connectionByte);
    private final String name;
    private final AbstractChronicleMapBuilder chronicleMapBuilder;

    private volatile ByteBuffer outBuffer;
    private volatile ByteBufferBytes outBytes;

    private volatile ByteBuffer inBuffer;
    private volatile ByteBufferBytes inBytes;

    private final ReaderWithSize<K> keyReaderWithSize;
    private final WriterWithSize<K> keyWriterWithSize;
    private final ReaderWithSize<V> valueReaderWithSize;
    private final WriterWithSize<V> valueWriterWithSize;
    private volatile SocketChannel clientChannel;

    private CloseablesManager closeables;
    private final StatelessMapConfig config;
    private int maxEntrySize;
    private final Class<K> kClass;
    private final Class<V> vClass;
    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;


    private final ReentrantLock inBytesLock = new ReentrantLock();
    private final ReentrantLock outBytesLock = new ReentrantLock(true);

    private ExecutorService executorService;

    static enum EventId {
        HEARTBEAT,
        STATEFUL_UPDATE,
        LONG_SIZE,
        SIZE,
        IS_EMPTY,
        CONTAINS_KEY,
        CONTAINS_VALUE,
        GET,
        PUT,
        PUT_WITHOUT_ACC,
        REMOVE,
        REMOVE_WITHOUT_ACC,
        CLEAR,
        KEY_SET,
        VALUES,
        ENTRY_SET,
        REPLACE,
        REPLACE_WITH_OLD_AND_NEW_VALUE,
        PUT_IF_ABSENT,
        REMOVE_WITH_VALUE,
        TO_STRING,
        PUT_ALL,
        PUT_ALL_WITHOUT_ACC,
        HASH_CODE,
        MAP_FOR_KEY,
        UPDATE_FOR_KEY
    }

    private AtomicLong transactionID = new AtomicLong(0);

    StatelessChronicleMap(final StatelessMapConfig config,
                          final AbstractChronicleMapBuilder chronicleMapBuilder) throws IOException {
        this.chronicleMapBuilder = chronicleMapBuilder;
        this.config = config;
        keyReaderWithSize = new ReaderWithSize<>(chronicleMapBuilder.keyBuilder);
        keyWriterWithSize = new WriterWithSize<>(chronicleMapBuilder.keyBuilder);
        valueReaderWithSize = new ReaderWithSize<>(chronicleMapBuilder.valueBuilder);
        valueWriterWithSize = new WriterWithSize<>(chronicleMapBuilder.valueBuilder);
        this.putReturnsNull = chronicleMapBuilder.putReturnsNull();
        this.removeReturnsNull = chronicleMapBuilder.removeReturnsNull();
        this.maxEntrySize = chronicleMapBuilder.entrySize(true);

        if (maxEntrySize < 128)
            maxEntrySize = 128; // the stateless client will not work with extreemly small buffers

        this.vClass = chronicleMapBuilder.valueBuilder.eClass;
        this.kClass = chronicleMapBuilder.keyBuilder.eClass;
        this.name = chronicleMapBuilder.name();
        attemptConnect(config.remoteAddress());

        outBuffer = allocateDirect(maxEntrySize).order(ByteOrder.nativeOrder());
        outBytes = new ByteBufferBytes(outBuffer.slice());


        inBuffer = allocateDirect(maxEntrySize).order(ByteOrder.nativeOrder());
        inBytes = new ByteBufferBytes(outBuffer.slice());

    }

    @Override
    public Future<V> getLater(@NotNull final K key) {
        return lazyExecutorService().submit(new Callable<V>() {
            @Override
            public V call() throws Exception {
                return StatelessChronicleMap.this.get(key);
            }
        });
    }

    @Override
    public Future<V> putLater(@NotNull final K key, @NotNull final V value) {
        return lazyExecutorService().submit(new Callable<V>() {
            @Override
            public V call() throws Exception {
                return StatelessChronicleMap.this.put(key, value);
            }
        });
    }

    @Override
    public Future<V> removeLater(@NotNull final K key) {
        return lazyExecutorService().submit(new Callable<V>() {
            @Override
            public V call() throws Exception {
                return StatelessChronicleMap.this.remove(key);
            }
        });
    }

    @Override
    public void getAll(File toFile) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(File fromFile) {
        throw new UnsupportedOperationException();
    }

    private ExecutorService lazyExecutorService() {
        if (executorService == null) {
            synchronized (this) {
                if (executorService == null)
                    executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory(chronicleMapBuilder.name() +
                            "-stateless-client-async-" + name,
                            true));
            }
        }

        return executorService;
    }

    private synchronized SocketChannel lazyConnect(final long timeoutMs,
                                                   final InetSocketAddress remoteAddress) throws IOException {

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress);

        SocketChannel result;

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        for (; ; ) {
            checkTimeout(timeoutAt);

            // ensures that the excising connection are closed
            closeExisting();

            try {
                result = AbstractChannelReplicator.openSocketChannel(closeables);
                result.connect(config.remoteAddress());
                result.socket().setTcpNoDelay(true);

                doHandShaking(result);
                break;
            } catch (IOException e) {
                closeables.closeQuietly();
            } catch (Exception e) {
                closeables.closeQuietly();
                throw e;
            }
        }

        return result;
    }

    /**
     * attempts a single connect without a timeout and eat any IOException used typically in the
     * constructor, its possible the server is not running with this instance is created, {@link
     * net.openhft.chronicle.map.StatelessChronicleMap#lazyConnect(long,
     * java.net.InetSocketAddress)} will attempt to establish the connection when the client make
     * the first map method call.
     *
     * @param remoteAddress the  Inet Socket Address
     * @return
     * @throws java.io.IOException
     * @see net.openhft.chronicle.map.StatelessChronicleMap#lazyConnect(long,
     * java.net.InetSocketAddress)
     */

    private synchronized void attemptConnect(final InetSocketAddress remoteAddress) throws IOException {

        // ensures that the excising connection are closed
        closeExisting();

        try {
            clientChannel = AbstractChannelReplicator.openSocketChannel(closeables);
            clientChannel.connect(remoteAddress);
            doHandShaking(clientChannel);
        } catch (IOException e) {
            closeables.closeQuietly();
        }
    }

    /**
     * closes the existing connections and establishes a new closeables
     */
    private void closeExisting() {
        // ensure that any excising connection are first closed
        if (closeables != null)
            closeables.closeQuietly();

        closeables = new CloseablesManager();
    }

    /**
     * initiates a very simple level of handshaking with the remote server, we send a special ID of
     * -127 ( when the server receives this it knows its dealing with a stateless client, receive
     * back an identifier from the server
     *
     * @param clientChannel clientChannel
     * @throws java.io.IOException
     */
    private synchronized void doHandShaking(@NotNull final SocketChannel clientChannel) throws IOException {

        connectionByte[0] = STATELESS_CLIENT_IDENTIFIER;
        this.connectionOutBuffer.clear();

        long timeoutTime = System.currentTimeMillis() + config.timeoutMs();

        // write a single byte
        while (connectionOutBuffer.hasRemaining()) {
            clientChannel.write(connectionOutBuffer);
            checkTimeout(timeoutTime);
        }

        this.connectionOutBuffer.clear();

        if (!clientChannel.finishConnect() || !clientChannel.socket().isBound())
            return;

        // read a single byte back
        while (this.connectionOutBuffer.position() <= 0) {
            clientChannel.read(this.connectionOutBuffer);  // the remote identifier
            checkTimeout(timeoutTime);
        }

        byte remoteIdentifier = connectionByte[0];

        if (LOG.isDebugEnabled())
            LOG.debug("Attached to a map with a remote identifier=" + remoteIdentifier);
    }

    public File file() {
        throw new UnsupportedOperationException();
    }

    public void close() {

        if (closeables != null)
            closeables.closeQuietly();
        closeables = null;

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.error("", e);
            }
        }

    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param time in milliseconds
     * @return a unique transactionId
     */
    private long nextUniqueTransaction(long time) {
        long old = transactionID.getAndSet(time);

        if (old == time)
            return transactionID.incrementAndGet();
        else
            return time;

    }

    public V putIfAbsent(K key, V value) {

        if (key == null || value == null)
            throw new NullPointerException();

        return fetchObject(vClass, PUT_IF_ABSENT, key, value);
    }


    public boolean remove( Object key, Object value) {

        if (key == null)
            throw new NullPointerException();

        if (value == null)
            return false;

        return fetchBoolean(REMOVE_WITH_VALUE, (K) key, (V) value);
    }


    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();

        return fetchBoolean(REPLACE_WITH_OLD_AND_NEW_VALUE, key, oldValue, newValue);
    }

    public V replace(K key,  V value) {
        if (key == null || value == null)
            throw new NullPointerException();

        return fetchObject(vClass, REPLACE, key, value);
    }

    public int size() {
        return (int) longSize();
    }

    /**
     * calling this method should be avoided at all cost, as the entire {@code object} is
     * serialized. This equals can be used to compare map that extends ChronicleMap.  So two
     * Chronicle Maps that contain the same data are considered equal, even if the instances of the
     * chronicle maps were of different types
     *
     * @param object the object that you are comparing against
     * @return true if the contain the same data
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || object.getClass().isAssignableFrom(Map.class))
            return false;

        final Map<? extends K, ? extends V> that = (Map<? extends K, ? extends V>) object;

        final int size = size();

        if (that.size() != size)
            return false;

        final Set<Map.Entry<K, V>> entries = entrySet();
        return that.entrySet().equals(entries);
    }

    @Override
    public int hashCode() {
        return fetchInt(HASH_CODE);
    }

    public String toString() {
        return fetchObject(String.class, TO_STRING);
    }

    public boolean isEmpty() {
        return fetchBoolean(IS_EMPTY);
    }


    public boolean containsKey(Object key) {
        return fetchBooleanK(CONTAINS_KEY, (K) key);
    }

    private NullPointerException keyNotNullNPE() {
        return new NullPointerException("key can not be null");
    }

    public boolean containsValue(Object value) {
        return fetchBooleanV(CONTAINS_VALUE, (V) value);
    }

    public long longSize() {
        return fetchLong(LONG_SIZE);
    }

    public V get(Object key) {
        return fetchObject(vClass, GET, (K) key);
    }

    public V getUsing(K key, V usingValue) {
        throw new UnsupportedOperationException("getUsing() is not supported for stateless " +
                "clients");
    }

    public V acquireUsing(@NotNull K key, V usingValue) {
        throw new UnsupportedOperationException("acquireUsing() is not supported for stateless " +
                "clients");
    }

    @NotNull
    @Override
    public WriteContext<K, V> acquireUsingLocked(@NotNull K key, @NotNull V
            usingValue) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ReadContext<K, V> getUsingLocked(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException();
    }

    public V remove(Object key) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(vClass, removeReturnsNull ? REMOVE_WITHOUT_ACC : REMOVE, (K) key);
    }

    public V put(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return fetchObject(vClass, putReturnsNull ? PUT_WITHOUT_ACC : PUT, key, value);
    }

    public <R> R mapForKey(K key, @NotNull Function<? super V, R> function) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(MAP_FOR_KEY, key, function);
    }


    @Override
    public <R> R updateForKey(K key, @NotNull Mutator<? super V, R> mutator) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(UPDATE_FOR_KEY, key, mutator);
    }


    private synchronized void writeEntriesForPutAll(Map<? extends K, ? extends V> map) {
        final int numberOfEntries = map.size();
        int numberOfEntriesReadSoFar = 0;

        outBytes.writeStopBit(numberOfEntries);
        assert outBytes.limit() == outBytes.capacity();

        ThreadLocalCopies copies = keyWriterWithSize.getCopies(null);
        final Object keyWriter = keyWriterWithSize.writerForLoop(copies);
        copies = valueWriterWithSize.getCopies(copies);
        final Object valueWriter = valueWriterWithSize.writerForLoop(copies);

        for (final Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            final K key = e.getKey();
            final V value = e.getValue();
            if (key == null || value == null)
                throw new NullPointerException();

            numberOfEntriesReadSoFar++;

            long start = outBytes.position();

            // putAll if a bit more complicated than the others
            // as the volume of data could cause us to have to resize our buffers
            resizeIfRequired(numberOfEntries, numberOfEntriesReadSoFar, start);

            final Class<?> keyClass = key.getClass();
            if (!kClass.isAssignableFrom(keyClass)) {
                throw new ClassCastException("key=" + key + " is of type=" + keyClass + " " +
                        "and should" +
                        " be of type=" + kClass);
            }

            writeKeyInLoop(key, keyWriter, copies);

            final Class<?> valueClass = value.getClass();
            if (!vClass.isAssignableFrom(valueClass))
                throw new ClassCastException("value=" + value + " is of type=" + valueClass +
                        " and " +
                        "should  be of type=" + vClass);

            writeValueInLoop(value, valueWriter, copies);

            final int len = (int) (outBytes.position() - start);

            if (len > maxEntrySize)
                maxEntrySize = len;
        }
    }

    private void resizeIfRequired(int numberOfEntries, int numberOfEntriesReadSoFar, final long start) {
        final long remaining = outBytes.remaining();

        if (remaining < maxEntrySize) {
            long estimatedRequiredSize = estimateSize(numberOfEntries, numberOfEntriesReadSoFar);
            resizeBufferOutBuffer((int) (estimatedRequiredSize + maxEntrySize), start);
        }
    }

    /**
     * estimates the size based on what been completed so far
     */
    private long estimateSize(int numberOfEntries, int numberOfEntriesReadSoFar) {
        // we will back the size estimate on what we have so far
        final double percentageComplete = (double) numberOfEntriesReadSoFar / (double) numberOfEntries;
        return (long) ((double) outBytes.position() / percentageComplete);
    }

    private void resizeBufferOutBuffer(int size, long start) {
        if (LOG.isDebugEnabled())
            LOG.debug("resizing buffer to size=" + size);

        if (size < outBuffer.capacity())
            throw new IllegalStateException("it not possible to resize the buffer smaller");

        assert size < Integer.MAX_VALUE;

        final ByteBuffer result = ByteBuffer.allocate(size).order(ByteOrder.nativeOrder());
        final long bytesPosition = outBytes.position();

        outBytes = new ByteBufferBytes(result.slice());

        outBuffer.position(0);
        outBuffer.limit((int) bytesPosition);

        int numberOfLongs = (int) bytesPosition / 8;

        // chunk in longs first
        for (int i = 0; i < numberOfLongs; i++) {
            outBytes.writeLong(outBuffer.getLong());
        }

        for (int i = numberOfLongs * 8; i < bytesPosition; i++) {
            outBytes.writeByte(outBuffer.get());
        }

        outBuffer = result;

        assert outBuffer.capacity() == outBytes.capacity();

        assert outBuffer.capacity() == size;
        assert outBuffer.capacity() == outBytes.capacity();
        assert outBytes.limit() == outBytes.capacity();
        outBytes.position(start);
    }

    private void resizeBufferInBuffer(int size, long start) {
        if (LOG.isDebugEnabled())
            LOG.debug("InBuffer resizing buffer to size=" + size);

        if (size < inBuffer.capacity())
            throw new IllegalStateException("it not possible to resize the buffer smaller");

        assert size < Integer.MAX_VALUE;

        final ByteBuffer result = ByteBuffer.allocate((int) size).order(ByteOrder.nativeOrder());
        final long bytesPosition = inBytes.position();

        inBytes = new ByteBufferBytes(result.slice());

        inBuffer.position(0);
        inBuffer.limit((int) bytesPosition);

        int numberOfLongs = (int) bytesPosition / 8;

        // chunk in longs first
        for (int i = 0; i < numberOfLongs; i++) {
            inBytes.writeLong(inBuffer.getLong());
        }

        for (int i = numberOfLongs * 8; i < bytesPosition; i++) {
            inBytes.writeByte(inBuffer.get());
        }

        inBuffer = result;

        assert inBuffer.capacity() == inBytes.capacity();
        assert inBuffer.capacity() == size;
        assert inBuffer.capacity() == inBytes.capacity();
        assert inBytes.limit() == inBytes.capacity();

        inBytes.position(start);
    }

    public void clear() {

        fetchVoid(CLEAR);
    }

    @NotNull
    public Collection<V> values() {
        final long sizeLocation = writeEventAnSkip(VALUES);

        final long startTime = System.currentTimeMillis();

        final long timeoutTime = System.currentTimeMillis() + config.timeoutMs();
        final long transactionId = send(sizeLocation, startTime);

        // get the data back from the server
        final Collection<V> result = new ArrayList<V>();

        BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(null);
        for (; ; ) {
            inBytesLock.lock();
            try {

                final Bytes in = blockingFetchReadOnly(timeoutTime, transactionId);

                final boolean hasMoreEntries = in.readBoolean();

                // number of entries in this chunk
                final long size = in.readInt();

                for (int i = 0; i < size; i++) {
                    result.add(valueReaderWithSize.readInLoop(in, valueReader));
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                inBytesLock.unlock();
            }
        }
        return result;
    }

    @NotNull
    public Set<Map.Entry<K, V>> entrySet() {

        final long sizeLocation = writeEventAnSkip(ENTRY_SET);
        final long startTime = System.currentTimeMillis();
        final long timeoutTime = System.currentTimeMillis() + config.timeoutMs();
        final long transactionId = send(sizeLocation, startTime);

        // get the data back from the server
        ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
        final BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(copies);
        copies = valueReaderWithSize.getCopies(copies);
        final BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(copies);
        final Map<K, V> result = new HashMap<K, V>();

        for (; ; ) {

            inBytesLock.lock();
            try {

                Bytes in = blockingFetchReadOnly(timeoutTime, transactionId);

                final boolean hasMoreEntries = in.readBoolean();

                // number of entries in this chunk
                final long size = in.readInt();

                for (int i = 0; i < size; i++) {
                    final K k = keyReaderWithSize.readInLoop(in, keyReader);
                    final V v = valueReaderWithSize.readInLoop(in, valueReader);
                    result.put(k, v);
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                inBytesLock.unlock();
            }
        }

        return result.entrySet();
    }

    public void putAll(@NotNull Map<? extends K, ? extends V> map) {


        final long sizeLocation = putReturnsNull ? writeEvent(PUT_ALL_WITHOUT_ACC) :
                writeEventAnSkip(PUT_ALL);

        final long startTime = System.currentTimeMillis();
        final long timeoutTime = startTime + config.timeoutMs();
        final long transactionId;
        final int numberOfEntries = map.size();
        int numberOfEntriesReadSoFar = 0;

        outBytesLock.lock();
        try {

            outBytes.writeStopBit(numberOfEntries);
            assert outBytes.limit() == outBytes.capacity();
            ThreadLocalCopies copies = keyWriterWithSize.getCopies(null);
            final Object keyWriter = keyWriterWithSize.writerForLoop(copies);
            copies = valueWriterWithSize.getCopies(copies);
            final Object valueWriter = valueWriterWithSize.writerForLoop(copies);

            for (final Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
                final K key = e.getKey();
                final V value = e.getValue();
                if (key == null || value == null)
                    throw new NullPointerException();

                numberOfEntriesReadSoFar++;

                long start = outBytes.position();

                // putAll if a bit more complicated than the others
                // as the volume of data could cause us to have to resize our buffers
                resizeIfRequired(numberOfEntries, numberOfEntriesReadSoFar, start);

                final Class<?> keyClass = key.getClass();
                if (!kClass.isAssignableFrom(keyClass)) {
                    throw new ClassCastException("key=" + key + " is of type=" + keyClass + " " +
                            "and should" +
                            " be of type=" + kClass);
                }

                writeKeyInLoop(key, keyWriter, copies);

                final Class<?> valueClass = value.getClass();
                if (!vClass.isAssignableFrom(valueClass))
                    throw new ClassCastException("value=" + value + " is of type=" + valueClass +
                            " and " +
                            "should  be of type=" + vClass);

                writeValueInLoop(value, valueWriter, copies);

                final int len = (int) (outBytes.position() - start);

                if (len > maxEntrySize)
                    maxEntrySize = len;
            }

            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }


        if (!putReturnsNull)
            blockingFetchReadOnly(timeoutTime, transactionId);
    }

    @NotNull
    public Set<K> keySet() {

        final long sizeLocation = writeEventAnSkip(KEY_SET);
        final long startTime = System.currentTimeMillis();
        final long timeoutTime = startTime + config.timeoutMs();
        final long transactionId = send(sizeLocation, startTime);
        final Set<K> result = new HashSet<>();
        final BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(null);

        for (; ; ) {

            inBytesLock.lock();
            try {

                final Bytes in = blockingFetchReadOnly(timeoutTime, transactionId);
                final boolean hasMoreEntries = in.readBoolean();

                // number of entries in the chunk
                long size = in.readInt();

                for (int i = 0; i < size; i++) {
                    result.add(keyReaderWithSize.readInLoop(in, keyReader));
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                inBytesLock.unlock();
            }
        }

        return result;
    }


    private long readLong(long transactionId, long startTime) {
        final long timeoutTime = startTime + this.config.timeoutMs();

        // receive
        inBytesLock.lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readLong();
        } finally {
            inBytesLock.unlock();
        }
    }


    private long writeEvent(StatelessChronicleMap.EventId event) {
        outBuffer.clear();
        outBytes.clear();

        outBytes.write((byte) event.ordinal());

        return markSizeLocation();
    }

    /**
     * skips for the transactionid
     */
    private long writeEventAnSkip(EventId event) {
        final long sizeLocation = writeEvent(event);

        // skips for the transaction id
        outBytes.skip(SIZE_OF_TRANSACTION_ID);
        return sizeLocation;
    }

    class Entry implements Map.Entry<K, V> {

        final K key;
        final V value;

        /**
         * Creates new entry.
         */
        Entry(K k1, V v) {
            value = v;
            key = k1;
        }

        public final K getKey() {
            return key;
        }

        public final V getValue() {
            return value;
        }

        public final V setValue(V newValue) {
            V oldValue = value;
            StatelessChronicleMap.this.put((K) getKey(), (V) newValue);
            return oldValue;
        }

        public final boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            final Map.Entry e = (Map.Entry) o;
            final Object k1 = getKey();
            final Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2)))
                    return true;
            }
            return false;
        }

        public final int hashCode() {
            return (key == null ? 0 : key.hashCode()) ^
                    (value == null ? 0 : value.hashCode());
        }

        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }


    /**
     * sends data to the server via TCP/IP
     *
     * @param sizeLocation the position of the bit that marks the size
     * @param startTime    the current time
     * @return a unique transaction id
     */
    private long send(long sizeLocation, final long startTime) {
        long transactionId = nextUniqueTransaction(startTime);
        final long timeoutTime = startTime + this.config.timeoutMs();
        try {

            for (; ; ) {
                if (clientChannel == null)
                    clientChannel = lazyConnect(config.timeoutMs(), config.remoteAddress());

                try {

                    if (LOG.isDebugEnabled())
                        LOG.debug("sending data with transactionId=" + transactionId);

                    writeSizeAndTransactionIdAt(sizeLocation, transactionId);

                    // send out all the bytes
                    send(outBytes, timeoutTime);
                    outBytes.clear();
                    outBytes.buffer().clear();
                    break;

                } catch (java.nio.channels.ClosedChannelException | ClosedConnectionException e) {
                    checkTimeout(timeoutTime);
                    clientChannel = lazyConnect(config.timeoutMs(), config.remoteAddress());
                }
            }
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }
        return transactionId;
    }

    private Bytes blockingFetchReadOnly(long timeoutTime, final long transactionId) {
        try {
            return blockingFetchThrowable(timeoutTime, transactionId);
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (RuntimeException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        } catch (AssertionError e) {
            LOG.error("", e);
            throw e;
        }
    }

    // this is a transactionid and size that has been read by another thread.
    private volatile long parkedTransactionId;
    private volatile int parkedRemainingBytes;
    private volatile long parkedTransactionTimeStamp;

    private Bytes blockingFetchThrowable(long timeoutTime, long transactionId) throws IOException,
            InterruptedException {
        int remainingBytes = 0;


        for (; ; ) {

            // read the next item from the socket
            if (parkedTransactionId == 0) {

                assert parkedTransactionTimeStamp == 0;
                assert parkedRemainingBytes == 0;

                receive(SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID, timeoutTime);

                final int messageSize = inBytes.readInt();
                final int remainingBytes0 = messageSize - (SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID);
                final long transactionId0 = inBytes.readLong();

                assert transactionId0 != 0;

                // check the transaction id is reasonable
                assert String.valueOf(transactionId0).startsWith("14");

                // if the transaction id is for this thread process it
                if (transactionId0 == transactionId) {

                    // we have the correct transaction id !
                    parkedTransactionId = 0;
                    remainingBytes = remainingBytes0;
                    assert remainingBytes > 0;

                    clearParked();
                    break;

                } else {
                    // if the transaction id is not for this thread, park it and read the next one
                    parkedTransactionTimeStamp = System.currentTimeMillis();
                    parkedRemainingBytes = remainingBytes0;
                    parkedTransactionId = transactionId0;

                    pause();
                    continue;
                }
            }

            // the transaction id was read by another thread, but is for this thread, process it
            if (parkedTransactionId == transactionId) {
                remainingBytes = parkedRemainingBytes;
                clearParked();
                break;
            }


            // time out the old transaction id
            if (System.currentTimeMillis() - timeoutTime >
                    parkedTransactionTimeStamp) {

                LOG.error("", new IllegalStateException("Skipped Message with transaction-id=" +
                        parkedTransactionTimeStamp +
                        ", this can occur when you have another thread which has called the " +
                        "stateless client and terminated abruptly before the message has been " +
                        "returned from the server"));

                // read the the next message
                receive(parkedRemainingBytes, timeoutTime);
                clearParked();

            }

            pause();
        }


        final int minBufferSize = remainingBytes;

        if (inBytes.capacity() < minBufferSize) {
            long pos = inBytes.position();
            long limit = inBytes.position();
            inBytes.position(limit);
            resizeBufferInBuffer(minBufferSize, pos);
        } else
            inBytes.limit(inBytes.capacity());

        // block until we have received all the bytes in this chunk
        receive(remainingBytes, timeoutTime);

        final boolean isException = inBytes.readBoolean();

        if (isException) {
            Throwable throwable = (Throwable) inBytes.readObject();
            try {
                Field stackTrace = Throwable.class.getDeclaredField("stackTrace");
                stackTrace.setAccessible(true);
                List<StackTraceElement> stes = new ArrayList<>(Arrays.asList((StackTraceElement[]) stackTrace.get(throwable)));
                // prune the end of the stack.
                for (int i = stes.size() - 1; i > 0 && stes.get(i).getClassName().startsWith("Thread"); i--) {
                    stes.remove(i);
                }
                InetSocketAddress address = config.remoteAddress();
                stes.add(new StackTraceElement("~ remote", "tcp ~", address.getHostName(), address.getPort()));
                StackTraceElement[] stackTrace2 = Thread.currentThread().getStackTrace();
                for (int i = 4; i < stackTrace2.length; i++)
                    stes.add(stackTrace2[i]);
                stackTrace.set(throwable, stes.toArray(new StackTraceElement[stes.size()]));
            } catch (Exception ignore) {
            }
            NativeBytes.UNSAFE.throwException(throwable);
        }


        return inBytes;
    }

    private void clearParked() {
        parkedTransactionId = 0;
        parkedRemainingBytes = 0;
        parkedTransactionTimeStamp = 0;
    }

    private void pause() throws InterruptedException {

        /// don't call inBytesLock.isHeldByCurrentThread() as it not atomic
        inBytesLock.unlock();
        inBytesLock.lock();
    }


    /**
     * reads up to the number of byte in {@code requiredNumberOfBytes}
     *
     * @param requiredNumberOfBytes the number of bytes to read
     * @param timeoutTime           timeout in milliseconds
     * @return bytes read from the TCP/IP socket
     * @throws IOException socket failed to read data
     */
    private Bytes receive(int requiredNumberOfBytes, long timeoutTime) throws IOException {

        inBytes.buffer().position(0);
        inBytes.buffer().limit(requiredNumberOfBytes);

        while (inBytes.buffer().remaining() > 0) {
            assert requiredNumberOfBytes <= inBytes.capacity();

            int len = clientChannel.read(inBytes.buffer());

            if (len == -1)
                throw new IORuntimeException("Disconnected to remote server");

            checkTimeout(timeoutTime);
        }

        inBytes.position(0);
        inBytes.limit(requiredNumberOfBytes);
        return inBytes;
    }


    private void send(final Bytes out, long timeoutTime) throws IOException {

        outBuffer.limit((int) out.position());
        outBuffer.position(0);

        while (outBuffer.remaining() > 0) {
            clientChannel.write(outBuffer);
            checkTimeout(timeoutTime);
        }

        out.clear();
        outBuffer.clear();
    }

    private void checkTimeout(long timeoutTime) {
        if (timeoutTime < System.currentTimeMillis())
            throw new RemoteCallTimeoutException();
    }

    private void writeSizeAndTransactionIdAt(long locationOfSize, final long transactionId) {
        final long size = outBytes.position() - locationOfSize;
        final long pos = outBytes.position();
        outBytes.position(locationOfSize);
        int size0 = (int) size - SIZE_OF_SIZE;
        assert size0 > 0;

        outBytes.writeInt(size0);
        outBytes.writeLong(transactionId);
        outBytes.position(pos);
    }

    private void writeSizeAt(long locationOfSize) {
        final long size = outBytes.position() - locationOfSize;
        int size0 = (int) size - SIZE_OF_SIZE;
        assert size0 > 0;
        outBytes.writeInt(locationOfSize, size0);
    }

    private long markSizeLocation() {
        final long position = outBytes.position();
        outBytes.skip(SIZE_OF_SIZE);
        return position;
    }

    private ThreadLocalCopies writeKey(K key) {
        return writeKey(key, null);
    }

    private ThreadLocalCopies writeKey(K key, ThreadLocalCopies copies) {
        long start = outBytes.position();

        for (; ; ) {
            try {
                return keyWriterWithSize.write(outBytes, key, copies);
            } catch (IndexOutOfBoundsException e) {
                resizeBufferOutBuffer((int) (outBytes.capacity() + maxEntrySize), start);
            } catch (IllegalArgumentException e) {
                resizeToMessage(start, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("Not enough available space"))
                    resizeBufferOutBuffer((int) (outBytes.capacity() + maxEntrySize), start);
                else
                    throw e;
            }
        }
    }

    private ThreadLocalCopies writeKeyInLoop(K key, Object writer, ThreadLocalCopies copies) {
        long start = outBytes.position();

        for (; ; ) {
            try {
                return keyWriterWithSize.writeInLoop(outBytes, key, writer, copies);
            } catch (IndexOutOfBoundsException e) {
                resizeBufferOutBuffer((int) (outBytes.capacity() + maxEntrySize), start);
            } catch (IllegalArgumentException e) {
                resizeToMessage(start, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("Not enough available space"))
                    resizeBufferOutBuffer((int) (outBytes.capacity() + maxEntrySize), start);
                else
                    throw e;
            }
        }
    }

    private ThreadLocalCopies writeValue(V value, ThreadLocalCopies copies) {
        long start = outBytes.position();
        for (; ; ) {
            try {
                assert outBytes.position() == start;
                outBytes.limit(outBytes.capacity());
                return valueWriterWithSize.write(outBytes, value, copies);
            } catch (IllegalArgumentException e) {
                resizeToMessage(start, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("Not enough available space"))
                    resizeBufferOutBuffer((int) (outBytes.capacity() + maxEntrySize), start);
                else
                    throw e;
            }
        }
    }

    private ThreadLocalCopies writeValueInLoop(V value, Object writer, ThreadLocalCopies copies) {
        long start = outBytes.position();
        for (; ; ) {
            try {
                assert outBytes.position() == start;
                outBytes.limit(outBytes.capacity());
                return valueWriterWithSize.writeInLoop(outBytes, value, writer, copies);
            } catch (IllegalArgumentException e) {
                resizeToMessage(start, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("Not enough available space"))
                    resizeBufferOutBuffer((int) (outBytes.capacity() + maxEntrySize), start);
                else
                    throw e;
            }
        }
    }

    private void resizeToMessage(long start, IllegalArgumentException e) {
        String message = e.getMessage();
        if (message.startsWith(START_OF)) {
            String substring = message.substring("Attempt to write ".length(), message.length());
            int i = substring.indexOf(' ');
            if (i != -1) {
                int size = Integer.parseInt(substring.substring(0, i));

                long requiresExtra = size - outBytes.remaining();
                resizeBufferOutBuffer((int) (outBytes.capacity() + requiresExtra), start);
            } else
                resizeBufferOutBuffer((int) (outBytes.capacity() + maxEntrySize), start);
        } else
            throw e;
    }

    private V readValue(long transactionId, long startTime, ThreadLocalCopies copies) {
        long timeoutTime = startTime + this.config.timeoutMs();

        // receive
        inBytesLock.lock();
        try {
            return readValue(copies, blockingFetchReadOnly(timeoutTime, transactionId));
        } finally {
            inBytesLock.unlock();
        }
    }

    private <O> O readObject(long transactionId, long startTime) {
        long timeoutTime = startTime + this.config.timeoutMs();

        // receive
        inBytesLock.lock();
        try {
            return (O) blockingFetchReadOnly(timeoutTime, transactionId).readObject();
        } finally {
            inBytesLock.unlock();
        }
    }

    private boolean readBoolean(long transactionId, long startTime) {
        long timeoutTime = startTime + this.config.timeoutMs();

        // receive
        inBytesLock.lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readBoolean();
        } finally {
            inBytesLock.unlock();
        }
    }


    private int readInt(long transactionId, long startTime) {
        long timeoutTime = startTime + this.config.timeoutMs();

        // receive
        inBytesLock.lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readInt();
        } finally {
            inBytesLock.unlock();
        }
    }

    private long send(final EventId eventId, final long startTime) {
        // send
        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            return send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }
    }

    private V readValue(ThreadLocalCopies copies, Bytes in) {
        return valueReaderWithSize.readNullable(in, copies);
    }

    private boolean fetchBoolean(final EventId eventId, K key, V value) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);

            copies = writeKey(key);
            writeValue(value, copies);

            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }


        return readBoolean(transactionId, startTime);

    }

    private boolean fetchBoolean(final EventId eventId, K key, V value1, V value2) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);


            copies = writeKey(key);
            copies = writeValue(value1, copies);
            writeValue(value2, copies);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }


        return readBoolean(transactionId, startTime);

    }

    private boolean fetchBooleanV(final EventId eventId, V value) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            writeValue(value, null);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        return readBoolean(transactionId, startTime);
    }


    private boolean fetchBooleanK(final EventId eventId, K key) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            writeKey(key, null);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        return readBoolean(transactionId, startTime);
    }


    private long fetchLong(final EventId eventId) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        return readLong(transactionId, startTime);
    }

    private boolean fetchBoolean(final EventId eventId) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    private void fetchVoid(final EventId eventId) {

        final long startTime = System.currentTimeMillis();

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        readVoid(transactionId, startTime);
    }

    private void readVoid(long transactionId, long startTime) {
        long timeoutTime = startTime + this.config.timeoutMs();

        // receive
        inBytesLock.lock();
        try {
            blockingFetchReadOnly(timeoutTime, transactionId);
        } finally {
            inBytesLock.unlock();
        }
    }


    private <O> O fetchObject(final Class<O> tClass, final EventId eventId) {
        final long startTime = System.currentTimeMillis();
        long transactionId = send(eventId, startTime);

        long timeoutTime = startTime + this.config.timeoutMs();

        // receive
        inBytesLock.lock();
        try {
            return (O) blockingFetchReadOnly(timeoutTime, transactionId).readObject(tClass);
        } finally {
            inBytesLock.unlock();
        }
    }


    private int fetchInt(final EventId eventId) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        return readInt(transactionId, startTime);
    }


    private <R> R fetchObject(Class<R> rClass, final EventId eventId, K key, V value) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {

            final long sizeLocation = eventReturnsNull(eventId)
                    ? writeEvent(eventId)
                    : writeEventAnSkip(eventId);

            copies = writeKey(key);
            copies = writeValue(value, copies);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }


        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, copies);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }

    private <R> R fetchObject(Class<R> rClass, final EventId eventId, K key, V value1, V value2) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {

            final long sizeLocation = eventReturnsNull(eventId)
                    ? writeEvent(eventId)
                    : writeEventAnSkip(eventId);

            copies = writeKey(key);
            copies = writeValue(value1, copies);
            copies = writeValue(value2, copies);

            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }


        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, copies);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }

    private <R> R fetchObject(Class<R> rClass, final EventId eventId, K key) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {

            final long sizeLocation = eventReturnsNull(eventId)
                    ? writeEvent(eventId)
                    : writeEventAnSkip(eventId);

            copies = writeKey(key);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, copies);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }


    private boolean eventReturnsNull(EventId eventId) {

        switch (eventId) {
            case PUT_ALL_WITHOUT_ACC:
            case PUT_WITHOUT_ACC:
            case REMOVE_WITHOUT_ACC:
                return true;
            default:
                return false;
        }

    }

    private void writeObject(@NotNull Object function) {
        long start = outBytes.position();
        for (; ; ) {
            try {
                outBytes.writeObject(function);
                return;
            } catch (IllegalStateException e) {
                Throwable cause = e.getCause();

                if (cause instanceof IOException && cause.getMessage().contains("Not enough available space")) {
                    LOG.debug("resizing buffer");
                    resizeBufferOutBuffer(outBuffer.capacity() + maxEntrySize, start);
                } else
                    throw e;
            }
        }
    }


    private <R> R fetchObject(final EventId eventId, K key, Object object) {
        final long startTime = System.currentTimeMillis();
        long transactionId;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            writeKey(key);
            writeObject(object);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        if (eventReturnsNull(eventId))
            return null;

        return (R) readObject(transactionId, startTime);

    }
}

