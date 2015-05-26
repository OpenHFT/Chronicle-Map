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

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.hash.RemoteCallTimeoutException;
import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.impl.util.CloseablesManager;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.ReaderWithSize;
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.nio.ByteBuffer.allocateDirect;
import static java.util.Collections.emptyList;
import static net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.IS_DEBUG;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_SIZE;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_TRANSACTION_ID;
import static net.openhft.chronicle.map.StatelessChronicleMap.EventId.*;

/**
 * @author Rob Austin.
 */
class StatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Closeable, Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);
    private static final byte STATELESS_CLIENT_IDENTIFIER = (byte) -127;

    private final byte[] connectionByte = new byte[1];
    private final ByteBuffer connectionOutBuffer = ByteBuffer.wrap(connectionByte);
    private final String name;

    private ByteBuffer outBuffer;
    private ByteBufferBytes outBytes;

    private ByteBuffer inBuffer;
    private ByteBufferBytes inBytes;
    private ReaderWithSize<K> keyReaderWithSize;
    private WriterWithSize<K> keyWriterWithSize;
    private ReaderWithSize<V> valueReaderWithSize;
    private WriterWithSize<V> valueWriterWithSize;
    private SocketChannel clientChannel;

    @Nullable
    private CloseablesManager closeables;
    private final InetSocketAddress remoteAddress;
    private final long timeoutMs;
    private final int tcpBufferSize;

    private Class<K> kClass;
    private Class<V> vClass;
    private boolean putReturnsNull;
    private boolean removeReturnsNull;


    private final ReentrantLock inBytesLock = new ReentrantLock(true);
    private final ReentrantLock outBytesLock = new ReentrantLock();

    private final BufferResizer outBufferResizer = newCapacity -> resizeBufferOutBuffer(newCapacity);

    enum EventId {
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
        APPLICATION_VERSION,
        PERSISTED_DATA_VERSION,
        PUT_ALL,
        PUT_ALL_WITHOUT_ACC,
        HASH_CODE,
        MAP_FOR_KEY,
        PUT_MAPPED,
        KEY_BUILDER,
        VALUE_BUILDER
    }


    //  used by the enterprise version
    private int identifier;

    @SuppressWarnings("UnusedDeclaration")
    void identifier(int identifier) {
        this.identifier = identifier;
    }


    @NotNull
    private final AtomicLong transactionID = new AtomicLong(0);

    StatelessChronicleMap(@NotNull final ChronicleMapStatelessClientBuilder config) {
        this.remoteAddress = config.remoteAddress();
        this.timeoutMs = config.timeoutMs();
        this.tcpBufferSize = config.tcpBufferSize();
        this.name = config.name();
        this.putReturnsNull = config.putReturnsNull();
        this.removeReturnsNull = config.removeReturnsNull();

        outBuffer = allocateDirect(128).order(ByteOrder.nativeOrder());
        outBytes = new ByteBufferBytes(outBuffer.slice());

        inBuffer = allocateDirect(128).order(ByteOrder.nativeOrder());
        inBytes = new ByteBufferBytes(inBuffer.slice());

        attemptConnect(remoteAddress);

        checkVersion();
        loadKeyValueSerializers();
    }

    private void loadKeyValueSerializers() {
        final SerializationBuilder keyBuilder =
                fetchObject(SerializationBuilder.class, KEY_BUILDER);
        kClass = keyBuilder.eClass;
        final SerializationBuilder valueBuilder =
                fetchObject(SerializationBuilder.class, VALUE_BUILDER);
        vClass = valueBuilder.eClass;

        keyReaderWithSize = new ReaderWithSize(keyBuilder);
        keyWriterWithSize = new WriterWithSize(keyBuilder, outBufferResizer);

        valueReaderWithSize = new ReaderWithSize(valueBuilder);
        valueWriterWithSize = new WriterWithSize(valueBuilder, outBufferResizer);

    }

    private void checkVersion() {

        String serverVersion = serverApplicationVersion();
        String clientVersion = clientVersion();

        if (!serverVersion.equals(clientVersion)) {
            LOG.warn("DIFFERENT CHRONICLE-MAP VERSIONS: The Chronicle-Map-Server and " +
                    "Stateless-Client are on different " +
                    "versions, " +
                    " we suggest that you use the same version, server=" + serverApplicationVersion() + ", " +
                    "client=" + clientVersion);
        }
    }

    @Override
    public void getAll(File toFile) throws IOException {
        JsonSerializer.getAll(toFile, this, emptyList());
    }

    @Override
    public void putAll(File fromFile) throws IOException {
        JsonSerializer.putAll(fromFile, this, emptyList());
    }

    @Override
    public V newValueInstance() {
        if (vClass.equals(CharSequence.class) || vClass.equals(StringBuilder.class)) {
            return (V) new StringBuilder();
        }
        return VanillaChronicleMap.newInstance(vClass, false);
    }

    @Override
    public K newKeyInstance() {
        return VanillaChronicleMap.newInstance(kClass, true);
    }

    @Override
    public Class<K> keyClass() {
        return kClass;
    }

    @Override
    public boolean forEachEntryWhile(Predicate<? super MapKeyContext<K, V>> predicate) {
        // TODO implement!
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachEntry(Consumer<? super MapKeyContext<K, V>> action) {
        // TODO implement!
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<V> valueClass() {
        return vClass;
    }


    private void checkTimeout(long timeoutTime) {
        if (timeoutTime < System.currentTimeMillis() && !IS_DEBUG)
            throw new RemoteCallTimeoutException();
    }

    private synchronized void lazyConnect(final long timeoutMs,
                                          final InetSocketAddress remoteAddress) {
        if (clientChannel != null)
            return;

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress + " ,name=" + name);

        SocketChannel result;

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        for (; ; ) {
            checkTimeout(timeoutAt);

            // ensures that the excising connection are closed
            closeExisting();

            try {
                result = AbstractChannelReplicator.openSocketChannel(closeables);
                if (!result.connect(remoteAddress)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    continue;
                }

                result.socket().setTcpNoDelay(true);
                doHandShaking(result);
                break;
            } catch (IOException e) {
                if (closeables != null) closeables.closeQuietly();
            } catch (Exception e) {
                if (closeables != null) closeables.closeQuietly();
                throw e;
            }
        }
        clientChannel = result;
        byte[] bytes = copyBufferBytes();

        long position = outBytes.position();
        int limit = outBuffer.limit();

        outBuffer.clear();
        outBytes.clear();
        //  outBytesLock.unlock();
        // assert !outBytesLock.isHeldByCurrentThread();
        try {
            checkVersion();
            loadKeyValueSerializers();
        } finally {
            outBuffer.clear();
            outBuffer.put(bytes);
            outBytes.limit(limit);
            outBytes.position(position);
            //     outBytesLock.lock();
        }

    }

    private byte[] copyBufferBytes() {
        byte[] bytes = new byte[outBuffer.limit()];
        outBuffer.position(0);
        outBuffer.get(bytes);
        return bytes;
    }

    /**
     * attempts a single connect without a timeout and eat any IOException used typically in the
     * constructor, its possible the server is not running with this instance is created, {@link
     * net.openhft.chronicle.map.StatelessChronicleMap#lazyConnect(long,
     * java.net.InetSocketAddress)} will attempt to establish the connection when the client make
     * the first map method call.
     *
     * @param remoteAddress the  Inet Socket Address
     * @throws java.io.IOException
     * @see net.openhft.chronicle.map.StatelessChronicleMap#lazyConnect(long,
     * java.net.InetSocketAddress)
     */

    private synchronized void attemptConnect(final InetSocketAddress remoteAddress) {

        // ensures that the excising connection are closed
        closeExisting();

        try {
            SocketChannel socketChannel = AbstractChannelReplicator.openSocketChannel(closeables);
            if (socketChannel.connect(remoteAddress)) {
                doHandShaking(socketChannel);
                clientChannel = socketChannel;
            }

        } catch (IOException e) {
            if (closeables != null) closeables.closeQuietly();
            clientChannel = null;
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

        long timeoutTime = System.currentTimeMillis() + timeoutMs;

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
            int read = clientChannel.read(this.connectionOutBuffer);// the remote identifier
            if (read == -1)
                throw new IOException("server conncetion closed");
            checkTimeout(timeoutTime);
        }

        byte remoteIdentifier = connectionByte[0];

        if (LOG.isDebugEnabled())
            LOG.debug("Attached to a map with a remote identifier=" + remoteIdentifier + " ,name=" + name);

    }

    @NotNull
    public File file() {
        throw new UnsupportedOperationException();
    }

    public synchronized void close() {

        if (closeables != null)
            closeables.closeQuietly();
        closeables = null;
        clientChannel = null;

    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param time in milliseconds
     * @return a unique transactionId
     */
    private long nextUniqueTransaction(long time) {
        long id = time * TcpReplicator.TIMESTAMP_FACTOR;
        for (; ; ) {
            long old = transactionID.get();
            if (old >= id) id = old + 1;
            if (transactionID.compareAndSet(old, id))
                break;
        }
        return id;
    }

    @SuppressWarnings("NullableProblems")
    public V putIfAbsent(K key, V value) {

        if (key == null || value == null)
            throw new NullPointerException();

        return fetchObject(vClass, PUT_IF_ABSENT, key, value);
    }


    @SuppressWarnings("NullableProblems")
    public boolean remove(Object key, Object value) {

        if (key == null)
            throw new NullPointerException();

        return value != null && fetchBoolean(REMOVE_WITH_VALUE, (K) key, (V) value);

    }


    @SuppressWarnings("NullableProblems")
    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();

        return fetchBoolean(REPLACE_WITH_OLD_AND_NEW_VALUE, key, oldValue, newValue);
    }

    @SuppressWarnings("NullableProblems")
    public V replace(K key, V value) {
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
    public boolean equals(@Nullable Object object) {
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

    @NotNull
    public String toString() {
        return fetchObject(String.class, TO_STRING);
    }

    @NotNull
    public String serverApplicationVersion() {
        return fetchObject(String.class, APPLICATION_VERSION);
    }

    @NotNull
    public String serverPersistedDataVersion() {
        return fetchObject(String.class, PERSISTED_DATA_VERSION);
    }

    @SuppressWarnings("WeakerAccess")
    @NotNull
    public String clientVersion() {
        return BuildVersion.version();
    }

    public boolean isEmpty() {
        return fetchBoolean(IS_EMPTY);
    }


    public boolean containsKey(Object key) {
        return fetchBooleanK(CONTAINS_KEY, (K) key);
    }

    @NotNull
    private NullPointerException keyNotNullNPE() {
        return new NullPointerException("key can not be null");
    }

    public boolean containsValue(Object value) {
        return fetchBooleanV(CONTAINS_VALUE, (V) value);
    }

    public long longSize() {
        return fetchLong(LONG_SIZE);
    }

    @Override
    public MapKeyContext<K, V> context(K key) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    @NotNull
    @Override
    public ExternalMapQueryContext<K, V, ?> queryContext(K key) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V get(Object key) {
        return fetchObject(vClass, GET, (K) key);
    }

    @Nullable
    public V getUsing(K key, V usingValue) {


        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {

            final long sizeLocation = writeEventAnSkip(GET);

            copies = writeKey((K) key);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        if (eventReturnsNull(GET))
            return null;

        return (V) readValue(transactionId, startTime, copies, usingValue);

    }

    @NotNull
    public V acquireUsing(@NotNull K key, V usingValue) {
        throw new UnsupportedOperationException("acquireUsing() is not supported for stateless " +
                "clients");
    }

    @NotNull
    @Override
    public MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
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

    @Nullable
    public <R> R getMapped(@Nullable K key, @NotNull SerializableFunction<? super V, R> function) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(MAP_FOR_KEY, key, function);
    }


    @Nullable
    @Override
    public V putMapped(@Nullable K key, @NotNull UnaryOperator<V> unaryOperator) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(PUT_MAPPED, key, unaryOperator);
    }


    private Bytes resizeBufferOutBuffer(int newCapacity) {
        return resizeBufferOutBuffer(newCapacity, outBytes.position());
    }

    private Bytes resizeBufferOutBuffer(int newCapacity, long start) {
        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        if (LOG.isDebugEnabled())
            LOG.debug("resizing buffer to newCapacity=" + newCapacity + " ,name=" + name);

        if (newCapacity < outBuffer.capacity())
            throw new IllegalStateException("it not possible to resize the buffer smaller");

        assert newCapacity < Integer.MAX_VALUE;

        final ByteBuffer result = ByteBuffer.allocate(newCapacity).order(ByteOrder.nativeOrder());
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

        assert outBuffer.capacity() == newCapacity;
        assert outBuffer.capacity() == outBytes.capacity();
        assert outBytes.limit() == outBytes.capacity();
        outBytes.position(start);
        return outBytes;
    }


    private void resizeBufferInBuffer(int newCapacity, long start) {
//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();

        if (LOG.isDebugEnabled())
            LOG.debug("InBuffer resizing buffer to newCapacity=" + newCapacity + " ,name=" + name);

        if (newCapacity < inBuffer.capacity())
            throw new IllegalStateException("it not possible to resize the buffer smaller");

        assert newCapacity < Integer.MAX_VALUE;

        final ByteBuffer result = ByteBuffer.allocate(newCapacity).order(ByteOrder.nativeOrder());
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
        assert inBuffer.capacity() == newCapacity;
        assert inBuffer.capacity() == inBytes.capacity();
        assert inBytes.limit() == inBytes.capacity();

        inBytes.position(start);
    }

    public void clear() {
        fetchVoid(CLEAR);
    }

    @NotNull
    public Collection<V> values() {

        final long timeoutTime;
        final long transactionId;

        outBytesLock.lock();
        try {

            final long sizeLocation = writeEventAnSkip(VALUES);
            final long startTime = System.currentTimeMillis();

            timeoutTime = System.currentTimeMillis() + timeoutMs;
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

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
        final long transactionId;
        final long timeoutTime;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(ENTRY_SET);
            final long startTime = System.currentTimeMillis();
            timeoutTime = System.currentTimeMillis() + timeoutMs;
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

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

    /**
     * @param callback each entry is passed to the callback
     */
    void entrySet(@NotNull MapEntryCallback<K, V> callback) {
        final long transactionId;
        final long timeoutTime;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(ENTRY_SET);
            final long startTime = System.currentTimeMillis();
            timeoutTime = System.currentTimeMillis() + timeoutMs;
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        // get the data back from the server
        ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
        final BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(copies);
        copies = valueReaderWithSize.getCopies(copies);
        final BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(copies);

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
                    callback.onEntry(k, v);
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                inBytesLock.unlock();
            }
        }


    }

    public void putAll(@NotNull Map<? extends K, ? extends V> map) {

        final long sizeLocation;

        outBytesLock.lock();
        try {
            sizeLocation = putReturnsNull ? writeEventAnSkip(PUT_ALL_WITHOUT_ACC) :
                    writeEventAnSkip(PUT_ALL);
        } finally {
            outBytesLock.unlock();
        }
        final long startTime = System.currentTimeMillis();
        final long timeoutTime = startTime + timeoutMs;
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

            }

            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }


        if (!putReturnsNull) {
            inBytesLock.lock();
            try {
                blockingFetchReadOnly(timeoutTime, transactionId);
            } finally {
                inBytesLock.unlock();
            }
        }
    }

    @NotNull
    public Set<K> keySet() {
        final long transactionId;
        final long timeoutTime;

        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(KEY_SET);
            final long startTime = System.currentTimeMillis();
            timeoutTime = startTime + timeoutMs;
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

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

        assert !outBytesLock.isHeldByCurrentThread();

        final long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock.lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readLong();
        } finally {
            inBytesLock.unlock();
        }
    }


    private long writeEvent(@NotNull StatelessChronicleMap.EventId event) {
        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();
        assert event != HEARTBEAT;

        if (outBytes.remaining() < 128)
            resizeBufferOutBuffer((int) outBytes.capacity() + 128, outBytes.position());

        outBytes.writeByte((byte) event.ordinal());


        return markSizeLocation();
    }

    /**
     * skips for the transactionid
     */
    private long writeEventAnSkip(@NotNull EventId event) {

        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        final long sizeLocation = writeEvent(event);
        assert outBytes.readByte(sizeLocation - 1) == event.ordinal();

        assert outBytes.position() > 0;

        // skips for the transaction id
        outBytes.skip(SIZE_OF_TRANSACTION_ID);
        outBytes.writeByte(identifier);
        outBytes.writeInt(0);
        assert outBytes.readByte(sizeLocation - 1) == event.ordinal();
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
            StatelessChronicleMap.this.put(getKey(), newValue);
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

        @NotNull
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

        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        long transactionId = nextUniqueTransaction(startTime);
        final long timeoutTime = startTime + this.timeoutMs;
        try {

            for (; ; ) {
                if (clientChannel == null) {
                    lazyConnect(timeoutMs, remoteAddress);
                }
                try {

                    if (LOG.isDebugEnabled())
                        LOG.debug("sending data with transactionId=" + transactionId + " ,name=" + name);

                    writeSizeAndTransactionIdAt(sizeLocation, transactionId);

                    // send out all the bytes
                    writeBytesToSocket(timeoutTime);

                    break;

                } catch (@NotNull java.nio.channels.ClosedChannelException /*| ClosedConnectionException*/ e) {
                    checkTimeout(timeoutTime);
                    lazyConnect(timeoutMs, remoteAddress);
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


        assert inBytesLock.isHeldByCurrentThread();
        //  assert !outBytesLock.isHeldByCurrentThread();
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
            LOG.error("name=" + name, e);
            throw e;
        }
    }

    // this is a transaction id and size that has been read by another thread.
    private volatile long parkedTransactionId;
    private volatile int parkedRemainingBytes;
    private volatile long parkedTransactionTimeStamp;

    private Bytes blockingFetchThrowable(long timeoutTime, long transactionId) throws IOException,
            InterruptedException {

//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();

        int remainingBytes = nextEntry(timeoutTime, transactionId);

        if (inBytes.capacity() < remainingBytes) {
            long pos = inBytes.position();
            long limit = inBytes.position();
            inBytes.position(limit);
            resizeBufferInBuffer(remainingBytes, pos);
        } else
            inBytes.limit(inBytes.capacity());

        // block until we have received all the bytes in this chunk
        receiveBytesFromSocket(remainingBytes, timeoutTime);

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
                InetSocketAddress address = remoteAddress;
                stes.add(new StackTraceElement("~ remote", "tcp ~", address.getHostName(), address.getPort()));
                StackTraceElement[] stackTrace2 = Thread.currentThread().getStackTrace();
                //noinspection ManualArrayToCollectionCopy
                for (int i = 4; i < stackTrace2.length; i++)
                    stes.add(stackTrace2[i]);
                stackTrace.set(throwable, stes.toArray(new StackTraceElement[stes.size()]));
            } catch (Exception ignore) {
            }
            NativeBytes.UNSAFE.throwException(throwable);
        }


        return inBytes;
    }

    private int nextEntry(long timeoutTime, long transactionId) throws IOException {
//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();

        int remainingBytes;
        for (; ; ) {

            // read the next item from the socket
            if (parkedTransactionId == 0) {

                assert parkedTransactionTimeStamp == 0;
                assert parkedRemainingBytes == 0;

                receiveBytesFromSocket(SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID, timeoutTime);

                final int messageSize = inBytes.readInt();
                assert messageSize > 0 : "Invalid message size " + messageSize;
                assert messageSize < 16 << 20 : "Invalid message size " + messageSize;

                final int remainingBytes0 = messageSize - (SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID);
                final long transactionId0 = inBytes.readLong();

                // check the transaction id is reasonable
                assert transactionId0 > 1410000000000L * TcpReplicator.TIMESTAMP_FACTOR :
                        "TransactionId too small " + transactionId0 + " messageSize " + messageSize;
                assert transactionId0 < 2100000000000L * TcpReplicator.TIMESTAMP_FACTOR :
                        "TransactionId too large " + transactionId0 + " messageSize " + messageSize;

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

                LOG.error("name=" + name, new IllegalStateException("Skipped Message with " +
                        "transaction-id=" +
                        parkedTransactionTimeStamp +
                        ", this can occur when you have another thread which has called the " +
                        "stateless client and terminated abruptly before the message has been " +
                        "returned from the server"));

                // read the the next message
                receiveBytesFromSocket(parkedRemainingBytes, timeoutTime);
                clearParked();

            }

            pause();
        }
        return remainingBytes;
    }

    private void clearParked() {
//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();
        parkedTransactionId = 0;
        parkedRemainingBytes = 0;
        parkedTransactionTimeStamp = 0;
    }

    private void pause() {

        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();

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
    @SuppressWarnings("UnusedReturnValue")
    private Bytes receiveBytesFromSocket(int requiredNumberOfBytes, long timeoutTime) throws IOException {

//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();

        inBytes.position(0);
        inBytes.limit(requiredNumberOfBytes);
        inBytes.buffer().position(0);
        inBytes.buffer().limit(requiredNumberOfBytes);

        while (inBytes.buffer().remaining() > 0) {
            assert requiredNumberOfBytes <= inBytes.capacity();

            int len = clientChannel.read(inBytes.buffer());

            if (len == -1)
                throw new IORuntimeException("Disconnection to server");

            checkTimeout(timeoutTime);
        }

        inBytes.position(0);
        inBytes.limit(requiredNumberOfBytes);
        return inBytes;
    }


    long largestEntrySoFar = 0;
    private long limitOfLast = 0;

    private void writeBytesToSocket(long timeoutTime) throws IOException {

        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        // if we have other threads waiting to send and the buffer is not full, let the other threads
        // write to the buffer
        if (outBytesLock.hasQueuedThreads() &&
                outBytes.position() + largestEntrySoFar <= tcpBufferSize) {
            return;
        }

        outBuffer.limit((int) outBytes.position());
        outBuffer.position(0);

        int sizeOfThisMessage = (int) (outBuffer.limit() - limitOfLast);
        if (largestEntrySoFar < sizeOfThisMessage)
            largestEntrySoFar = sizeOfThisMessage;

        limitOfLast = outBuffer.limit();


        while (outBuffer.remaining() > 0) {

            int len = clientChannel.write(outBuffer);
            if (len == -1)
                throw new IORuntimeException("Disconnection to server");


            // if we have queued threads then we don't have to write all the bytes as the other
            // threads will write the remains bytes.
            if (outBuffer.remaining() > 0 && outBytesLock.hasQueuedThreads() &&
                    outBuffer.remaining() + largestEntrySoFar <= tcpBufferSize) {

                LOG.debug("continuing -  without all the data being written to the buffer as " +
                        "it will be written by the next thread");
                outBuffer.compact();
                outBytes.limit(outBuffer.limit());
                outBytes.position(outBuffer.position());
                return;
            }

            checkTimeout(timeoutTime);

        }

        outBuffer.clear();
        outBytes.clear();

    }


    private void writeSizeAndTransactionIdAt(long locationOfSize, final long transactionId) {

        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        assert outBytes.readByte(locationOfSize - 1) >= LONG_SIZE.ordinal();


        final long size = outBytes.position() - locationOfSize;
        final long pos = outBytes.position();
        outBytes.position(locationOfSize);
        try {
            outBuffer.position((int) locationOfSize);

        } catch (IllegalArgumentException e) {
            LOG.error("locationOfSize=" + locationOfSize + ", limit=" + outBuffer.limit(), e);
        }
        int size0 = (int) size - SIZE_OF_SIZE;

        outBytes.writeInt(size0);
        assert transactionId != 0;
        outBytes.writeLong(transactionId);

        outBytes.position(pos);
    }

    private long markSizeLocation() {
        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        final long position = outBytes.position();
        outBytes.writeInt(0); // skip the size
        return position;
    }

    private ThreadLocalCopies writeKey(K key) {
        return writeKey(key, null);
    }

    @SuppressWarnings("SameParameterValue")
    private ThreadLocalCopies writeKey(K key, ThreadLocalCopies copies) {
        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();
        return keyWriterWithSize.write(outBytes, key, copies);
    }

    @SuppressWarnings("UnusedReturnValue")
    private ThreadLocalCopies writeKeyInLoop(K key, Object writer, ThreadLocalCopies copies) {

        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();
        return keyWriterWithSize.writeInLoop(outBytes, key, writer, copies);

    }

    private ThreadLocalCopies writeValue(V value, ThreadLocalCopies copies) {
        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        long start = outBytes.position();

        assert outBytes.position() == start;
        outBytes.limit(outBytes.capacity());
        return valueWriterWithSize.write(outBytes, value, copies);

    }

    @SuppressWarnings("UnusedReturnValue")
    private ThreadLocalCopies writeValueInLoop(V value, Object writer, ThreadLocalCopies copies) {
        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();
        long start = outBytes.position();


        assert outBytes.position() == start;
        outBytes.limit(outBytes.capacity());
        return valueWriterWithSize.writeInLoop(outBytes, value, writer, copies);

    }

    private V readValue(long transactionId, long startTime, ThreadLocalCopies copies, V usingValue) {
        assert !outBytesLock.isHeldByCurrentThread();

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock.lock();
        try {

            if (usingValue != null)
                return readValue(copies, blockingFetchReadOnly(timeoutTime, transactionId), usingValue);
            else

                return readValue(copies, blockingFetchReadOnly(timeoutTime, transactionId));
        } finally {
            inBytesLock.unlock();
        }
    }

    @Nullable
    private <O> O readObject(long transactionId, long startTime) {
        assert !outBytesLock.isHeldByCurrentThread();

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock.lock();
        try {
            return (O) blockingFetchReadOnly(timeoutTime, transactionId).readObject();
        } finally {
            inBytesLock.unlock();
        }
    }

    private boolean readBoolean(long transactionId, long startTime) {
        assert !outBytesLock.isHeldByCurrentThread();

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock.lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readBoolean();
        } finally {
            inBytesLock.unlock();
        }
    }


    private int readInt(long transactionId, long startTime) {
        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();
        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock.lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readInt();
        } finally {
            inBytesLock.unlock();
        }
    }

    private long send(@NotNull final EventId eventId, final long startTime) {

        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        assert eventId.ordinal() != 0;
        // send
        outBytesLock.lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            assert sizeLocation == 1;
            return send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }
    }

    private V readValue(ThreadLocalCopies copies, Bytes in) {
        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();
        return valueReaderWithSize.readNullable(in, copies, null);
    }

    private V readValue(ThreadLocalCopies copies, Bytes in, V using) {
        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock.isHeldByCurrentThread();
        return valueReaderWithSize.readNullable(in, copies, using);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBoolean(@NotNull final EventId eventId, K key, V value) {
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

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBoolean(@NotNull final EventId eventId, K key, V value1, V value2) {
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

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBooleanV(@NotNull final EventId eventId, V value) {
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


    @SuppressWarnings("SameParameterValue")
    private boolean fetchBooleanK(@NotNull final EventId eventId, K key) {
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


    @SuppressWarnings("SameParameterValue")
    private long fetchLong(@NotNull final EventId eventId) {
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

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBoolean(@NotNull final EventId eventId) {
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

    @SuppressWarnings("SameParameterValue")
    private void fetchVoid(@NotNull final EventId eventId) {

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
        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock.lock();
        try {
            blockingFetchReadOnly(timeoutTime, transactionId);
        } finally {
            inBytesLock.unlock();
        }
    }


    @SuppressWarnings("SameParameterValue")
    @Nullable
    private <O> O fetchObject(final Class<O> tClass, @NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();
        long transactionId;

        outBytesLock.lock();
        try {
            transactionId = send(eventId, startTime);
        } finally {
            outBytesLock.unlock();
        }

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock.lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readObject(tClass);
        } finally {
            inBytesLock.unlock();
        }
    }


    @SuppressWarnings("SameParameterValue")
    private int fetchInt(@NotNull final EventId eventId) {
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


    @Nullable
    private <R> R fetchObject(Class<R> rClass, @NotNull final EventId eventId, K key, V value) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {

            final long sizeLocation = writeEventAnSkip(eventId);

            copies = writeKey(key);
            copies = writeValue(value, copies);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }


        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, copies, null);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }


    @Nullable
    private <R> R fetchObject(Class<R> rClass, @NotNull final EventId eventId, K key) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        outBytesLock.lock();
        try {

            final long sizeLocation = writeEventAnSkip(eventId);

            copies = writeKey(key);
            transactionId = send(sizeLocation, startTime);
        } finally {
            outBytesLock.unlock();
        }

        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, copies, null);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }


    private boolean eventReturnsNull(@NotNull EventId eventId) {

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

        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();

        long start = outBytes.position();
        for (; ; ) {
            try {
                outBytes.writeObject(function);
                return;
            } catch (IllegalStateException e) {
                Throwable cause = e.getCause();

                if (cause instanceof IOException && cause.getMessage().contains("Not enough available space")) {
                    LOG.debug("resizing buffer, name=" + name);

                    try {
                        resizeToMessageOutBuffer(start, e);
                    } catch (Exception e2) {
                        throw e;
                    }

                } else
                    throw e;
            }
        }
    }

    private void resizeToMessageOutBuffer(long start, @NotNull Exception e) throws Exception {
        assert outBytesLock.isHeldByCurrentThread();
        assert !inBytesLock.isHeldByCurrentThread();
        String message = e.getMessage();
        if (message.startsWith("java.io.IOException: Not enough available space for writing ")) {
            String substring = message.substring("java.io.IOException: Not enough available space for writing ".length(), message.length());
            int i = substring.indexOf(' ');
            if (i != -1) {
                int size = Integer.parseInt(substring.substring(0, i));

                long requiresExtra = size - outBytes.remaining();
                resizeBufferOutBuffer((int) (outBytes.capacity() + requiresExtra), start);
            } else
                throw e;
        } else
            throw e;
    }


    @Nullable
    private <R> R fetchObject(@NotNull final EventId eventId, K key, @NotNull Object object) {
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

