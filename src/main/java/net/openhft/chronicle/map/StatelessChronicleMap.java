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
import net.openhft.chronicle.common.StatelessBuilder;
import net.openhft.chronicle.common.exceptions.IORuntimeException;
import net.openhft.chronicle.common.exceptions.TimeoutRuntimeException;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.*;

import static java.nio.ByteBuffer.allocateDirect;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_SIZE;
import static net.openhft.chronicle.map.StatelessChronicleMap.EventId.*;

/**
 * @author Rob Austin.
 */
class StatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);
    public static final byte STATELESS_CLIENT_IDENTIFIER = (byte) -127;

    private final byte[] connectionByte = new byte[1];
    private final ByteBuffer connectionOutBuffer = ByteBuffer.wrap(connectionByte);

    private ByteBuffer buffer;
    private ByteBufferBytes bytes;

    private final KeyValueSerializer<K, V> keyValueSerializer;
    private volatile SocketChannel clientChannel;

    private CloseablesManager closeables;
    private final StatelessBuilder builder;
    private int maxEntrySize;
    private final Class<K> kClass;
    private final Class<V> vClass;
    private int headroom = 1024;

    static enum EventId {
        HEARTBEAT,
        STATEFUL_UPDATE,
        LONG_SIZE, SIZE,
        IS_EMPTY,
        CONTAINS_KEY,
        CONTAINS_VALUE,
        GET, PUT,
        REMOVE,
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
        EQUALS,
        HASH_CODE
    }

    private long transactionID;

    StatelessChronicleMap(final KeyValueSerializer<K, V> keyValueSerializer,
                          final StatelessBuilder builder,
                          int maxEntrySize, Class<K> kClass, Class<V> vClass) throws IOException {
        this.keyValueSerializer = keyValueSerializer;
        this.builder = builder;
        this.maxEntrySize = maxEntrySize;
        this.kClass = kClass;
        this.vClass = vClass;
        attemptConnect(builder.remoteAddress());
        //  headroom = 1024 + maxEntrySize;
        buffer = allocateDirect(maxEntrySize).order(ByteOrder.nativeOrder());

        bytes = new ByteBufferBytes(buffer.slice());

    }


    private SocketChannel lazyConnect(final long timeoutMs,
                                      final InetSocketAddress remoteAddress) throws IOException {

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress);

        SocketChannel result = null;

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        for (; ; ) {

            checkTimeout(timeoutAt);

            // ensures that the excising connection are closed
            closeExisting();

            try {
                result = AbstractChannelReplicator.openSocketChannel(closeables);
                result.connect(builder.remoteAddress());
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
     * StatelessChronicleMap#lazyConnect(long, java.net.InetSocketAddress)} will attempt to
     * establish the connection when the client make the first map method call.
     *
     * @param remoteAddress
     * @return
     * @throws IOException
     * @see StatelessChronicleMap#lazyConnect(long, java.net.InetSocketAddress)
     */
    private void attemptConnect(final InetSocketAddress remoteAddress) throws IOException {

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
     * @param clientChannel
     * @throws IOException
     */
    private void doHandShaking(@NotNull final SocketChannel clientChannel) throws IOException {

        connectionByte[0] = STATELESS_CLIENT_IDENTIFIER;
        this.connectionOutBuffer.clear();

        long timeoutTime = System.currentTimeMillis() + builder.timeoutMs();

        // write a single byte
        while (connectionOutBuffer.hasRemaining()) {
            clientChannel.write(connectionOutBuffer);
            checkTimeout(timeoutTime);
        }

        this.connectionOutBuffer.clear();

        // read a single  byte back
        while (this.connectionOutBuffer.position() <= 0) {
            clientChannel.read(this.connectionOutBuffer);
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
    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param time in milliseconds
     * @return a unique transactionId
     */
    private long nextUniqueTransaction(long time) {
        transactionID = (time == transactionID) ? time + 1 : time;
        return transactionID;
    }


    public synchronized V putIfAbsent(K key, V value) {
        long sizeLocation = writeEvent(PUT_IF_ABSENT);
        writeKey(key);
        writeValue(value);
        return readKey(sizeLocation);
    }


    public synchronized boolean remove(Object key, Object value) {
        long sizeLocation = writeEvent(REMOVE_WITH_VALUE);
        writeKey((K) key);
        writeValue((V) value);

        // get the data back from the server
        return blockingFetch(sizeLocation).readBoolean();
    }


    public synchronized boolean replace(K key, V oldValue, V newValue) {
        long sizeLocation = writeEvent(REPLACE_WITH_OLD_AND_NEW_VALUE);
        writeKey(key);
        writeValue(oldValue);
        writeValue(newValue);

        // get the data back from the server
        return blockingFetch(sizeLocation).readBoolean();
    }


    public synchronized V replace(K key, V value) {
        long sizeLocation = writeEvent(REPLACE);
        writeKey(key);
        writeValue(value);

        // get the data back from the server
        return readKey(sizeLocation);
    }


    public synchronized int size() {
        long sizeLocation = writeEvent(SIZE);

        // get the data back from the server
        return blockingFetch(sizeLocation).readInt();
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

        long sizeLocation = writeEvent(EQUALS);

        try {
            writeEntries(that);
        } catch (ClassCastException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("failed equals() due to a difference in types", e);
            return false;
        }

        // get the data back from the server
        return blockingFetch(sizeLocation).readBoolean();
    }


    public synchronized void putAll(Map<? extends K, ? extends V> map) {

        final long sizeLocation = writeEvent(PUT_ALL);
        writeEntries(map);

        blockingFetch(sizeLocation);

    }


    @Override
    public int hashCode() {
        long sizeLocation = writeEvent(HASH_CODE);

        // get the data back from the server
        return blockingFetch(sizeLocation).readInt();
    }

    public synchronized String toString() {
        long sizeLocation = writeEvent(TO_STRING);

        // get the data back from the server
        return blockingFetch(sizeLocation).readObject(String.class);

    }


    public synchronized boolean isEmpty() {
        long sizeLocation = writeEvent(IS_EMPTY);

        // get the data back from the server
        return blockingFetch(sizeLocation).readBoolean();
    }


    public synchronized boolean containsKey(Object key) {
        long sizeLocation = writeEvent(CONTAINS_KEY);
        writeKey((K) key);

        // get the data back from the server
        return blockingFetch(sizeLocation).readBoolean();

    }


    public synchronized boolean containsValue(Object value) {
        long sizeLocation = writeEvent(CONTAINS_VALUE);
        writeValue((V) value);

        // get the data back from the server
        return blockingFetch(sizeLocation).readBoolean();
    }


    public synchronized long longSize() {
        long sizeLocation = writeEvent(LONG_SIZE);
        // get the data back from the server
        return blockingFetch(sizeLocation).readLong();
    }


    public synchronized V get(Object key) {
        long sizeLocation = writeEvent(GET);
        writeKey((K) key);

        // get the data back from the server
        return readKey(sizeLocation);

    }

    public synchronized V getUsing(K key, V usingValue) {
        throw new UnsupportedOperationException("getUsing is not supported for stateless clients");
    }


    public synchronized V acquireUsing(K key, V usingValue) {
        throw new UnsupportedOperationException("getUsing is not supported for stateless clients");
    }

    public synchronized V put(K key, V value) {

        long sizeLocation = writeEvent(PUT);
        writeKey(key);
        writeValue(value);

        // get the data back from the server
        return readKey(sizeLocation);
    }

    public synchronized V remove(Object key) {
        long sizeLocation = writeEvent(REMOVE);
        writeKey((K) key);

        // get the data back from the server
        return readKey(sizeLocation);
    }


    private void writeEntries(Map<? extends K, ? extends V> map) {

        final int numberOfEntries = map.size();
        int numberOfEntriesReadSoFar = 0;

        bytes.writeStopBit(numberOfEntries);
        assert bytes.limit() == bytes.capacity();
        for (final Map.Entry<? extends K, ? extends V> e : map.entrySet()) {

            numberOfEntriesReadSoFar++;
            assert bytes.limit() == bytes.capacity();
            // putAll if a bit more complicated than the others
            // as the volume of data could cause us to have to resize our buffers
            resizeIfRequired(numberOfEntries, numberOfEntriesReadSoFar);

            assert bytes.limit() == bytes.capacity();

            final long start = bytes.position();
            final K key = e.getKey();

            final Class<?> keyClass = key.getClass();
            if (!kClass.isAssignableFrom(keyClass))
                throw new ClassCastException("key=" + key + " is of type=" + keyClass + " " +
                        "and should" +
                        " be of type=" + kClass);

            writeKey(key);
            assert bytes.limit() == bytes.capacity();
            final V value = e.getValue();

            final Class<?> valueClass = value.getClass();
            if (!vClass.isAssignableFrom(valueClass))
                throw new ClassCastException("value=" + value + " is of type=" + valueClass +
                        " and " +
                        "should  be of type=" + vClass);


            try {
                assert bytes.limit() == bytes.capacity();
                writeValue(value);


            } catch (
                    Exception e4) {
                LOG.info("bytes position=" + bytes.position() + ",capacity=" + bytes.capacity() +
                        ", " +
                        "limit=" + bytes.limit());
                assert bytes.limit() == bytes.capacity();

                writeValue(value);
                int i = 0;
                throw e4;
            }


            final int len = (int) (bytes.position() - start);
            assert bytes.limit() == bytes.capacity();
            if (len > maxEntrySize)
                maxEntrySize = len;
        }
    }

    private void resizeIfRequired(int numberOfEntries, int numberOfEntriesReadSoFar) {

        //  System.out.println("bytes=" + bytes.position());

        long remaining = bytes.remaining();


        if (remaining < maxEntrySize) {

            // todo check this again as I  think it may have a bug
            long estimatedRequiredSize = estimateSize(numberOfEntries, numberOfEntriesReadSoFar);
            resizeBuffer(estimatedRequiredSize + maxEntrySize * 128);
        }
    }

    /**
     * estimates the size based on what been completed so far
     *
     * @param numberOfEntries
     * @param numberOfEntriesReadSoFar
     */
    private long estimateSize(int numberOfEntries, int numberOfEntriesReadSoFar) {
        // we will back the size estimate on what we have so far
        double percentageComplete = (double) numberOfEntriesReadSoFar / (double) numberOfEntries;
        return (long) ((double) bytes.position() / percentageComplete);

        // LOG.info("bytes position=" + bytes.position() + ",capacity=" + bytes.capacity() + ", " +
        //"limit=" + bytes.limit());
        // return bytes.position() + (maxEntrySize * 128);
    }


    void resizeBuffer(long size) {
        if (size < buffer.capacity())
            throw new IllegalStateException("it not possible to resize the buffer smaller");

        assert size < Integer.MAX_VALUE;


        LOG.info("resizing to=" + size);

        LOG.info("bytes before position=" + bytes.position() +
                ", " +
                "limit=" + bytes.limit() + ",capacity=" + bytes.capacity());

        LOG.info("buffer before");

        LOG.info("resize buffer to " + size + " bytes");
        final ByteBuffer result = ByteBuffer.allocateDirect((int) size).order(ByteOrder.nativeOrder());

        int bufferPosition = buffer.position();
        int bufferLimit = buffer.limit();

        long bytesPosition = bytes.position();
        bytes = new ByteBufferBytes(result.slice());

        buffer.position(0);
        buffer.limit((int) bytesPosition);
        LOG.info("bytesPosition to=" + bytesPosition + ", buffer.position=" + buffer.position() +
                ", " +
                "limit=" + buffer.limit() + ",capacity=" + buffer.capacity());
        for (int i = 0; i < bytesPosition; i++) {
            result.put(buffer.get());
        }
        buffer = result;

        buffer.limit(bufferLimit);
        buffer.position(bufferPosition);

        assert buffer.capacity() == bytes.capacity();

        bytes.limit(bytes.capacity());
        bytes.position(bytesPosition);

        assert buffer.capacity() == size;
        assert buffer.capacity() == bytes.capacity();
        assert bytes.limit() == bytes.capacity();


    }


    public synchronized void clear() {
        long sizeLocation = writeEvent(CLEAR);

        // get the data back from the server
        blockingFetch(sizeLocation);

    }


    @NotNull
    public synchronized Collection<V> values() {
        long sizeLocation = writeEvent(VALUES);

        // get the data back from the server
        final Bytes in = blockingFetch(sizeLocation);

        final long size = in.readStopBit();

        if (size > Integer.MAX_VALUE)
            throw new IllegalStateException("size=" + size + " is too large.");

        final ArrayList<V> result = new ArrayList<V>((int) size);

        for (int i = 0; i < size; i++) {
            result.add(keyValueSerializer.readValue(in));
        }
        return result;
    }


    @NotNull
    public synchronized Set<Map.Entry<K, V>> entrySet() {
        final long sizeLocation = writeEvent(ENTRY_SET);

        final long startTime = System.currentTimeMillis();
        final long transactionId = nextUniqueTransaction(startTime);
        final long timeoutTime = System.currentTimeMillis() + builder.timeoutMs();

        // get the data back from the server
        Bytes in = blockingFetch0(sizeLocation, transactionId, startTime);

        final Set<Map.Entry<K, V>> result = new HashSet<Map.Entry<K, V>>();

        for (; ; ) {

            boolean hasMoreEntries = in.readBoolean();

            // number of entries in the chunk
            long size = in.readInt();

            for (int i = 0; i < size; i++) {

                K k = keyValueSerializer.readKey(in);
                V v = keyValueSerializer.readValue(in);
                result.add(new Entry(k, v));
            }

            if (!hasMoreEntries)
                break;

            compact(in);

            in = blockingFetchReadOnly(timeoutTime, transactionId);
        }


        return result;
    }

    private void compact(Bytes in) {
        if (in.remaining() == 0) {
            bytes.clear();
            bytes.buffer().clear();
        } else {
            buffer.compact();
        }
    }


    @NotNull
    public synchronized Set<K> keySet() {
        final long sizeLocation = writeEvent(KEY_SET);

        final long startTime = System.currentTimeMillis();
        final long transactionId = nextUniqueTransaction(startTime);
        final long timeoutTime = System.currentTimeMillis() + builder.timeoutMs();

        // get the data back from the server
        Bytes in = blockingFetch0(sizeLocation, transactionId, startTime);

        final Set<K> result = new HashSet<>();

        for (; ; ) {

            boolean hasMoreEntries = in.readBoolean();
            LOG.info("entrySet - hasMoreEntries=" + hasMoreEntries);

            // number of entries in the chunk
            long size = in.readInt();

            for (int i = 0; i < size; i++) {
                K k = keyValueSerializer.readKey(in);
                result.add(k);
            }

            if (!hasMoreEntries)
                break;

            compact(in);

            in = blockingFetchReadOnly(timeoutTime, transactionId);
        }

        return result;
    }


    private long writeEvent(EventId event) {

        buffer.clear();
        bytes.clear();

        //bytes.limit(buffer.capacity());

        bytes.write((byte) event.ordinal());
        long sizeLocation = markSizeLocation();
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
            Map.Entry e = (Map.Entry) o;
            Object k1 = getKey();
            Object k2 = e.getKey();
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

    private Bytes blockingFetch(long sizeLocation) {

        try {
            long startTime = System.currentTimeMillis();
            return blockingFetchThrowable(sizeLocation, this.builder.timeoutMs(),
                    nextUniqueTransaction(startTime), startTime);
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }

    }


    private Bytes blockingFetch0(long sizeLocation, final long transactionId, long startTime) {

        try {
            return blockingFetchThrowable(sizeLocation, this.builder.timeoutMs(), transactionId, startTime);
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }

    }


    private Bytes blockingFetchThrowable(long sizeLocation, long timeOutMs, final long transactionId, final long startTime) throws IOException {

        long timeoutTime = startTime + timeOutMs;

        for (; ; ) {

            if (clientChannel == null)
                clientChannel = lazyConnect(builder.timeoutMs(), builder.remoteAddress());

            try {

                if (LOG.isDebugEnabled())
                    LOG.debug("sending data with transactionId=" + transactionId);

                write(transactionId);
                writeSizeAt(sizeLocation);

                // send out all the bytes
                send(bytes, timeoutTime);

                bytes.clear();
                bytes.buffer().clear();

                return blockingFetch(timeoutTime, transactionId);

            } catch (java.nio.channels.ClosedChannelException | ClosedConnectionException e) {
                checkTimeout(timeoutTime);
                clientChannel = lazyConnect(builder.timeoutMs(), builder.remoteAddress());
            }
        }
    }

    private Bytes blockingFetchReadOnly(long timeoutTime, final long transactionId) {

        try {
            return blockingFetch(timeoutTime, transactionId);
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }

    }


    private Bytes blockingFetch(long timeoutTime, long transactionId) throws IOException {


        // the number of bytes in the response
        int size = receive(SIZE_OF_SIZE, timeoutTime).readInt();

        int requiredSize = size + SIZE_OF_SIZE;
        if (bytes.capacity() < requiredSize) {
            bytes = new ByteBufferBytes(allocateDirect(requiredSize));
        }

        // block until we have received all the byte in this chunk
        receive(size, timeoutTime);

        boolean isException = bytes.readBoolean();
        long inTransactionId = bytes.readLong();

        if (inTransactionId != transactionId)
            throw new IllegalStateException("the received transaction-id=" + inTransactionId +
                    ", does not match the expected transaction-id=" + transactionId);

        if (isException)
            throw (RuntimeException) bytes.readObject();

        return bytes;
    }

    private void write(long transactionId) {
        bytes.writeLong(transactionId);
    }

    private ByteBufferBytes receive(int requiredNumberOfBytes, long timeoutTime) throws IOException {

        while (bytes.buffer().position() < requiredNumberOfBytes) {
            clientChannel.read(bytes.buffer());
            checkTimeout(timeoutTime);
        }

        bytes.limit(bytes.buffer().position());
        return bytes;
    }

    private void send(final ByteBufferBytes out, long timeoutTime) throws IOException {

        buffer.limit((int) out.position());
        buffer.position(0);

        while (buffer.remaining() > 0) {
            clientChannel.write(buffer);
            checkTimeout(timeoutTime);
        }

        out.clear();
        buffer.clear();
    }


    private void checkTimeout(long timeoutTime) {
        if (timeoutTime < System.currentTimeMillis())
            throw new TimeoutRuntimeException();
    }

    private void writeSizeAt(long locationOfSize) {
        long size = bytes.position() - locationOfSize;
//        LOG.info("writing size = " + size);
        bytes.writeInt(locationOfSize, (int) size - SIZE_OF_SIZE);
    }

    private long markSizeLocation() {
        long position = bytes.position();
        bytes.skip(SIZE_OF_SIZE);
        return position;
    }

    private void writeKey(K key) {
        keyValueSerializer.writeKey(key, bytes);
    }

    private V readKey(final long sizeLocation) {
        return keyValueSerializer.readValue(blockingFetch(sizeLocation));
    }

    private void writeValue(V value) {
        //  LOG.info("bytes position=" + bytes.position() + ",capacity=" + bytes.capacity() + ", " +
        //             "limit=" + bytes.limit());
        keyValueSerializer.writeValue(value, bytes);
    }

    private Set<K> readKeySet(Bytes in) {
        long size = in.readStopBit();
        final HashSet<K> result = new HashSet<>();

        for (long i = 0; i < size; i++) {
            result.add(keyValueSerializer.readKey(in));
        }
        return result;
    }

}

