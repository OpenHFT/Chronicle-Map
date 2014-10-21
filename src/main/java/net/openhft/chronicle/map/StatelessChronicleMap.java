package net.openhft.chronicle.map;

import net.openhft.chronicle.common.ClosableHolder;
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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.map.StatelessChronicleMap.EventId.*;

/**
 * @author Rob Austin.
 */
class StatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Closeable, ClosableHolder {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);

    private final SocketChannel clientChannel;
    public static final byte STATELESS_CLIENT_IDENTIFER = (byte) -127;
    private CloseablesManager closeables;
    private final StatelessBuilder builder;

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
        TO_STRING
    }

    private final AtomicLong transactionID = new AtomicLong();

    StatelessChronicleMap(final KeyValueSerializer<K, V> keyValueSerializer,
                          final StatelessBuilder builder) throws IOException {

        this.keyValueSerializer = keyValueSerializer;
        this.builder = builder;
        this.clientChannel = connect(builder);
    }

    private SocketChannel connect(StatelessBuilder builder) throws IOException {
        IOException exception = null;
        SocketChannel clientChannel0 = null;

        long timeoutAt = System.currentTimeMillis() + builder.timeoutMs();
        for (; ; ) {

            if (System.currentTimeMillis() > timeoutAt) {
                clientChannel0.close();
                close();
                throw new RuntimeException("unable to connect to remote server");
            }

            closeables = new CloseablesManager();
            clientChannel0 = AbstractChannelReplicator.openSocketChannel(closeables);

            try {
                clientChannel0.connect(builder.remoteAddress());
                doHandShaking(clientChannel0);
                exception = null;
                break;
            } catch (IOException e) {

                clientChannel0.close();
                close();

            }

        }

        if (exception != null)
            throw exception;

        if (clientChannel0 == null)
            throw new IOException("unable to connect to remote server");

        return clientChannel0;
    }


    private void doHandShaking(final SocketChannel clientChannel) throws IOException {

        out.clear();
        out.writeByte(STATELESS_CLIENT_IDENTIFER);


        ByteBuffer buffer = out.buffer().slice();
        buffer.position(0);
        buffer.limit(1);
        long timeoutTime = System.currentTimeMillis() + builder.timeoutMs();
        while (buffer.hasRemaining()) {
            clientChannel.write(buffer);
            checkTimeout(timeoutTime);
        }

        in.buffer().clear();
        in.clear();
        out.buffer().clear();
        out.clear();

        while (in.buffer().position() <= 0) {
            clientChannel.read(in.buffer());
            checkTimeout(timeoutTime);
        }

        in.limit(in.buffer().position());

        byte remoteIdentifier = in.readByte();

        LOG.info("Attached to a map with a remote identifier=" + remoteIdentifier);

    }

    public File file() {
        return null;
    }

    public void close() {
        closeables.closeQuietly();
        clearBuffers();
    }


    public void addCloseable(final Closeable closeable) {
        try {
            closeables.add(closeable);
        } catch (IllegalStateException e) {
            // already closed
        }
    }


    long nextUniqueTransaction(long time) {

        long l = transactionID.get();
        if (time > l) {
            boolean b = transactionID.compareAndSet(l, time);
            if (b) return time;
        }

        return transactionID.incrementAndGet();
    }

    private final ByteBufferBytes out = new ByteBufferBytes(ByteBuffer.allocate(1024));
    private final ByteBufferBytes in = new ByteBufferBytes(ByteBuffer.allocate(1024));
    private final KeyValueSerializer<K, V> keyValueSerializer;

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


    public synchronized V getUsing(K key, V value) {
        throw new UnsupportedOperationException("getUsing is not supported for stateless clients");
    }


    public synchronized V acquireUsing(K key, V value) {
        throw new UnsupportedOperationException("getUsing is not supported for stateless clients");
    }


    public synchronized V put(K key, V value) {

        long sizeLocation = writeEvent(PUT);
        writeKey(key);
        writeValue(value);

        // get the data back from the server

        return readKey(sizeLocation);


    }

    private void compact() {
        in.buffer().compact();

    }


    public synchronized V remove(Object key) {
        long sizeLocation = writeEvent(REMOVE);
        writeKey((K) key);

        // get the data back from the server
        return readKey(sizeLocation);

    }


    public synchronized void putAll(Map<? extends K, ? extends V> map) {

        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }

    }

    private void writeEntries(Map<? extends K, ? extends V> map) {

        final HashMap<K, V> safeCopy = new HashMap<K, V>(map);

        out.writeStopBit(safeCopy.size());

        final Set<Entry> entries = (Set) safeCopy.entrySet();

        for (Entry e : entries) {
            writeKey(e.getKey());
            writeValue(e.getValue());
        }
    }


    public synchronized void clear() {
        long sizeLocation = writeEvent(CLEAR);

        // get the data back from the server
        blockingFetch(sizeLocation);

    }

    @NotNull

    public synchronized Set<K> keySet() {
        long sizeLocation = writeEvent(KEY_SET);

        // get the data back from the server
        return readKeySet(blockingFetch(sizeLocation));
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
        long sizeLocation = writeEvent(ENTRY_SET);

        // get the data back from the server
        Bytes in = blockingFetch(sizeLocation);

        long size = in.readStopBit();

        Set<Map.Entry<K, V>> result = new HashSet<Map.Entry<K, V>>();

        for (int i = 0; i < size; i++) {
            K k = keyValueSerializer.readKey(in);
            V v = keyValueSerializer.readValue(in);
            result.add(new Entry(k, v));
        }

        return result;
    }


    private long writeEvent(EventId event) {
        out.clear();
        out.buffer().clear();

        out.write((byte) event.ordinal());
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
            return blockingFetchThrowable(sizeLocation, this.builder.timeoutMs());
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }

    }

    private void clearBuffers() {
        out.clear();
        out.buffer().clear();
        in.clear();
        in.buffer().clear();
    }


    private Bytes blockingFetchThrowable(long sizeLocation, long timeOutMs) throws IOException {

        // free up space in the buffer
        //   compact();

        long startTime = System.currentTimeMillis();
        long transactionId = nextUniqueTransaction(startTime);
        long timeoutTime = startTime + timeOutMs;

        if (LOG.isDebugEnabled())
            LOG.debug("sending data with transactionId=" + transactionId);

        out.writeLong(transactionId);
        writeSizeAt(sizeLocation);

        out.buffer().limit((int) out.position());
        while (out.buffer().remaining() > 0) {
            clientChannel.write(out.buffer());
            checkTimeout(timeoutTime);
        }

        in.clear();
        in.buffer().clear();

       /* in.buffer().limit(in.buffer().capacity());
        in.limit(in.buffer().position());
*/
        // read size
        while (in.buffer().position() < 2) {
            clientChannel.read(in.buffer());
            checkTimeout(timeoutTime);
        }

        in.limit(in.buffer().position());
        int size = in.readUnsignedShort();

        LOG.info("size=" + size);

        while (in.buffer().position() < size) {
            clientChannel.read(in.buffer());
            checkTimeout(timeoutTime);
        }
        in.limit(in.buffer().position());

        boolean isException = in.readBoolean();

        long inTransactionId = in.readLong();

        if (inTransactionId != transactionId) {
            throw new IllegalStateException("the received transaction-id=" + inTransactionId +
                    ", does not match the expected transaction-id=" + transactionId);
        }

        if (isException) {
            int len = in.readUnsignedShort();

            in.limit(in.position() + len);

            Object o = in.readObject();
            RuntimeException runtimeException = (RuntimeException) o;
            throw runtimeException;
        }

        return in;
    }

    private void checkTimeout(long timeoutTime) {
        if (timeoutTime < System.currentTimeMillis())
            throw new TimeoutRuntimeException();
    }

    private void writeSizeAt(long locationOfSize) {
        long size = out.position() - locationOfSize;
        out.writeUnsignedShort(locationOfSize, (int) size - 2);
    }

    private long markSizeLocation() {
        long position = out.position();
        out.skip(2);
        return position;
    }

    private void writeKey(K key) {
        keyValueSerializer.writeKey(key, out);
    }

    private V readKey(final long sizeLocation) {
        return keyValueSerializer.readValue(blockingFetch(sizeLocation));
    }

    private void writeValue(V value) {
        keyValueSerializer.writeValue(value, out);
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

