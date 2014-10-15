package net.openhft.chronicle.map;

import net.openhft.chronicle.map.serialization.MetaBytesInterop;
import net.openhft.chronicle.map.serialization.MetaBytesWriter;
import net.openhft.chronicle.map.threadlocal.ThreadLocalCopies;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.map.StatelessMapClient.EventId.*;

/**
 * **** THIS IS VERY MUCH IN DRAFT ***
 *
 * @author Rob Austin.
 */
public class StatelessMapClient<K, KI, MKI extends MetaBytesInterop<K, KI>,
        V, VW, MVW extends MetaBytesWriter<V, VW>> extends AbstractMap<K, V>
        implements ChronicleMap<K, V>, Serializable {


    private ThreadLocal<Buffer> sourceBuffer = new ThreadLocal<Buffer>();


    Map<Long, Object> transactionIDs = new HashMap<Long, Object>();


    AtomicLong transactionID = new AtomicLong();

    private long timeoutMs = TimeUnit.SECONDS.toMillis(20);
    private final Serializer<V, ?, ?> valueSerializer;
    private final Serializer<K, ?, ?> keySerializer;


    @Override
    public File file() {
        return null;
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("This is not supported in the " + this.getClass()
                .getSimpleName());
    }

    public static enum EventId {
        LONG_SIZE, SIZE,
        IS_EMPTY,
        CONTAINS_KEY,
        CONTAINS_VALUE,
        GET, PUT,
        REMOVE,
        PUT_ALL,
        CLEAR,
        KEY_SET,
        VALUES,
        ENTRY_SET,
        REPLACE,
        REPLACE_WITH_OLD_AND_NEW_VALUE,
        PUT_IF_ABSENT,
        REMOVE_WITH_VALUE
    }


    long nextUniqueTransaction() {
        long time = System.currentTimeMillis();

        long l = transactionID.get();
        if (time > l) {
            boolean b = transactionID.compareAndSet(l, time);
            if (b) return time;
        }

        return transactionID.incrementAndGet();
    }


    private final Bytes out;


    public StatelessMapClient(Bytes out, ChronicleMapBuilder<K, V> builder) {

        valueSerializer = new Serializer(builder.valueBuilder);
        keySerializer = new Serializer(builder.keyBuilder);

        this.out = out;

    }


    @Override
    public V putIfAbsent(K key, V value) {
        out.writeByte(PUT_IF_ABSENT.ordinal());

        writeKey(key);
        writeValue(value);

        return null;
    }


    @Override
    public boolean remove(Object key, Object value) {
        out.writeByte(REMOVE_WITH_VALUE.ordinal());


        writeKey((K) key);
        writeValue((V) value);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();

    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        out.writeByte(REPLACE_WITH_OLD_AND_NEW_VALUE.ordinal());

        writeKey(key);
        writeValue(oldValue);
        writeValue(newValue);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();

    }


    @Override
    public V replace(K key, V value) {
        out.writeByte(REPLACE.ordinal());

        writeKey(key);
        writeValue(value);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));
    }


    @Override
    public int size() {

        out.writeByte(SIZE.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readInt();
    }


    @Override
    public boolean isEmpty() {
        out.writeByte(IS_EMPTY.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();
    }

    @Override
    public boolean containsKey(Object key) {
        out.writeByte(CONTAINS_KEY.ordinal());

        writeKey((K) key);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();

    }

    @Override
    public boolean containsValue(Object value) {
        out.writeByte(CONTAINS_VALUE.ordinal());
        writeValue((V) value);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();
    }

    @Override
    public long longSize() {
        out.writeByte(LONG_SIZE.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readLong();
    }

    @Override
    public V get(Object key) {
        out.writeByte(GET.ordinal());

        writeKey((K) key);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));

    }

    V readValue(ByteBufferBytes source) {


        return valueSerializer.readMarshallable(source);


    }

    @Override
    public V getUsing(K key, V value) {
        throw new UnsupportedOperationException("getUsing is not supported for stateless clients");
    }

    @Override
    public V acquireUsing(K key, V value) {
        throw new UnsupportedOperationException("getUsing is not supported for stateless clients");
    }

    @Override
    public V put(K key, V value) {
        out.writeByte(PUT.ordinal());


        writeKey((K) key);
        writeValue((V) value);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));

    }

    @Override
    public V remove(Object key) {
        out.writeByte(REMOVE.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));

    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {

        throw new UnsupportedOperationException("todo");
    /*    out.writeByte(PUT_ALL.ordinal());


        HashMap<K, V> safeCopy = new HashMap<K, V>(map);

        writeEntries(copies, safeCopy);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        blockingFetch(transactionId);
*/
        //return;


    }

    private void writeEntries(ThreadLocalCopies copies, HashMap<K, V> safeCopy) {
        out.writeLong(safeCopy.size());

        Set<Entry> entries = (Set) safeCopy.entrySet();

        for (Entry e : entries) {
            writeKey(e.getKey());
            writeValue(e.getValue());
        }
    }

    @Override
    public void clear() {
        out.writeByte(CLEAR.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        blockingFetch(transactionId);

        return;
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        out.writeByte(KEY_SET.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return readKeySet(blockingFetch(transactionId));
    }

    private Set<K> readKeySet(ByteBufferBytes bufferBytes) {
        int size = bufferBytes.readInt();

        HashSet<K> result = new HashSet<>();

        for (int i = 0; i < size; i++) {
            result.add(readKey(bufferBytes));
        }
        return result;
    }

    private K readKey(ByteBufferBytes bufferBytes) {
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);

        // get the data back from the server
        return readKey(blockingFetch(transactionId));

    }

    @NotNull
    @Override
    public Collection<V> values() {
        out.writeByte(VALUES.ordinal());
        long transactionId = nextUniqueTransaction();

        out.writeLong(transactionId);

        // get the data back from the server
        ByteBufferBytes bufferBytes = blockingFetch(transactionId);

        int size = bufferBytes.readInt();

        ArrayList<V> result = new ArrayList<V>();

        for (int i = 0; i < size; i++) {
            result.add(readValue(bufferBytes));
        }
        return result;
    }

    @NotNull
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        out.writeByte(ENTRY_SET.ordinal());

        long transactionId = nextUniqueTransaction();

        // get the data back from the server
        ByteBufferBytes bufferBytes = blockingFetch(transactionId);

        int size = bufferBytes.readInt();

        Set<Map.Entry<K, V>> result = new HashSet<Map.Entry<K, V>>();

        for (int i = 0; i < size; i++) {
            K k = readKey(bufferBytes);
            V v = readValue(bufferBytes);
            result.add(new Entry(k, v));

        }


        return result;
    }


    void writeValue(V value) {
        valueSerializer.writeMarshallable(out, value);
    }

    /**
     * write the keysize and the key to the the {@code target} buffer
     *
     * @param key the key of the map
     */
    private void writeKey(K key) {
        keySerializer.writeMarshallable(out, key);
    }


    public Buffer buffer() {
        Buffer result = sourceBuffer.get();


        if (result != null) {
            return result;
        }

        result = new Buffer() {

            volatile ByteBufferBytes buffer = null;

            @Override
            public void set(ByteBufferBytes source) {
                buffer = source;
            }

            @Override
            public ByteBufferBytes get() {
                return buffer;
            }
        };

        sourceBuffer.set(result);
        return result;
    }


    /**
     * blocks until a message is received from the server or a timeout is reached
     *
     * @param transactionId
     * @return
     */
    private ByteBufferBytes blockingFetch(long transactionId) {
        final Buffer buffer = buffer();
        buffer.set(null);

        synchronized (this) {
            transactionIDs.put(transactionId, buffer);
            try {
                buffer.wait(timeoutMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        final ByteBufferBytes bufferBytes = buffer.get();

        if (bufferBytes == null)
            throw new RuntimeException("Timed-out", new TimeoutException());

        return bufferBytes;
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
            StatelessMapClient.this.put((K) getKey(), (V) newValue);
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
}


interface Buffer {

    void set(ByteBufferBytes source);

    ByteBufferBytes get();
}
