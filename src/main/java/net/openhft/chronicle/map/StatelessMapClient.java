package net.openhft.chronicle.map;

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
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
public class StatelessMapClient<K, V> implements ChronicleMap<K, V> {

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

    private final ThreadLocal<Buffer> sourceBuffer = new ThreadLocal<Buffer>();
    private final Map<Long, Object> transactionIDs = new HashMap<Long, Object>();

    private final AtomicLong transactionID = new AtomicLong();

    private long timeoutMs = TimeUnit.SECONDS.toMillis(20);
    private final Serializer<V, ?, ?> valueSerializer;
    private final Serializer<K, ?, ?> keySerializer;

    public StatelessMapClient(Bytes out, Class<K> kClass, Class<V> vClass) {

        final SerializationBuilder<K> keyBuilder = new SerializationBuilder<K>(kClass,
                SerializationBuilder.Role.KEY);
        final SerializationBuilder<V> valueBuilder = new SerializationBuilder<V>(vClass,
                SerializationBuilder.Role.VALUE);

        keySerializer = new Serializer(keyBuilder);
        valueSerializer = new Serializer(valueBuilder);

        this.out = out;
    }

    @Override
    public File file() {
        return null;
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("This is not supported in the " + this.getClass()
                .getSimpleName());
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


    @Override
    public V putIfAbsent(K key, V value) {
        writeEvent(PUT_IF_ABSENT);
        writeKey(key);
        writeValue(value);

        return readValue(blockingFetch());

    }


    @Override
    public boolean remove(Object key, Object value) {
        writeEvent(REMOVE_WITH_VALUE);
        writeKey((K) key);
        writeValue((V) value);

        // get the data back from the server
        return blockingFetch().readBoolean();

    }


    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        writeEvent(REPLACE_WITH_OLD_AND_NEW_VALUE);
        writeKey(key);
        writeValue(oldValue);
        writeValue(newValue);

        // get the data back from the server
        return blockingFetch().readBoolean();
    }

    private void writeValue(V newValue) {
        valueSerializer.writeMarshallable(newValue, out);
    }


    @Override
    public V replace(K key, V value) {
        writeEvent(REPLACE);

        writeKey(key);
        writeValue(value);

        // get the data back from the server
        return readValue(blockingFetch());
    }


    @Override
    public int size() {

        writeEvent(SIZE);

        // get the data back from the server
        return blockingFetch().readInt();
    }


    @Override
    public boolean isEmpty() {
        writeEvent(IS_EMPTY);

        // get the data back from the server
        return blockingFetch().readBoolean();
    }

    @Override
    public boolean containsKey(Object key) {
        writeEvent(CONTAINS_KEY);
        writeKey((K) key);

        // get the data back from the server
        return blockingFetch().readBoolean();

    }

    @Override
    public boolean containsValue(Object value) {
        writeEvent(CONTAINS_VALUE);
        writeValue((V) value);

        // get the data back from the server
        return blockingFetch().readBoolean();
    }

    @Override
    public long longSize() {
        writeEvent(LONG_SIZE);

        // get the data back from the server
        return blockingFetch().readLong();
    }

    @Override
    public V get(Object key) {
        writeEvent(GET);
        writeKey((K) key);
        // get the data back from the server
        return readValue(blockingFetch());

    }

    V readValue(Bytes source) {
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
        writeEvent(PUT);
        writeKey(key);
        writeValue(value);


        // get the data back from the server
        return readValue(blockingFetch());

    }

    @Override
    public V remove(Object key) {
        writeEvent(REMOVE);
        writeKey((K) key);
        // get the data back from the server
        return readValue(blockingFetch());

    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {

        writeEvent(PUT_ALL);
        writeEntries(map);

        // get the data back from the server
        blockingFetch();

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

    @Override
    public void clear() {
        writeEvent(CLEAR);

        // get the data back from the server
        blockingFetch();

    }

    @NotNull
    @Override
    public Set<K> keySet() {
        writeEvent(KEY_SET);

        // get the data back from the server
        return readKeySet(blockingFetch());
    }

    private Set<K> readKeySet(Bytes in) {
        long size = in.readStopBit();
        final HashSet<K> result = new HashSet<>();

        for (long i = 0; i < size; i++) {
            result.add(readKey(out));
        }
        return result;
    }

    private K readKey(Bytes bufferBytes) {
        return keySerializer.readMarshallable(bufferBytes);
    }

    @NotNull
    @Override
    public Collection<V> values() {
        writeEvent(VALUES);

        // get the data back from the server
      final  Bytes in = blockingFetch();

        final  long size = in.readStopBit();

        if (size > Integer.MAX_VALUE)
            throw new IllegalStateException("size=" + size + " is too large.");

        final   ArrayList<V> result = new ArrayList<V>((int) size);

        for (int i = 0; i < size; i++) {
            result.add(readValue(in));
        }
        return result;
    }

    @NotNull
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        writeEvent(ENTRY_SET);


        // get the data back from the server
        Bytes bytes = blockingFetch();

        long size = bytes.readStopBit();

        Set<Map.Entry<K, V>> result = new HashSet<Map.Entry<K, V>>();

        for (int i = 0; i < size; i++) {
            K k = keySerializer.readMarshallable(bytes);
            V v = valueSerializer.readMarshallable(bytes);
            result.add(new Entry(k, v));
        }

        return result;
    }


    /**
     * write the keysize and the key to the the {@code target} buffer
     *
     * @param key the key of the map
     */
    private void writeKey(K key) {
        keySerializer.writeMarshallable(key, out);
    }

    private void writeEvent(EventId event) {
        out.write((byte) event.ordinal());
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
    private Bytes blockingFetch(long transactionId) {
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


    private Bytes blockingFetch() {
        long transactionId = nextUniqueTransaction();
        out.writeLong(transactionId);
        return blockingFetch(transactionId);
    }
}


interface Buffer {

    void set(ByteBufferBytes source);

    ByteBufferBytes get();
}
