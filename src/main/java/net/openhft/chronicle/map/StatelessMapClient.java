package net.openhft.chronicle.map;

import net.openhft.chronicle.map.serialization.*;
import net.openhft.chronicle.map.threadlocal.Provider;
import net.openhft.chronicle.map.threadlocal.ThreadLocalCopies;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.BytesStore;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

import javax.swing.text.Segment;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.map.StatelessMapClient.EventId.*;

/**
 *
 * **** THIS IS VERY MUCH IN DRAFT ***
 *
 * @author Rob Austin.
 */
public class StatelessMapClient<K, KI, MKI extends MetaBytesInterop<K, KI>,
        V, VW, MVW extends MetaBytesWriter<V, VW>> extends AbstractMap<K, V>
        implements ChronicleMap<K, V>, Serializable {

    final Class<K> kClass;
    private ThreadLocal<Buffer> sourceBuffer = new ThreadLocal<Buffer>();

    final BytesReader<K> originalKeyReader;
    transient Provider<BytesReader<K>> keyReaderProvider;
    final KI originalKeyInterop;
    transient Provider<KI> keyInteropProvider;
    final MKI originalMetaKeyInterop;
    final MetaProvider<K, KI, MKI> metaKeyInteropProvider;

    final Class<V> vClass;
    final SizeMarshaller valueSizeMarshaller;
    final BytesReader<V> originalValueReader;
    transient Provider<BytesReader<V>> valueReaderProvider;
    final VW originalValueWriter;
    transient Provider<VW> valueWriterProvider;
    final MVW originalMetaValueWriter;
    final MetaProvider<V, VW, MVW> metaValueWriterProvider;
    final ObjectFactory<V> valueFactory;
    final DefaultValueProvider<K, V> defaultValueProvider;

    Map<Long, Object> transactionIDs = new HashMap<Long, Object>();


    AtomicLong transactionID = new AtomicLong();

    private final long lockTimeOutNS;

    transient Segment[] segments; // non-final for close()
    // non-final for close() and because it is initialized out of constructor
    transient BytesStore ms;
    transient long headerSize;
    transient Set<Map.Entry<K, V>> entrySet;

    private int bits;
    private int mask;
    private long timeoutMs = TimeUnit.SECONDS.toMillis(20);


    @Override
    public File file() {
        return null;
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("This is not supported in the " + this.getClass()
                .getSimpleName());
    }

    enum EventId {
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


    private final ByteBufferBytes target;
    private final SizeMarshaller keySizeMarshaller;

    public StatelessMapClient(ByteBufferBytes target, ChronicleMapBuilder<K, V> builder) {
        this.target = target;

        SerializationBuilder<K> keyBuilder = builder.keyBuilder;
        kClass = keyBuilder.eClass;
        keySizeMarshaller = keyBuilder.sizeMarshaller();
        originalKeyReader = keyBuilder.reader();

        originalKeyInterop = (KI) keyBuilder.interop();

        originalMetaKeyInterop = (MKI) keyBuilder.metaInterop();
        metaKeyInteropProvider = (MetaProvider<K, KI, MKI>) keyBuilder.metaInteropProvider();

        SerializationBuilder<V> valueBuilder = builder.valueBuilder;
        vClass = valueBuilder.eClass;
        valueSizeMarshaller = valueBuilder.sizeMarshaller();
        originalValueReader = valueBuilder.reader();

        originalValueWriter = (VW) valueBuilder.interop();
        originalMetaValueWriter = (MVW) valueBuilder.metaInterop();
        metaValueWriterProvider = (MetaProvider) valueBuilder.metaInteropProvider();
        valueFactory = valueBuilder.factory();
        defaultValueProvider = builder.defaultValueProvider();

        lockTimeOutNS = builder.lockTimeOut(TimeUnit.NANOSECONDS);


    }

    @Override
    public V putIfAbsent(K key, V value) {
        target.writeByte(PUT_IF_ABSENT.ordinal());

        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);

        writeKey(key, copies);
        writeValue(value, copies);

        return null;
    }


    @Override
    public boolean remove(Object key, Object value) {
        target.writeByte(REMOVE_WITH_VALUE.ordinal());

        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        writeKey((K) key, copies);
        writeValue((V) value, copies);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();

    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        target.writeByte(REPLACE_WITH_OLD_AND_NEW_VALUE.ordinal());
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        writeKey(key, copies);
        writeValue(oldValue, copies);
        writeValue(newValue, copies);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();

    }

    @Override
    public V replace(K key, V value) {
        target.writeByte(REPLACE.ordinal());
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        writeKey(key, copies);
        writeValue(value, copies);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));
    }


    @Override
    public int size() {

        target.writeByte(SIZE.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readInt();
    }


    @Override
    public boolean isEmpty() {
        target.writeByte(IS_EMPTY.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();
    }

    @Override
    public boolean containsKey(Object key) {
        target.writeByte(CONTAINS_KEY.ordinal());
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        writeKey((K) key, copies);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();

    }

    @Override
    public boolean containsValue(Object value) {
        target.writeByte(CONTAINS_VALUE.ordinal());
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        writeValue((V) value, copies);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readBoolean();
    }

    @Override
    public long longSize() {
        target.writeByte(LONG_SIZE.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return blockingFetch(transactionId).readLong();
    }

    @Override
    public V get(Object key) {
        target.writeByte(GET.ordinal());
        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        writeKey((K) key, copies);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));

    }

    private V readValue(ByteBufferBytes byteBufferBytes) {
        throw new UnsupportedOperationException("todo");
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
        target.writeByte(PUT.ordinal());

        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);
        writeKey((K) key, copies);
        writeValue((V) value, copies);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));

    }

    @Override
    public V remove(Object key) {
        target.writeByte(REMOVE.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        return readValue(blockingFetch(transactionId));

    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        target.writeByte(PUT_ALL.ordinal());

        ThreadLocalCopies copies = keyInteropProvider.getCopies(null);

        HashMap<K, V> safeCopy = new HashMap<K, V>(map);

        writeEntries(copies, safeCopy);

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        blockingFetch(transactionId);

        return;


    }

    private void writeEntries(ThreadLocalCopies copies, HashMap<K, V> safeCopy) {
        target.writeLong(safeCopy.size());

        Set<Entry> entries = (Set) safeCopy.entrySet();

        for (Entry e : entries) {
            writeKey(e.getKey(), copies);
            writeValue(e.getValue(), copies);
        }
    }

    @Override
    public void clear() {
        target.writeByte(CLEAR.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

        // get the data back from the server
        blockingFetch(transactionId);

        return;
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        target.writeByte(KEY_SET.ordinal());

        // send the transaction id
        long transactionId = nextUniqueTransaction();
        target.writeLong(transactionId);

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
        target.writeLong(transactionId);

        // get the data back from the server
        return readKey(blockingFetch(transactionId));

    }

    @NotNull
    @Override
    public Collection<V> values() {
        target.writeByte(VALUES.ordinal());
        long transactionId = nextUniqueTransaction();

        target.writeLong(transactionId);

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
        target.writeByte(ENTRY_SET.ordinal());

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


    private void writeValue(V value, ThreadLocalCopies copies) {

        VW valueWriter = valueWriterProvider.get(copies, originalValueWriter);
        copies = valueWriterProvider.getCopies(copies);
        long valueSize;
        MetaBytesWriter<V, VW> metaValueWriter = null;
        Byteable valueAsByteable = null;

        if (value instanceof Byteable) {
            valueAsByteable = (Byteable) value;
            valueSize = valueAsByteable.maxSize();

            target.writeLong(valueSize);
            target.write(valueAsByteable.bytes());
        } else {
            copies = valueWriterProvider.getCopies(copies);
            valueWriter = valueWriterProvider.get(copies, originalValueWriter);
            copies = metaValueWriterProvider.getCopies(copies);
            metaValueWriter = metaValueWriterProvider.get(
                    copies, originalMetaValueWriter, valueWriter, value);
            valueSize = metaValueWriter.size(valueWriter, value);
            target.writeLong(valueSize);
            metaValueWriter.write(valueWriter, target, value);
        }

    }

    /**
     * write the keysize and the key to the the {@code target} buffer
     *
     * @param key    the key of the map
     * @param copies
     */
    private void writeKey(K key, final ThreadLocalCopies copies) {
        KI keyWriter = keyInteropProvider.get(copies, originalKeyInterop);

        MKI metaKeyWriter =
                metaKeyInteropProvider.get(copies, originalMetaKeyInterop, keyWriter, key);

        long keySize = metaKeyWriter.size(keyWriter, key);

        keySizeMarshaller.writeSize(target, keySize);
        metaKeyWriter.write(keyWriter, target, key);
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
