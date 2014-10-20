package net.openhft.chronicle.map;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;

import java.util.*;

/**
 * @author Rob Austin.
 */
class StatelessServerConnector<K, V> {


    private final KeyValueSerializer<K, V> keyValueSerializer;
    private final ChronicleMap<K, V> map;
    private double maxEntrySizeBytes;


    public StatelessServerConnector(KeyValueSerializer<K, V> keyValueSerializer, ChronicleMap<K, V> map) {
        this.keyValueSerializer = keyValueSerializer;
        this.map = map;
    }

    public Work processStatelessEvent(byte eventId, @NotNull Bytes in, @NotNull Bytes out) {

        final StatelessMapClient.EventId event = StatelessMapClient.EventId.values()[eventId];

        switch (event) {

            case LONG_SIZE:
                return longSize(in, out);

            case IS_EMPTY:
                return isEmpty(in, out);

            case CONTAINS_KEY:
                return containsKey(in, out);

            case CONTAINS_VALUE:
                return containsValue(in, out);

            case GET:
                return get(in, out);

            case PUT:
                return put(in, out);

            case REMOVE:
                return remove(in, out);

            case CLEAR:
                return clear(in, out);

            case KEY_SET:
                return keySet(in, out);

            case VALUES:
                return values(in, out);

            case ENTRY_SET:
                return entrySet(in, out);

            case REPLACE:
                return replace(in, out);

            case REPLACE_WITH_OLD_AND_NEW_VALUE:
                return replaceWithOldAndNew(in, out);

            case PUT_IF_ABSENT:
                return putIfAbsent(in, out);

            case REMOVE_WITH_VALUE:
                return removeWithValue(in, out);

            case SIZE:
                return size(in, out);

            default:
                throw new IllegalStateException("unsupported event=" + event);

        }

    }

    private Work removeWithValue(Bytes in, Bytes out) {
        boolean result = map.remove(readKey(in), readValue(in));
        out.writeBoolean(result);
        return null;
    }

    private Work replaceWithOldAndNew(Bytes in, Bytes out) {

        final K key = readKey(in);
        V oldValue = readValue(in);
        V newValue = readValue(in);

        long sizeLocation = reflectTransactionId(in, out);
        try {
            map.replace(key, oldValue, newValue);
        } catch (RuntimeException e) {
            writeException(e, out);
            writeSizeAndFlags(sizeLocation, true, out);
            return null;
        }
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }


    public Work longSize(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        out.writeLong(map.longSize());
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work size(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        out.writeInt(map.size());
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work isEmpty(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        out.writeBoolean(map.isEmpty());
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work containsKey(Bytes in, Bytes out) {
        K k = readKey(in);
        long sizeLocation = reflectTransactionId(in, out);
        out.writeBoolean(map.containsKey(k));
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work containsValue(Bytes in, Bytes out) {
        V v = readValue(in);
        long sizeLocation = reflectTransactionId(in, out);
        out.writeBoolean(map.containsValue(v));
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work get(Bytes in, Bytes out) {
        K k = readKey(in);
        long sizeLocation = reflectTransactionId(in, out);
        try {
            writeValue(map.get(k), out);
        } catch (RuntimeException e) {
            writeException(e, out);
            writeSizeAndFlags(sizeLocation, true, out);
            return null;
        }
        writeSizeAndFlags(sizeLocation, false, out);
        return null;

    }


    public Work put(Bytes in, Bytes out) {
        K k = readKey(in);
        V v = readValue(in);
        long sizeLocation = reflectTransactionId(in, out);
        writeValue(map.put(k, v), out);
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work remove(Bytes in, Bytes out) {
        final V value = map.remove(readKey(in));
        long sizeLocation = reflectTransactionId(in, out);
        writeValue(value, out);
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work putAll(Bytes in, Bytes out) {
        map.putAll(readEntries(in));
        long sizeLocation = reflectTransactionId(in, out);
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }


    public Work clear(Bytes in, Bytes out) {
        map.clear();
        long sizeLocation = reflectTransactionId(in, out);
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }


    public Work keySet(Bytes in, final Bytes out) {
        final long sizeLocation = reflectTransactionId(in, out);

        final Set<K> ks = map.keySet();
        out.writeStopBit(ks.size());

        final Iterator<K> iterator = ks.iterator();

        // this allows us to write more data than the buffer will allow
        return new Work() {

            @Override
            public boolean doWork(Bytes out) {

                while (iterator.hasNext()) {

                    // we've filled up the buffer, so lets give another channel a chance to send
                    // some data, we don't know the max key size, we will use the entrySize instead
                    if (out.remaining() <= maxEntrySizeBytes)
                        return false;

                    writeKey(iterator.next(), out);
                }

                writeSizeAndFlags(sizeLocation, false, out);
                return true;
            }
        };
    }

    public Work values(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        final Collection<V> values = map.values();
        out.writeStopBit(values.size());
        for (final V value : values) {
            writeValue(value, out);
        }
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    public Work entrySet(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);

        final Set<Map.Entry<K, V>> entries = map.entrySet();
        out.writeStopBit(entries.size());
        for (Map.Entry<K, V> e : entries) {
            writeKey(e.getKey(), out);
            writeValue(e.getValue(), out);
        }
        return null;
    }

    public Work putIfAbsent(Bytes in, Bytes out) {
        K key = readKey(in);
        V v = readValue(in);
        long sizeLocation = reflectTransactionId(in, out);
        writeValue(map.putIfAbsent(key, v), out);
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }


    public Work replace(Bytes in, Bytes out) {
        K k = readKey(in);
        V v = readValue(in);

        long sizeLocation = reflectTransactionId(in, out);
        map.replace(k, v);
        writeSizeAndFlags(sizeLocation, false, out);
        return null;
    }

    /**
     * write the keysize and the key to the the {@code target} buffer
     *
     * @param key the key of the map
     */
    private void writeKey(K key, Bytes out) {
        keyValueSerializer.writeKey(key, out);
    }

    private long reflectTransactionId(Bytes in, Bytes out) {
        long sizeLocation = out.position();
        out.skip(3);
        long transactionId = in.readLong();
        out.writeLong(transactionId);
        return sizeLocation;
    }

    private void writeValue(V value, final Bytes out) {
        keyValueSerializer.writeValue(value, out);
    }

    private K readKey(Bytes in) {
        return keyValueSerializer.readKey(in);
    }

    private V readValue(Bytes in) {
        return keyValueSerializer.readValue(in);
    }

    private void writeSizeAndFlags(long locationOfSize, boolean isException, Bytes out) {
        long size = out.position() - locationOfSize;
        out.writeUnsignedShort(0L, (int) size);
        out.writeBoolean(2L, isException);
    }

    private void writeException(RuntimeException e, Bytes out) {

        long start = out.position();
        out.skip(2);
        out.writeObject(e);
        long len = out.position() - (start + 2L);
        out.writeUnsignedShort(start, (int) len);
    }


    private Map<K, V> readEntries(Bytes in) {

        long size = in.readStopBit();
        final HashMap<K, V> result = new HashMap<K, V>();

        for (long i = 0; i < size; i++) {
            result.put(readKey(in), readValue(in));
        }
        return result;
    }
}

class KeyValueSerializer<K, V> {

    private final Serializer<V> valueSerializer;
    private final Serializer<K> keySerializer;

    KeyValueSerializer(SerializationBuilder<K> keySerializer,
                       SerializationBuilder<V> valueSerializer) {
        this.keySerializer = new Serializer<K>(keySerializer);
        this.valueSerializer = new Serializer<V>(valueSerializer);
    }

    V readValue(Bytes in) {
        if (in.readBoolean())
            return null;
        return valueSerializer.readMarshallable(in);
    }

    K readKey(Bytes in) {
        if (in.readBoolean())
            return null;

        return keySerializer.readMarshallable(in);
    }

    /**
     * write the keysize and the key to the the {@code target} buffer
     *
     * @param key the key of the map
     */
    void writeKey(K key, Bytes out) {

        out.writeBoolean(key == null);
        if (key != null)
            keySerializer.writeMarshallable(key, out);
    }

    void writeValue(V value, Bytes out) {
        out.writeBoolean(value == null);
        if (value != null)
            valueSerializer.writeMarshallable(value, out);
    }


}
