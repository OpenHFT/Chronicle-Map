package net.openhft.chronicle.map;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Rob Austin.
 */
class StatelessServerConnector<K, V> {


    private final KeyValueSerializer<K, V> keyValueSerializer;
    private final ChronicleMap<K, V> map;


    public StatelessServerConnector(KeyValueSerializer<K, V> keyValueSerializer, ChronicleMap<K, V> map) {
        this.keyValueSerializer = keyValueSerializer;
        this.map = map;
    }


    public void processStatelessEvent(byte eventId, @NotNull Bytes in, @NotNull Bytes out) {


        StatelessMapClient.EventId event = StatelessMapClient.EventId.values()[eventId];
        switch (event) {

            case LONG_SIZE:
                longSize(in, out);
                break;

            case IS_EMPTY:
                isEmpty(in, out);
                break;

            case CONTAINS_KEY:
                containsKey(in, out);
                break;
            case CONTAINS_VALUE:
                containsValue(in, out);
                break;
            case GET:
                get(in, out);
                break;
            case PUT:
                put(in, out);
                break;
            case REMOVE:
                remove(in, out);
                break;
            case CLEAR:
                clear(in, out);
                break;
            case KEY_SET:
                keySet(in, out);
                break;
            case VALUES:
                values(in, out);
                break;
            case ENTRY_SET:
                entrySet(in, out);
                break;
            case REPLACE:
                replace(in, out);
                break;
            case REPLACE_WITH_OLD_AND_NEW_VALUE:
                replaceWithOldAndNew(in, out);
                break;
            case PUT_IF_ABSENT:
                putIfAbsent(in, out);
                break;
            case REMOVE_WITH_VALUE:
                removeWithValue(in, out);
                break;

            case SIZE:
                size(in, out);
                break;

            default:
                throw new IllegalStateException("unsupported event=" + event);

        }

    }

    private void removeWithValue(Bytes in, Bytes out) {
        boolean result = map.remove(readKey(in), readValue(in));
        out.writeBoolean(result);
    }

    private void replaceWithOldAndNew(Bytes in, Bytes out) {

        final K key = readKey(in);
        V oldValue = readValue(in);
        V newValue = readValue(in);

        long sizeLocation = reflectTransactionId(in, out);
        try {
            map.replace(key, oldValue, newValue);
        } catch (RuntimeException e) {
            writeException(e, out);
            writeSizeAndFlags(sizeLocation, true, out);
            return;
        }
        writeSizeAndFlags(sizeLocation, false, out);
    }


    public void longSize(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        out.writeLong(map.longSize());
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void size(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        out.writeInt(map.size());
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void isEmpty(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        out.writeBoolean(map.isEmpty());
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void containsKey(Bytes in, Bytes out) {
        K k = readKey(in);
        long sizeLocation = reflectTransactionId(in, out);
        out.writeBoolean(map.containsKey(k));
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void containsValue(Bytes in, Bytes out) {
        V v = readValue(in);
        long sizeLocation = reflectTransactionId(in, out);
        out.writeBoolean(map.containsValue(v));
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void get(Bytes in, Bytes out) {
        K k = readKey(in);
        long sizeLocation = reflectTransactionId(in, out);
        try {
            writeValue(map.get(k), out);
        } catch (RuntimeException e) {
            writeException(e, out);
            writeSizeAndFlags(sizeLocation, true, out);
            return;
        }
        writeSizeAndFlags(sizeLocation, false, out);

    }

    private void writeException(RuntimeException e, Bytes out) {

        long start = out.position();
        out.skip(2);
        out.writeObject(e);
        long len = out.position() - (start + 2L);
        out.writeUnsignedShort(start,(int) len);

    }

    public void put(Bytes in, Bytes out) {
        K k = readKey(in);
        V v = readValue(in);
        long sizeLocation = reflectTransactionId(in, out);
        writeValue(map.put(k, v), out);
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void remove(Bytes in, Bytes out) {
        final V value = map.remove(readKey(in));
        long sizeLocation = reflectTransactionId(in, out);
        writeValue(value, out);
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void putAll(Bytes in, Bytes out) {
        map.putAll(readEntries(in));
        long sizeLocation = reflectTransactionId(in, out);
        writeSizeAndFlags(sizeLocation, false, out);
    }

    private Map<K, V> readEntries(Bytes in) {

        long size = in.readStopBit();
        final HashMap<K, V> result = new HashMap<K, V>();

        for (long i = 0; i < size; i++) {
            result.put(readKey(in), readValue(in));
        }
        return result;

    }

    public void clear(Bytes in, Bytes out) {
        map.clear();
        long sizeLocation = reflectTransactionId(in, out);
        writeSizeAndFlags(sizeLocation, false, out);
    }


    public void keySet(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);

        final Set<K> ks = map.keySet();
        out.writeStopBit(ks.size());
        for (K key : ks) {
            writeKey(key, out);
        }
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void values(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);
        final Collection<V> values = map.values();
        out.writeStopBit(values.size());
        for (final V value : values) {
            writeValue(value, out);
        }
        writeSizeAndFlags(sizeLocation, false, out);
    }

    public void entrySet(Bytes in, Bytes out) {
        long sizeLocation = reflectTransactionId(in, out);

        final Set<Map.Entry<K, V>> entries = map.entrySet();
        out.writeStopBit(entries.size());
        for (Map.Entry<K, V> e : entries) {
            writeKey(e.getKey(), out);
            writeValue(e.getValue(), out);
        }
    }

    public void putIfAbsent(Bytes in, Bytes out) {
        K key = readKey(in);
        V v = readValue(in);
        long sizeLocation = reflectTransactionId(in, out);
        writeValue(map.putIfAbsent(key, v), out);
        writeSizeAndFlags(sizeLocation, false, out);
    }


    public void replace(Bytes in, Bytes out) {
        K k = readKey(in);
        V v = readValue(in);

        long sizeLocation = reflectTransactionId(in, out);
        map.replace(k, v);
        writeSizeAndFlags(sizeLocation, false, out);
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
