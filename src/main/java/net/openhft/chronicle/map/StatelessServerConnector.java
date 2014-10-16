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


    public void marshall(@NotNull Bytes in, @NotNull Bytes out) {

        byte b = in.readByte();

        StatelessMapClient.EventId eventId = StatelessMapClient.EventId.values()[b];

        switch (eventId) {

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
            case PUT_ALL:
                putAll(in, out);
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

        }

    }

    private void removeWithValue(Bytes in, Bytes out) {
        boolean result = map.remove(readKey(in), readValue(in));
        reflectTransactionId(in, out);
        out.writeBoolean(result);
    }

    private void replaceWithOldAndNew(Bytes in, Bytes out) {
        final K key = readKey(in);
        V oldValue = readValue(in);
        V newValue = readValue(in);
        reflectTransactionId(in, out);
        map.replace(key, oldValue, newValue);
    }

    public void longSize(Bytes in, Bytes out) {
        reflectTransactionId(in, out);
        out.writeLong(map.longSize());
    }

    public void size(Bytes in, Bytes out) {
        reflectTransactionId(in, out);
        out.writeInt(map.size());
    }

    public void isEmpty(Bytes in, Bytes out) {
        reflectTransactionId(in, out);
        out.writeBoolean(map.isEmpty());
    }

    public void containsKey(Bytes in, Bytes out) {
        K k = readKey(in);
        reflectTransactionId(in, out);
        out.writeBoolean(map.containsKey(k));
    }

    public void containsValue(Bytes in, Bytes out) {
        V v = readValue(in);
        reflectTransactionId(in, out);
        out.writeBoolean(map.containsValue(v));
    }

    public void get(Bytes in, Bytes out) {
        K k = readKey(in);
        reflectTransactionId(in, out);
        writeValue(map.get(k), out);
    }

    public void put(Bytes in, Bytes out) {
        K k = readKey(in);
        V v = readValue(in);
        reflectTransactionId(in, out);
        writeValue(map.put(k, v), out);
    }

    public void remove(Bytes in, Bytes out) {
        final V value = map.remove(readKey(in));
        reflectTransactionId(in, out);
        writeValue(value, out);
    }

    public void putAll(Bytes in, Bytes out) {
        map.putAll(readEntries(in));
        reflectTransactionId(in, out);
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
        reflectTransactionId(in, out);
    }


    public void keySet(Bytes in, Bytes out) {
        reflectTransactionId(in, out);

        final Set<K> ks = map.keySet();
        out.writeStopBit(ks.size());
        for (K key : ks) {
            writeKey(key, out);
        }
    }

    public void values(Bytes in, Bytes out) {
        reflectTransactionId(in, out);
        final Collection<V> values = map.values();
        out.writeStopBit(values.size());
        for (final V value : values) {
            writeValue(value, out);
        }
    }

    public void entrySet(Bytes in, Bytes out) {
        reflectTransactionId(in, out);

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
        reflectTransactionId(in, out);
        writeValue(map.putIfAbsent(key, v), out);
    }


    public void replace(Bytes in, Bytes out) {
        K k = readKey(in);
        V v = readValue(in);

        reflectTransactionId(in, out);
        map.replace(k, v);
    }

    /**
     * write the keysize and the key to the the {@code target} buffer
     *
     * @param key the key of the map
     */
    private void writeKey(K key, Bytes out) {
        keyValueSerializer.writeKey(key, out);
    }

    private void reflectTransactionId(Bytes in, Bytes out) {
        out.writeLong(in.readLong());
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
