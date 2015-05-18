package net.openhft.chronicle.map;

/**
 * Created by daniel on 18/05/15.
 */
public interface ChronicleMapEventListener<K,V> {
    void update(K key, V oldValue, V newValue);

    default void insert(K key, V value) {
        update(key, null, value);
    }

    default void remove(K key, V value) {
        update(key, value, null);
    }
}
