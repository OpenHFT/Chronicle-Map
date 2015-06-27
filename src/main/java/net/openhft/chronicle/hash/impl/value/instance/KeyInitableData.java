package net.openhft.chronicle.hash.impl.value.instance;

import net.openhft.chronicle.hash.Data;

public interface KeyInitableData<K, T> extends Data<K, T> {
    
    void initKey(K key);
}
