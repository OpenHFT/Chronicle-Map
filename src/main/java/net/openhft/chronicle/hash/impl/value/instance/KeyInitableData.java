package net.openhft.chronicle.hash.impl.value.instance;

import net.openhft.chronicle.hash.Data;

public interface KeyInitableData<K> extends Data<K> {
    
    void initKey(K key);
}
