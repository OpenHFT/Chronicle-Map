package net.openhft.chronicle.hash.impl.value.instance;

import net.openhft.chronicle.hash.Value;

public interface KeyInitableValue<K, T> extends Value<K, T> {
    
    void initKey(K key);
}
