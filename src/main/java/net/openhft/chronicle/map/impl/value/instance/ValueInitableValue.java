package net.openhft.chronicle.map.impl.value.instance;

import net.openhft.chronicle.hash.Value;

public interface ValueInitableValue<V, T> extends Value<V, T> {
    
    void initValue(V value);
}
