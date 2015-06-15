package net.openhft.chronicle.map.impl.value.instance;

import net.openhft.chronicle.hash.Data;

public interface ValueInitableData<V, T> extends Data<V, T> {
    
    void initValue(V value);
}
