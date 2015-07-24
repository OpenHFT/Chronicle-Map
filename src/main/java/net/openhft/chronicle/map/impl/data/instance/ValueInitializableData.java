package net.openhft.chronicle.map.impl.data.instance;

import net.openhft.chronicle.hash.Data;

public interface ValueInitializableData<V> extends Data<V> {
    
    void initValue(V value);
}
