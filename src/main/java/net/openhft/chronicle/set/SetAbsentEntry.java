package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashAbsentEntry;

public interface SetAbsentEntry<K> extends HashAbsentEntry<K> {
    @Override
    SetContext<K, ?> context();
    
    void doInsert();
}
