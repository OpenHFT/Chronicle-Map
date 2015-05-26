package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashEntry;

public interface SetEntry<K> extends HashEntry<K> {
    @Override
    SetContext<K, ?> context();
}
