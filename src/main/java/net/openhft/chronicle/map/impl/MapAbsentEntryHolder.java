package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.map.MapAbsentEntry;

public interface MapAbsentEntryHolder<K, V> {

    MapAbsentEntry<K, V> absent();
}
