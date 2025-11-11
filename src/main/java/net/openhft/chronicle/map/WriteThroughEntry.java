/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import java.util.AbstractMap;

@SuppressWarnings("serial")
class WriteThroughEntry<K, V> extends AbstractMap.SimpleEntry<K, V> {
    private static final long serialVersionUID = 0L;

    private final ChronicleMap<K, V> map;

    public WriteThroughEntry(ChronicleMap<K, V> map, K key, V value) {
        super(key, value);
        this.map = map;
    }

    @Override
    public V setValue(V value) {
        map.put(getKey(), value);
        return super.setValue(value);
    }
}
