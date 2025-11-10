//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map;

import org.jetbrains.annotations.NotNull;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

class ChronicleMapEntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {

    private final AbstractChronicleMap<K, V> map;

    public ChronicleMapEntrySet(AbstractChronicleMap<K, V> map) {
        this.map = map;
    }

    @NotNull
    public Iterator<Map.Entry<K, V>> iterator() {
        return new ChronicleMapIterator.OfEntries<>(map);
    }

    public final boolean contains(Object o) {
        if (!(o instanceof Map.Entry))
            return false;
        Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
        try {
            V v = map.get(e.getKey());
            return v != null && v.equals(e.getValue());
        } catch (ClassCastException | NullPointerException ex) {
            return false;
        }
    }

    public final boolean remove(Object o) {
        if (!(o instanceof Map.Entry))
            return false;
        Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
        try {
            Object key = e.getKey();
            Object value = e.getValue();
            return map.remove(key, value);
        } catch (ClassCastException | NullPointerException ex) {
            return false;
        }
    }

    public final int size() {
        return map.size();
    }

    public final boolean isEmpty() {
        return map.isEmpty();
    }

    public final void clear() {
        map.clear();
    }
}
