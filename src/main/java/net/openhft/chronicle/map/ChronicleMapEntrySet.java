/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
