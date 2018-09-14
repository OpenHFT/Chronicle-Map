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

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.hash.impl.util.CharSequences;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static net.openhft.chronicle.hash.impl.util.Objects.requireNonNull;

interface AbstractChronicleMap<K, V> extends ChronicleMap<K, V> {

    // TODO quick and dirty. Think about how generic guava/koloboke equivalence interface could be
    // used. See also BytesInterop.equivalent() and hash().
    static int hashCode(Object obj) {
        if (!(obj instanceof CharSequence)) {
            return obj.hashCode();
        } else {
            return CharSequences.hash((CharSequence) obj);
        }
    }

    @Override
    default <R> R getMapped(K key, @NotNull SerializableFunction<? super V, R> function) {
        requireNonNull(function);
        try (ExternalMapQueryContext<K, V, ?> c = queryContext(key)) {
            MapEntry<K, V> entry = c.entry();
            return entry != null ? function.apply(entry.value().get()) : null;
        }
    }

    @Override
    default void getAll(File toFile) throws IOException {
        synchronized (this) {
            JsonSerializer.getAll(toFile, this, emptyList());
        }
    }

    @Override
    default void putAll(File fromFile) throws IOException {
        synchronized (this) {
            JsonSerializer.putAll(fromFile, this, emptyList());
        }
    }

    @Override
    default boolean containsValue(Object value) {
        return !forEachEntryWhile(c -> !c.value().equals(c.context().wrapValueAsData((V) value)));
    }

    @Override
    default boolean isEmpty() {
        return size() == 0;
    }

    @Override
    default void forEach(BiConsumer<? super K, ? super V> action) {
        forEachEntry(c -> action.accept(c.key().get(), c.value().get()));
    }

    @Override
    default void putAll(Map<? extends K, ? extends V> m) {
        m.forEach(this::put);
    }

    @NotNull
    @Override
    default Collection<V> values() {
        // todo cache view object
        return new AbstractCollection<V>() {
            @Override
            public Iterator<V> iterator() {
                return new Iterator<V>() {
                    private Iterator<Entry<K, V>> i = entrySet().iterator();

                    @Override
                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override
                    public V next() {
                        return i.next().getValue();
                    }

                    @Override
                    public void remove() {
                        i.remove();
                    }
                };
            }

            @Override
            public int size() {
                return AbstractChronicleMap.this.size();
            }

            @Override
            public boolean isEmpty() {
                return AbstractChronicleMap.this.isEmpty();
            }

            @Override
            public void clear() {
                AbstractChronicleMap.this.clear();
            }

            @Override
            public boolean contains(Object v) {
                return AbstractChronicleMap.this.containsValue(v);
            }

            @Override
            public void forEach(java.util.function.Consumer<? super V> action) {
                AbstractChronicleMap.this.forEachEntry(c -> action.accept(c.value().get()));
            }
        };
    }

    @NotNull
    @Override
    default Set<K> keySet() {
        // todo cache view object
        return new AbstractSet<K>() {
            @Override
            public Iterator<K> iterator() {
                return new ChronicleMapIterator.OfKeys<>(AbstractChronicleMap.this);
            }

            @Override
            public int size() {
                return AbstractChronicleMap.this.size();
            }

            public boolean isEmpty() {
                return AbstractChronicleMap.this.isEmpty();
            }

            public void clear() {
                AbstractChronicleMap.this.clear();
            }

            public boolean contains(Object k) {
                return AbstractChronicleMap.this.containsKey(k);
            }

            @Override
            public void forEach(java.util.function.Consumer<? super K> action) {
                AbstractChronicleMap.this.forEachEntry(c -> action.accept(c.key().get()));
            }
        };
    }

    default boolean mapEquals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof Map))
            return false;
        Map<?, ?> m = (Map<?, ?>) o;
        if ((m instanceof ChronicleMap ? ((ChronicleMap) m).longSize() : m.size()) != longSize())
            return false;

        try {
            return forEachEntryWhile(c -> {
                K k = c.key().get();
                V v = (V) m.get(k);
                if (v instanceof CharSequence) {
                    return CharSequences.equivalent(
                            (CharSequence) v, (CharSequence) c.value().get());
                } else if (v instanceof Set) {
                    return v.equals(c.value().get());
                } else if (v instanceof Map) {
                    return v.equals(c.value().get());
                }
                return v != null && c.value().equals(c.context().wrapValueAsData(v));
            });
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }
    }

    default int mapHashCode() {
        int[] h = new int[1];
        forEach((k, v) -> h[0] += hashCode(k) ^ hashCode(v));
        return h[0];
    }

    default String mapToString() {
        if (isEmpty())
            return "{}";
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        forEach((k, v) -> sb
                .append(k != AbstractChronicleMap.this ? k : "(this Map)")
                .append('=')
                .append(v != AbstractChronicleMap.this ? v : "(this Map)")
                .append(',').append(' '));
        if (sb.length() > 2)
            sb.setLength(sb.length() - 2);
        sb.append('}');
        return sb.toString();
    }

    default Set<Entry<K, V>> newEntrySet() {
        return new ChronicleMapEntrySet<>(this);
    }

    @Override
    default void forEachEntry(final Consumer<? super MapEntry<K, V>> action) {
        forEachEntryWhile(c -> {
            action.accept(c);
            return true;
        });
    }

    @Override
    default boolean forEachEntryWhile(final Predicate<? super MapEntry<K, V>> action) {
        boolean interrupt = false;
        for (int i = segments() - 1; i >= 0; i--) {
            try (MapSegmentContext<K, V, ?> c = segmentContext(i)) {
                if (!c.forEachSegmentEntryWhile(action)) {
                    interrupt = true;
                    break;
                }
            }
        }
        return !interrupt;
    }
}
