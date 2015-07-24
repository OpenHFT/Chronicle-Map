/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.hash.impl.util.CharSequences;
import net.openhft.chronicle.map.impl.IterationContextInterface;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static net.openhft.chronicle.hash.impl.util.Objects.requireNonNull;

interface AbstractChronicleMap<K, V> extends ChronicleMap<K, V>, Serializable {

    @Override
    default <R> R getMapped(K key, @NotNull SerializableFunction<? super V, R> function) {
        requireNonNull(function);
        try (ExternalMapQueryContext<K, V, ?> c = queryContext(key)) {
            MapEntry<K, V> entry = c.entry();
            return entry != null ? function.apply(entry.value().get()) : null;
        }
    }

    @Override
    default V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {
        requireNonNull(unaryOperator);
        try (ExternalMapQueryContext<K, V, ?> c = queryContext(key)) {
            // putMapped() should find a value, update & put most of the time,
            // so don't try to check key presence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            MapEntry<K, V> entry = c.entry();
            if (entry != null) {
                V newValue = unaryOperator.update(entry.value().get());
                c.replaceValue(entry, c.wrapValueAsData(newValue));
                return newValue;
            } else {
                return null;
            }
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

    int actualSegments();

    @Override
    default boolean containsValue(Object value) {
        return !forEachEntryWhile(c -> !c.valueEqualTo((V) value));
    }

    @Override
    default boolean isEmpty() {
        return size() == 0;
    }

    @Override
    default void forEach(BiConsumer<? super K, ? super V> action) {
        forEachEntry(c -> action.accept(c.key(), c.get()));
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
                    private Iterator<Entry<K,V>> i = entrySet().iterator();

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
                AbstractChronicleMap.this.forEachEntry(c -> action.accept(c.get()));
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
                AbstractChronicleMap.this.forEachEntry(c -> action.accept(c.key()));
            }
        };
    }

    default boolean mapEquals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof Map))
            return false;
        Map<?,?> m = (Map<?,?>) o;
        if ((m instanceof ChronicleMap ? ((ChronicleMap) m).longSize() : m.size()) != longSize())
            return false;

        try {
            return forEachEntryWhile(c -> {
                K k = c.key();
                V v = (V) m.get(k instanceof CharSequence ? k.toString() : k);
                return v != null && c.valueEqualTo(v);
            });
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }
    }

    default int mapHashCode() {
        int[] h = new int[1];
        forEach((k, v) -> h[0] += hashCode(k) ^ hashCode(v));
        return h[0];
    }

    // TODO quick and dirty. Think about how generic guava/koloboke equivalence interface could be
    // used. See also BytesInterop.equivalent() and hash().
    public static int hashCode(Object obj) {
        if (!(obj instanceof CharSequence)) {
            return obj.hashCode();
        } else {
            return CharSequences.hash((CharSequence) obj);
        }
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
    default void forEachEntry(final Consumer<? super MapKeyContext<K, V>> action) {
        forEachEntryWhile(c -> {
            action.accept(c);
            return true;
        });
    }
    
    IterationContextInterface<K, V> iterationContext();

    @Override
    default boolean forEachEntryWhile(final Predicate<? super MapKeyContext<K, V>> action) {
        boolean interrupt = false;
        iteration:
        try (IterationContextInterface<K, V> c = iterationContext()) {
            for (int segmentIndex = actualSegments() - 1; segmentIndex >= 0; segmentIndex--) {
                c.initTheSegmentIndex(segmentIndex);
                if (!c.forEachRemoving(e -> action.test(c.deprecatedMapKeyContextOnIteration()))) {
                    interrupt = true;
                    break iteration;
                }
            }
        }
        return !interrupt;
    }
}
