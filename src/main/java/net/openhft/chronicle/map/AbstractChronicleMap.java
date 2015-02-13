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

import net.openhft.chronicle.hash.KeyContext;
import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.hash.impl.hashlookup.HashLookupIteration;
import net.openhft.chronicle.hash.impl.util.CharSequences;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static net.openhft.chronicle.hash.impl.HashContext.SearchState.DELETED;
import static net.openhft.chronicle.hash.impl.util.Objects.requireNonNull;

interface AbstractChronicleMap<K, V> extends ChronicleMap<K, V>, Serializable {

    @Override
    VanillaContext<K, ?, ?, V, ?, ?> context(K key);

    @Override
    @SuppressWarnings("unchecked")
    default boolean containsKey(Object key) {
        try (MapKeyContext<K, V> c = context((K) key)) {
            return c.containsKey();
        }
    }

    @Override
    default V put(K key, V value) {
        try (MapKeyContext<K, V> c = context(key)) {
            // We cannot read the previous value under read lock, because then we will need
            // to release the read lock -> acquire write lock, the value might be updated in
            // between, that will break ConcurrentMap.put() atomicity guarantee. So, we acquire
            // update lock from the start:
            c.updateLock().lock();
            V prevValue = prevValueOnPut(c);
            c.put(value);
            return prevValue;
        }
    }

    default V prevValueOnPut(MapKeyContext<K, V> context) {
        return context.getUsing(null);
    }

    @Override
    default V putIfAbsent(K key, V value) {
        checkValue(value);
        try (MapKeyContext<K, V> c = context(key)) {
            // putIfAbsent() shouldn't actually put most of the time,
            // so check if the key is present under read lock first:
            if (c.readLock().tryLock()) {
                // c.get() returns cached value, that might be unexpected by user,
                // so use getUsing(null) which surely creates a new value instance:
                V currentValue = c.getUsing(null);
                if (currentValue != null)
                    return currentValue;
                // Key is absent
                upgradeReadToUpdateLockWithUnlockingIfNeeded(c);
            }
            // Entry with this key might be put into the map before we acquired
            // update lock (exclusive) at any time, so even if we successfully upgraded
            // to update lock, we should check if the value is still absent again
            V currentValue = c.getUsing(null);
            if (currentValue != null)
                return currentValue;
            // Key is absent
            c.put(value);
            return null;
        }
    }

    void checkValue(V value);

    @Override
    @SuppressWarnings("unchecked")
    default V get(Object key) {
        return getUsing((K) key, null);
    }

    @Override
    default V getUsing(K key, V usingValue) {
        try (MapKeyContext<K, V> c = context(key)) {
            return c.getUsing(usingValue);
        }
    }

    @Override
    default V acquireUsing(K key, V usingValue) {
        try (VanillaContext<K, ?, ?, V, ?, ?> c = context(key)) {
            // acquireUsing() should just read an existing value most of the time,
            // so try to check if the key is already present under read lock first:
            if (c.readLock().tryLock()) {
                V value = c.getUsing(usingValue);
                if (value != null)
                    return value;
                // Key is absent
                upgradeReadToUpdateLockWithUnlockingIfNeeded(c);
            } else {
                c.updateLock().lock();
            }
            // Entry with this key might be put into the map before we acquired
            // update lock (exclusive) at any time, so even if we successfully upgraded
            // to update lock, we should check if the value is still absent again
            V value = c.getUsing(usingValue);
            if (value != null)
                return value;
            // Key is absent
            putDefaultValue(c);
            return c.getUsing(usingValue);
        }
    }

    default void upgradeReadToUpdateLockWithUnlockingIfNeeded(KeyContext c) {
        if (!c.updateLock().tryLock()) {
            c.readLock().unlock();
            c.updateLock().lock();
        }
    }

    void putDefaultValue(VanillaContext context);

    @Override
    default <R> R getMapped(K key, @NotNull SerializableFunction<? super V, R> function) {
        requireNonNull(function);
        try (MapKeyContext<K, V> c = context(key)) {
            return c.containsKey() ? function.apply(c.get()) : null;
        }
    }

    @Override
    default V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {
        requireNonNull(unaryOperator);
        try (MapKeyContext<K, V> c = context(key)) {
            // putMapped() should find a value, update & put most of the time,
            // so don't try to check key presence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            if (c.containsKey()) {
                V newValue = unaryOperator.update(c.get());
                c.put(newValue);
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

    @Override
    @SuppressWarnings("unchecked")
    default V remove(Object key) {
        try (MapKeyContext<K, V> c = context((K) key)) {
            // We cannot read the previous value under read lock, because then we will need
            // to release the read lock -> acquire write lock, the value might be updated in
            // between, that will break ConcurrentMap.remove() atomicity guarantee. So, we acquire
            // update lock from the start:
            c.updateLock().lock();
            V prevValue = prevValueOnRemove(c);
            c.remove();
            return prevValue;
        }
    }

    default V prevValueOnRemove(MapKeyContext<K, V> context) {
        return context.getUsing(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    default boolean remove(Object key, Object value) {
        if (value == null)
            return false; // CHM compatibility; General ChronicleMap policy is to throw NPE
        checkValue((V) value);
        try (MapKeyContext<K, V> c = context((K) key)) {
            // remove(key, value) should find the entry & remove most of the time,
            // so don't try to check key presence and value equivalence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            return c.containsKey() && c.valueEqualTo((V) value) && c.remove();
        }
    }

    @Override
    default V replace(K key, V value) {
        checkValue(value);
        try (MapKeyContext<K, V> c = context(key)) {
            // replace(key, value) should find the key & put the value most of the time,
            // so don't try to check key presence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            // 1. c.get() returns cached value, that might be unexpected by user,
            // so use getUsing(null) which surely creates a new value instance:
            // 2. Always "try to read" the previous value (not via if (c.containsKey()) {...
            // because BytesChronicleMap relies on it to write null to the output stream
            V prevValue = c.getUsing(null);
            if (prevValue != null)
                c.put(value);
            return prevValue;
        }
    }

    @Override
    default boolean replace(K key, V oldValue, V newValue) {
        checkValue(oldValue);
        checkValue(newValue);
        try (MapKeyContext<K, V> c = context(key)) {
            // replace(key, old, new) should find the entry & put new value most of the time,
            // so don't try to check key presence and value equivalence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            return c.containsKey() && c.valueEqualTo(oldValue) && c.put(newValue);
        }
    }

    int actualSegments();

    VanillaContext<K, ?, ?, V, ?, ?> mapContext();

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

    @Override
    default boolean forEachEntryWhile(final Predicate<? super MapKeyContext<K, V>> predicate) {
        boolean interrupt = false;
        iteration:
        try (VanillaContext<K, ?, ?, V, ?, ?> c = mapContext()) {
            ForEachWhilePredicate<K, V> hashLookupPredicate =
                    new ForEachWhilePredicate<>(c, predicate);
            for (int segmentIndex = actualSegments() - 1; segmentIndex >= 0; segmentIndex--) {
                c.segmentIndex = segmentIndex;
                c.forEachEntry = true;
                try {
                    c.updateLock().lock();
                    if (c.size() == 0)
                        continue;
                    c.initSegment();
                    c.hashLookup.forEachRemoving(hashLookupPredicate);
                    if (hashLookupPredicate.shouldBreak) {
                        interrupt = true;
                        break iteration;
                    }
                } finally {
                    c.forEachEntry = false;
                    c.closeSegmentIndex();
                }
            }
        }
        return !interrupt;
    }


    static class ForEachWhilePredicate<K, V> implements HashLookupIteration {
        private final VanillaContext<K, ?, ?, V, ?, ?> c;
        private final Predicate<? super MapKeyContext<K, V>> predicate;
        boolean shouldRemove = false;
        boolean shouldBreak = false;

        public ForEachWhilePredicate(VanillaContext<K, ?, ?, V, ?, ?> c,
                                     Predicate<? super MapKeyContext<K, V>> predicate) {
            this.c = c;
            this.predicate = predicate;
            shouldBreak = false;
        }

        @Override
        public void accept(long hash, long pos) {
            c.pos = pos;
            c.initKeyFromPos();
            try {
                if (!c.containsKey()) { // for replicated map
                    shouldRemove = false;
                    return;
                }
                shouldBreak = !predicate.test(c);
                c.closeKey0();
                // release all exclusive locks: possibly if context.remove() is performed
                // in the callback, or acquired manually
                while (c.writeLockCount > 0) {
                    c.writeLock().unlock();
                }
                if (!c.isUpdateLocked()) {
                    throw new IllegalStateException("Shouldn't release update lock " +
                            "inside forEachEntry() callback");
                }
                shouldRemove = c.searchState0() == DELETED;
            } finally {
                c.closeKeySearch();
            }
        }

        @Override
        public boolean remove() {
            return shouldRemove;
        }

        @Override
        public boolean continueIteration() {
            return !shouldBreak;
        }
    }
}
