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
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static java.util.Collections.emptyList;

abstract class AbstractChronicleMap<K, V> extends AbstractMap<K, V>
        implements ChronicleMap<K, V>, Serializable {
    private static final long serialVersionUID = 0L;

    transient Set<Map.Entry<K, V>> entrySet;

    @Override
    public abstract VanillaContext<K, ?, ?, V, ?, ?> context(K key);

    @Override
    @SuppressWarnings("unchecked")
    public final boolean containsKey(Object key) {
        try (MapKeyContext<V> c = context((K) key)) {
            return c.containsKey();
        }
    }

    @Override
    public final V put(K key, V value) {
        try (MapKeyContext<V> c = context(key)) {
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

    V prevValueOnPut(MapKeyContext<V> context) {
        return context.getUsing(null);
    }

    @Override
    public final V putIfAbsent(K key, V value) {
        try (MapKeyContext<V> c = context(key)) {
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

    @Override
    @SuppressWarnings("unchecked")
    public final V get(Object key) {
        return getUsing((K) key, null);
    }

    @Override
    public final V getUsing(K key, V usingValue) {
        try (MapKeyContext<V> c = context(key)) {
            return c.getUsing(usingValue);
        }
    }

    @Override
    public final V acquireUsing(K key, V usingValue) {
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

    void upgradeReadToUpdateLockWithUnlockingIfNeeded(KeyContext c) {
        if (!c.updateLock().tryLock()) {
            c.readLock().unlock();
            c.updateLock().lock();
        }
    }

    abstract void putDefaultValue(VanillaContext context);

    @Override
    public <R> R getMapped(K key, @NotNull Function<? super V, R> function) {
        try (MapKeyContext<V> c = context(key)) {
            return c.containsKey() ? function.apply(c.get()) : null;
        }
    }

    @Override
    public V putMapped(K key, @NotNull UnaryOperator<V> unaryOperator) {
        try (MapKeyContext<V> c = context(key)) {
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
    public synchronized void getAll(File toFile) throws IOException {
        JsonSerializer.getAll(toFile, this, emptyList());
    }

    @Override
    public synchronized void putAll(File fromFile) throws IOException {
        JsonSerializer.putAll(fromFile, this, emptyList());
    }

    @Override
    @SuppressWarnings("unchecked")
    public final V remove(Object key) {
        try (MapKeyContext<V> c = context((K) key)) {
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

    V prevValueOnRemove(MapKeyContext<V> context) {
        return context.getUsing(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final boolean remove(Object key, Object value) {
        if (value == null)
            return false; // CHM compatibility; General ChronicleMap policy is to throw NPE
        try (MapKeyContext<V> c = context((K) key)) {
            // remove(key, value) should find the entry & remove most of the time,
            // so don't try to check key presence and value equivalence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            return c.containsKey() && c.valueEqualTo((V) value) && c.remove();
        }
    }

    @Override
    public final V replace(K key, V value) {
        try (MapKeyContext<V> c = context(key)) {
            // replace(key, value) should find the key & put the value most of the time,
            // so don't try to check key presence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            if (c.containsKey()) {
                // c.get() returns cached value, that might be unexpected by user,
                // so use getUsing(null) which surely creates a new value instance:
                V prevValue = c.getUsing(null);
                c.put(value);
                return prevValue;
            } else {
                return null;
            }
        }
    }

    @Override
    public final boolean replace(K key, V oldValue, V newValue) {
        try (MapKeyContext<V> c = context(key)) {
            // replace(key, old, new) should find the entry & put new value most of the time,
            // so don't try to check key presence and value equivalence under read lock first,
            // as in putIfAbsent()/acquireUsing(), start with update lock:
            c.updateLock().lock();
            return c.containsKey() && c.valueEqualTo(oldValue) && c.put(newValue);
        }
    }

    abstract int actualSegments();

    abstract VanillaContext<K, ?, ?, V, ?, ?> rawContext();

    abstract VanillaContext<K, ?, ?, V, ?, ?> mapContext();

    @NotNull
    @Override
    public final Set<Entry<K, V>> entrySet() {
        return (entrySet != null) ? entrySet : (entrySet = newEntrySet());
    }

    Set<Entry<K, V>> newEntrySet() {
        return new EntrySet();
    }


    class EntryIterator implements Iterator<Entry<K, V>>, HashLookup.EntryConsumer {
        /**
         * Very inefficient (esp. if segments are large), but CORRECT implementation
         * TODO map.forEachEntry(Consumer<MapKeyContext<V>>)
         */
        private final Thread ownerThread = Thread.currentThread();
        private int segmentIndex = actualSegments() - 1;
        private final Queue<Entry<K, V>> entryBuffer = new ArrayDeque<>();
        private VanillaContext<K, ?, ?, V, ?, ?> context;
        private Entry<K, V> returned;

        private void checkSingleThreaded() {
            if (ownerThread != Thread.currentThread()) {
                throw new IllegalStateException(
                        "Iterator should be accessed only from a single thread");
            }
        }

        private void fillEntryBuffer() {
            if (!entryBuffer.isEmpty())
                return;
            while (true) {
                if (segmentIndex < 0)
                    return;
                try (VanillaContext<K, ?, ?, V, ?, ?> c = mapContext()) {
                    context = c;
                    c.segmentIndex = segmentIndex;
                    segmentIndex--;
                    if (c.size() == 0)
                        continue;
                    c.updateLock().lock();
                    c.initSegment();
                    c.hashLookup.forEach(this);
                } finally {
                    context = null;
                }
            }
        }

        @Override
        public void accept(long key, long pos) {
            context.pos = pos;
            context.initKeyFromPos();
            if (!context.containsKey()) // for replicated map
                return;
            entryBuffer.add(new WriteThroughEntry(context.immutableKey(), context.getUsing(null)));
        }

        @Override
        public boolean hasNext() {
            checkSingleThreaded();
            fillEntryBuffer();
            return !entryBuffer.isEmpty();
        }

        @Override
        public Entry<K, V> next() {
            checkSingleThreaded();
            fillEntryBuffer();
            if ((returned = entryBuffer.poll()) == null)
                throw new NoSuchElementException();
            return returned;
        }

        @Override
        public void remove() {
            checkSingleThreaded();
            if (returned == null)
                throw new IllegalStateException();
            AbstractChronicleMap.this.remove(returned.getKey(), returned.getValue());
            returned = null;
        }
    }

    class WriteThroughEntry extends AbstractMap.SimpleEntry<K, V> {

        public WriteThroughEntry(K key, V value) {
            super(key, value);
        }

        @Override
        public V setValue(V value) {
            try (MapKeyContext<V> c = AbstractChronicleMap.this.context(getKey())) {
                c.put(value);
            }
            return super.setValue(value);
        }
    }

    class EntrySet extends AbstractSet<Entry<K, V>> {
        @NotNull
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        public final boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            try {
                V v = AbstractChronicleMap.this.get(e.getKey());
                return v != null && v.equals(e.getValue());
            } catch (ClassCastException ex) {
                return false;
            } catch (NullPointerException ex) {
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
                return AbstractChronicleMap.this.remove(key, value);
            } catch (ClassCastException ex) {
                return false;
            } catch (NullPointerException ex) {
                return false;
            }
        }

        public final int size() {
            return AbstractChronicleMap.this.size();
        }

        public final boolean isEmpty() {
            return AbstractChronicleMap.this.isEmpty();
        }

        public final void clear() {
            AbstractChronicleMap.this.clear();
        }
    }
}
