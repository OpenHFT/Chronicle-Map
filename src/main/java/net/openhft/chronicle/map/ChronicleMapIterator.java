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

import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;

import java.util.*;

/**
 * Very inefficient (esp. if segments are large), but CORRECT implementation
 */
abstract class ChronicleMapIterator<K, V, E> implements Iterator<E>, EntryConsumer {

    final AbstractChronicleMap<K, V> map;
    private final Thread ownerThread = Thread.currentThread();
    private final Queue<E> entryBuffer = new ArrayDeque<>();
    private int segmentIndex;
    VanillaContext<K, ?, ?, V, ?, ?> context;
    E returned;

    ChronicleMapIterator(AbstractChronicleMap<K, V> map) {
        this.map = map;
        segmentIndex = map.actualSegments() - 1;
    }

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
            try (VanillaContext<K, ?, ?, V, ?, ?> c = map.mapContext()) {
                context = c;
                c.segmentIndex = segmentIndex;
                segmentIndex--;
                if (c.size() == 0)
                    continue;
                c.updateLock().lock();
                c.initSegment();
                c.hashLookup.forEach(this);
                return;
            } finally {
                context = null;
            }
        }
    }

    @Override
    public void accept(long hash, long pos) {
        context.pos = pos;
        context.initKeyFromPos();
        try {
            if (!context.containsKey()) // for replicated map
                return;
            entryBuffer.add(read());
        } finally {
            context.closeKeySearch();
        }
    }

    abstract E read();

    @Override
    public boolean hasNext() {
        checkSingleThreaded();
        fillEntryBuffer();
        return !entryBuffer.isEmpty();
    }

    @Override
    public E next() {
        checkSingleThreaded();
        fillEntryBuffer();
        E e;
        if ((e = entryBuffer.poll()) == null)
            throw new NoSuchElementException();
        return returned = e;
    }

    @Override
    public void remove() {
        checkSingleThreaded();
        if (returned == null)
            throw new IllegalStateException();
        removeReturned();
        returned = null;
    }

    abstract void removeReturned();

    static class OfEntries<K, V> extends ChronicleMapIterator<K, V, Map.Entry<K, V>> {

        OfEntries(AbstractChronicleMap<K, V> map) {
            super(map);
        }

        @Override
        Map.Entry<K, V> read() {
            K key = context.immutableKey();
            V value = context.getUsing(null);
            return new WriteThroughEntry<>(map, key, value);
        }

        @Override
        void removeReturned() {
            map.remove(returned.getKey(), returned.getValue());
        }
    }

    static class OfKeys<K, V> extends ChronicleMapIterator<K, V, K> {

        OfKeys(AbstractChronicleMap<K, V> map) {
            super(map);
        }

        @Override
        K read() {
            return context.immutableKey();
        }

        @Override
        void removeReturned() {
            map.remove(returned);
        }
    }
}
