/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Very inefficient (esp. if segments are large), but CORRECT implementation
 */
abstract class ChronicleMapIterator<K, V, E> implements Iterator<E>, Consumer<MapEntry<K, V>> {

    final AbstractChronicleMap<K, V> map;
    private final Thread ownerThread = Thread.currentThread();
    private final Queue<E> entryBuffer = new ArrayDeque<>();
    E returned;
    private int segmentIndex;

    ChronicleMapIterator(AbstractChronicleMap<K, V> map) {
        this.map = map;
        segmentIndex = map.segments() - 1;
    }

    private void checkSingleThreaded() {
        if (ownerThread != Thread.currentThread()) {
            throw new IllegalStateException(map.toIdentityString() +
                    ": Iterator should be accessed only from a single thread");
        }
    }

    private void fillEntryBuffer() {
        if (!entryBuffer.isEmpty())
            return;
        while (true) {
            if (segmentIndex < 0)
                return;

            try (MapSegmentContext<K, V, ?> c = map.segmentContext(segmentIndex)) {
                segmentIndex--;
                if (c.size() == 0)
                    continue;
                c.forEachSegmentEntry(this);
                return;
            }
        }
    }

    @Override
    public void accept(MapEntry<K, V> e) {
        entryBuffer.add(read(e));
    }

    abstract E read(MapEntry<K, V> entry);

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
            throw new NoSuchElementException(map.toIdentityString());
        return returned = e;
    }

    @Override
    public void remove() {
        checkSingleThreaded();
        if (returned == null)
            throw new IllegalStateException(map.toIdentityString());
        removeReturned();
        returned = null;
    }

    abstract void removeReturned();

    static class OfEntries<K, V> extends ChronicleMapIterator<K, V, Entry<K, V>> {

        OfEntries(AbstractChronicleMap<K, V> map) {
            super(map);
        }

        @Override
        Entry<K, V> read(MapEntry<K, V> entry) {
            K key = entry.key().getUsing(null);
            V value = entry.value().getUsing(null);
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
        K read(MapEntry<K, V> entry) {
            return entry.key().getUsing(null);
        }

        @Override
        void removeReturned() {
            map.remove(returned);
        }
    }
}
