/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;

import static net.openhft.lang.Maths.isPowerOf2;

/**
 * Supports a simple interface for int -> int[] off heap.
 */
class VanillaIntIntMultiMap implements MultiMap {

    static final long MAX_CAPACITY = (1L << 32);

    static long multiMapCapacity(long minCapacity) {
        if (minCapacity < 0L)
            throw new IllegalArgumentException("minCapacity should be positive");
        long capacity = Maths.nextPower2(minCapacity, 16L);
        if (((double) minCapacity) / capacity > 2./3.) {
            // multi map shouldn't be too dense
            capacity <<= 1L;
        }
        return capacity;
    }

    private static final long ENTRY_SIZE = 8L;
    private static final int ENTRY_SIZE_SHIFT = 3;
    private static final long UNSET_KEY = 0L;
    private static final long HASH_INSTEAD_OF_UNSET_KEY = 0xFFFFFFFFL;
    private static final long UNSET_ENTRY = 0L;

    final DirectBitSet positions;
    private final long capacity;
    private final long capacityMask;
    private final long capacityMask2;
    private final Bytes bytes;

    private long searchHash = -1L;
    private long searchPos = -1L;

    public VanillaIntIntMultiMap(long minCapacity) {
        capacity = multiMapCapacity(minCapacity);
        capacityMask = capacity - 1L;
        capacityMask2 = indexToPos(capacity - 1L);
        bytes = DirectStore.allocateLazy(indexToPos(capacity)).bytes();
        positions = newPositions(capacity);
        clear();
    }

    public VanillaIntIntMultiMap(Bytes multiMapBytes, Bytes multiMapBitSetBytes) {
        capacity = multiMapBytes.capacity() / ENTRY_SIZE;
        assert capacity == Maths.nextPower2(capacity, 16L);
        capacityMask = capacity - 1L;
        capacityMask2 = indexToPos(capacity - 1L);
        this.bytes = multiMapBytes;
        positions = new ATSDirectBitSet(multiMapBitSetBytes);
    }

    /**
     * @param minCapacity as in {@link #VanillaIntIntMultiMap(long)} constructor
     * @return size of {@link Bytes} to provide to {@link #VanillaIntIntMultiMap(Bytes, Bytes)}
     * constructor as the first argument
     */
    public static long sizeInBytes(long minCapacity) {
        return indexToPos(multiMapCapacity(minCapacity));
    }

    /**
     * @param minCapacity as in {@link #VanillaIntIntMultiMap(long)} constructor
     * @return size of {@link Bytes} to provide to {@link #VanillaIntIntMultiMap(Bytes, Bytes)}
     * constructor as the second argument
     */
    public static long sizeOfBitSetInBytes(long minCapacity) {
        return Math.max(multiMapCapacity(minCapacity), 64L) / 8L;
    }

    public static ATSDirectBitSet newPositions(long capacity) {
        if (!isPowerOf2(capacity))
            throw new AssertionError("capacity should be a power of 2");
        // bit set size should be at least 1 native long (in bits)
        capacity = Math.max(capacity, 64L);
        // no round-up, because capacity is already a power of 2
        long bitSetSizeInBytes = capacity / 8L;
        return new ATSDirectBitSet(DirectStore.allocateLazy(bitSetSizeInBytes).bytes());
    }

    private static long indexToPos(long index) {
        return index << ENTRY_SIZE_SHIFT;
    }

    private static long maskUnsetKey(long key) {
        return (key &= 0xFFFFFFFFL) != UNSET_KEY ? key : HASH_INSTEAD_OF_UNSET_KEY;
    }

    private void checkValueForPut(long value) {
        assert (value & ~0xFFFFFFFFL) == 0L : "Value out of range, was " + value;
        assert positions.isClear(value) : "Shouldn't put existing value";
    }

    private void checkValueForRemove(long value) {
        assert (value & ~0xFFFFFFFFL) == 0L : "Value out of range, was " + value;
        assert positions.isSet(value) : "Shouldn't remove absent value";
    }

    private static long key(long entry) {
        return entry >>> 32;
    }

    private static long value(long entry) {
        return entry & 0xFFFFFFFFL;
    }

    private static long entry(long key, long value) {
        return (key << 32) | value;
    }

    private long pos(long key) {
        return indexToPos(key & capacityMask);
    }

    private long step(long pos) {
        return (pos + ENTRY_SIZE) & capacityMask2;
    }

    private long stepBack(long pos) {
        return (pos - ENTRY_SIZE) & capacityMask2;
    }

    @Override
    public void put(long key, long value) {
        key = maskUnsetKey(key);
        checkValueForPut(value);
        for (long pos = pos(key); ; pos = step(pos)) {
            long entry = bytes.readLong(pos);
            if (entry == UNSET_ENTRY) {
                bytes.writeLong(pos, entry(key, value));
                positions.set(value);
                return;
            }
        }
    }

    @Override
    public void remove(long key, long value) {
        key = maskUnsetKey(key);
        checkValueForRemove(value);
        long posToRemove;
        for (long pos = pos(key); ; pos = step(pos)) {
            long entry = bytes.readLong(pos);
            if (key(entry) == key && value(entry) == value) {
                posToRemove = pos;
                break;
            }
        }
        positions.clear(value);
        removePos(posToRemove);
    }

    @Override
    public void replace(long key, long oldValue, long newValue) {
        key = maskUnsetKey(key);
        checkValueForRemove(oldValue);
        checkValueForPut(newValue);
        for (long pos = pos(key); ; pos = step(pos)) {
            long entry = bytes.readLong(pos);
            if (key(entry) == key && value(entry) == oldValue) {
                positions.clear(oldValue);
                positions.set(newValue);
                bytes.writeLong(pos, entry(key, newValue));
                return;
            }
        }
    }

    private void removePos(long posToRemove) {
        long posToShift = posToRemove;
        while (true) {
            posToShift = step(posToShift);
            long entryToShift = bytes.readLong(posToShift);
            if (entryToShift == UNSET_ENTRY)
                break;
            long insertPos = pos(key(entryToShift));
            // the following condition essentially means circular permutations
            // of three (r = posToRemove, s = posToShift, i = insertPos)
            // positions are accepted:
            // [...i..r...s.] or
            // [...r..s...i.] or
            // [...s..i...r.]
            boolean cond1 = insertPos <= posToRemove;
            boolean cond2 = posToRemove <= posToShift;
            if ((cond1 && cond2) ||
                    // chain wrapped around capacity
                    (posToShift < insertPos && (cond1 || cond2))) {
                bytes.writeLong(posToRemove, entryToShift);
                posToRemove = posToShift;
            }
        }
        bytes.writeLong(posToRemove, UNSET_ENTRY);
    }

    /////////////////////
    // Stateful methods

    @Override
    public void startSearch(long key) {
        key = maskUnsetKey(key);
        searchPos = pos(key);
        searchHash = key;
    }

    @Override
    public long nextPos() {
        long pos = searchPos;
        while (true) {
            long entry = bytes.readLong(pos);
            if (entry == UNSET_ENTRY) {
                searchPos = pos;
                return -1L;
            }
            pos = step(pos);
            if (key(entry) == searchHash) {
                searchPos = pos;
                return value(entry);
            }
        }
    }

    @Override
    public void removePrevPos() {
        long prevPos = stepBack(searchPos);
        long entry = bytes.readLong(prevPos);
        positions.clear(value(entry));
        removePos(prevPos);
    }

    @Override
    public void replacePrevPos(long newValue) {
        checkValueForPut(newValue);
        long prevPos = stepBack(searchPos);
        long oldEntry = bytes.readLong(prevPos);
        long oldValue = value(oldEntry);
        positions.clear(oldValue);
        positions.set(newValue);
        bytes.writeLong(prevPos, entry(searchHash, newValue));
    }


    @Override
    public void putAfterFailedSearch(long value) {
        checkValueForPut(value);
        positions.set(value);
        bytes.writeLong(searchPos, entry(searchHash, value));
    }

    public long getSearchHash() {
        return searchHash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (long i = 0L, pos = 0L; i < capacity; i++, pos += ENTRY_SIZE) {
            long entry = bytes.readLong(pos);
            if (entry != UNSET_ENTRY)
                sb.append(key(entry)).append('=').append(value(entry)).append(", ");
        }
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
            return sb.append(" }").toString();
        }
        return "{ }";
    }

    @Override
    public void forEach(EntryConsumer action) {
        for (long i = 0L, pos = 0L; i < capacity; i++, pos += ENTRY_SIZE) {
            long entry = bytes.readLong(pos);
            if (entry != UNSET_ENTRY)
                action.accept(key(entry), value(entry));
        }
    }

    @Override
    public DirectBitSet getPositions() {
        return positions;
    }

    @Override
    public void clear() {
        positions.clear();
        bytes.zeroOut();
    }
}
