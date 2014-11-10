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

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;

import static net.openhft.lang.Maths.isPowerOf2;

/**
 * Supports a simple interface for int -> int[] off heap.
 */
class Int24Int24MultiMap implements MultiMap {

    static final int MAX_CAPACITY = (1 << 24);

    private static final int ENTRY_SIZE = 6;
    //    private static final int ENTRY_SIZE_SHIFT = 3;
    private static final int UNSET_KEY = 0;
    private static final int MASK = 0xFFFFFF;
    private static final int HASH_INSTEAD_OF_UNSET_KEY = MASK;
    private static final long UNSET_ENTRY = 0L;

    final DirectBitSet positions;
    private final int capacity;
    private final int capacityMask;
    private final int capacityMod2;
    private final Bytes bytes;

    private int searchHash = -1;
    private int searchPos = -1;

    public Int24Int24MultiMap(long minCapacity) {
        capacity = (int) IntIntMultiMap.multiMapCapacity(minCapacity);
        capacityMask = capacity - 1;
        capacityMod2 = indexToPos(capacity - 1);
        bytes = DirectStore.allocateLazy(indexToPos(capacity)).bytes();
        positions = newPositions(capacity);
        clear();
    }

    public Int24Int24MultiMap(Bytes multiMapBytes, Bytes multiMapBitSetBytes) {
        capacity = (int) Math.min(MAX_CAPACITY, (multiMapBytes.capacity() / ENTRY_SIZE));
        assert capacity <= MAX_CAPACITY;
        capacityMask = capacity - 1;
        capacityMod2 = indexToPos(capacity - 1);
        this.bytes = multiMapBytes;
        positions = new ATSDirectBitSet(multiMapBitSetBytes);
    }

    /**
     * @param minCapacity as in {@link #Int24Int24MultiMap(long)} constructor
     * @return size of {@link Bytes} to provide to {@link #Int24Int24MultiMap(Bytes, Bytes)}
     * constructor as the first argument
     */
    public static long sizeInBytes(long minCapacity) {
        return indexToPos((int) IntIntMultiMap.multiMapCapacity(minCapacity));
    }

    /**
     * @param minCapacity as in {@link #Int24Int24MultiMap(long)} constructor
     * @return size of {@link Bytes} to provide to {@link #Int24Int24MultiMap(Bytes, Bytes)}
     * constructor as the second argument
     */
    public static long sizeOfBitSetInBytes(long minCapacity) {
        return (int) (Math.max(IntIntMultiMap.multiMapCapacity(minCapacity), 64L) * 6L + 63) / 64;
    }

    public static ATSDirectBitSet newPositions(int capacity) {
        if (!isPowerOf2(capacity))
            throw new AssertionError("capacity should be a power of 2");
        // bit set size should be at least 1 native long (in bits)
        capacity = Math.max(capacity, 64);
        // no round-up, because capacity is already a power of 2
        int bitSetSizeInBytes = capacity / 8;
        return new ATSDirectBitSet(DirectStore.allocateLazy(bitSetSizeInBytes).bytes());
    }

    private static int indexToPos(int index) {
        return index * ENTRY_SIZE;
    }

    private static long maskUnsetKey(long key) {
        return (key &= MASK) != UNSET_KEY ? key : HASH_INSTEAD_OF_UNSET_KEY;
    }

    private void checkValueForPut(long value) {
        assert (value & ~MASK) == 0L : "Value out of range, was " + value;
        assert positions.isClear(value) : "Shouldn't put existing value";
    }

    private void checkValueForRemove(long value) {
        assert (value & ~MASK) == 0L : "Value out of range, was " + value;
        assert positions.isSet(value) : "Shouldn't remove absent value";
    }

    private static int key(long entry) {
        return (int) (entry >>> 24);
    }

    private static int value(long entry) {
        return (int) (entry & MASK);
    }

    private static long entry(long key, long value) {
        return (key << 24) | (value & MASK);
    }

    private int pos(int key) {
        return indexToPos(key & capacityMask);
    }

    private int step(int pos) {
        return (pos + ENTRY_SIZE) % capacityMod2;
    }

    private int stepBack(int pos) {
        return (pos - ENTRY_SIZE) % capacityMod2;
    }

    @Override
    public void put(long key, long value) {
        key = maskUnsetKey(key);
        checkValueForPut(value);
        for (int pos = pos((int) key); ; pos = step(pos)) {
            long entry = bytes.readInt48(pos);
            if (entry == UNSET_ENTRY) {
                bytes.writeInt48(pos, entry(key, value));
                positions.set(value);
                return;
            }
        }
    }

    @Override
    public void remove(long key, long value) {
        key = maskUnsetKey(key);
        checkValueForRemove(value);
        int posToRemove;
        for (int pos = pos((int) key); ; pos = step(pos)) {
            long entry = bytes.readInt48(pos);
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
        for (int pos = pos((int) key); ; pos = step(pos)) {
            long entry = bytes.readInt48(pos);
            if (key(entry) == key && value(entry) == oldValue) {
                positions.clear(oldValue);
                positions.set(newValue);
                bytes.writeInt48(pos, entry(key, newValue));
                return;
            }
        }
    }

    private void removePos(int posToRemove) {
        int posToShift = posToRemove;
        while (true) {
            posToShift = step(posToShift);
            long entryToShift = bytes.readInt48(posToShift);
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
                bytes.writeInt48(posToRemove, entryToShift);
                posToRemove = posToShift;
            }
        }
        bytes.writeInt48(posToRemove, UNSET_ENTRY);
    }

    /////////////////////
    // Stateful methods

    @Override
    public void startSearch(long key) {
        key = maskUnsetKey(key);
        searchPos = pos((int) key);
        searchHash = (int) key;
    }

    @Override
    public long nextPos() {
        int pos = searchPos;
        while (true) {
            long entry = bytes.readInt48(pos);
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
        int prevPos = stepBack(searchPos);
        long entry = bytes.readInt48(prevPos);
        positions.clear(value(entry));
        removePos(prevPos);
    }

    @Override
    public void replacePrevPos(long newValue) {
        checkValueForPut(newValue);
        long prevPos = stepBack(searchPos);
        long oldEntry = bytes.readInt48(prevPos);
        long oldValue = value(oldEntry);
        positions.clear(oldValue);
        positions.set(newValue);
        bytes.writeInt48(prevPos, entry(searchHash, newValue));
    }


    @Override
    public void putAfterFailedSearch(long value) {
        checkValueForPut(value);
        positions.set(value);
        bytes.writeInt48(searchPos, entry(searchHash, value));
    }

    public long getSearchHash() {
        return searchHash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (long i = 0L, pos = 0L; i < capacity; i++, pos += ENTRY_SIZE) {
            long entry = bytes.readInt48(pos);
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
            long entry = bytes.readInt48(pos);
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
