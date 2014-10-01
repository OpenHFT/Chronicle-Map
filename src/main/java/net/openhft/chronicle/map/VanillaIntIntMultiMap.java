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

import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;

/**
 * Supports a simple interface for int -> int[] off heap.
 */
class VanillaIntIntMultiMap implements IntIntMultiMap {

    static int multiMapCapacity(int minCapacity) {
        if (minCapacity < 0)
            throw new IllegalArgumentException();
        int capacity = Maths.nextPower2(minCapacity, 16);
        if (((double) minCapacity) / capacity > 2./3.) {
            // multi map shouldn't be too dense
            capacity <<= 1;
        }
        return capacity;
    }

    private static final int ENTRY_SIZE = 8;
    private static final int ENTRY_SIZE_SHIFT = 3;
    private static final int UNSET_KEY = 0;
    private static final int HASH_INSTEAD_OF_UNSET_KEY = -1;
    private static final int UNSET_VALUE = Integer.MIN_VALUE;
    /**
     * hash is in 32 higher order bits, because in Intel's little-endian they are written first
     * in memory, and in memory we have keys and values in natural order: 4 bytes of k1, 4 bytes
     * of v1, 4 bytes of k2, ... and this is somehow compatible with previous version of this class,
     * where keys were written before values explicitly. However, this layout increases latency
     * of map operations by 1 clock cycle, because we always need to perform shift to obtain the key
     * between memory read and comparison with UNSET_KEY.
     */
    private static final long UNSET_ENTRY = Integer.MIN_VALUE & 0xFFFFFFFFL;
    final DirectBitSet positions;
    private final int capacity;
    private final int capacityMask;
    private final long capacityMask2;
    private final Bytes bytes;
    private int searchHash = -1;
    private long searchPos = -1L;

    public VanillaIntIntMultiMap(int minCapacity) {
        capacity = multiMapCapacity(minCapacity);
        capacityMask = capacity - 1;
        capacityMask2 = indexToPos(capacity - 1);
        bytes = DirectStore.allocateLazy(indexToPos(capacity)).bytes();
        positions = newPositions(capacity);
        clear();
    }

    public VanillaIntIntMultiMap(Bytes multiMapBytes, Bytes multiMapBitSetBytes) {
        capacity = (int) (multiMapBytes.capacity() / ENTRY_SIZE);
        assert capacity == Maths.nextPower2(capacity, 16);
        capacityMask = capacity - 1;
        capacityMask2 = indexToPos(capacity - 1);
        this.bytes = multiMapBytes;
        positions = new ATSDirectBitSet(multiMapBitSetBytes);
    }

    /**
     * @param minCapacity as in {@link #VanillaIntIntMultiMap(int)} constructor
     * @return size of {@link Bytes} to provide to {@link #VanillaIntIntMultiMap(Bytes, Bytes)}
     * constructor as the first argument
     */
    public static long sizeInBytes(int minCapacity) {
        return indexToPos(multiMapCapacity(minCapacity));
    }

    /**
     * @param minCapacity as in {@link #VanillaIntIntMultiMap(int)} constructor
     * @return size of {@link Bytes} to provide to {@link #VanillaIntIntMultiMap(Bytes, Bytes)}
     * constructor as the second argument
     */
    public static long sizeOfBitSetInBytes(int minCapacity) {
        return Math.max(multiMapCapacity(minCapacity), 64L) / 8L;
    }

    public static ATSDirectBitSet newPositions(int capacity) {
        // bit set size should be at least 1 native long (in bits)
        capacity = Math.max(capacity, 64);
        // no round-up, because capacity is already a power of 2
        int bitSetSizeInBytes = capacity / 8;
        return new ATSDirectBitSet(DirectStore.allocateLazy(bitSetSizeInBytes).bytes());
    }

    /**
     * Separate method because it is too easy to forget to cast to long before shifting.
     */
    private static long indexToPos(int index) {
        return ((long) index) << ENTRY_SIZE_SHIFT;
    }

    private static int maskUnsetKey(int key) {
        return key != UNSET_KEY ? key : HASH_INSTEAD_OF_UNSET_KEY;
    }

    @Override
    public void put(int key, int value) {
        key = maskUnsetKey(key);
        long pos = indexToPos(key & capacityMask);
        for (int i = 0; i <= capacityMask; i++) {
            long entry = bytes.readLong(pos);
            int hash2 = (int) (entry >> 32);
            if (hash2 == UNSET_KEY) {
                bytes.writeLong(pos, (((long) key) << 32) | (value & 0xFFFFFFFFL));
                positions.set(value);
                return;
            }
            if (hash2 == key) {
                int value2 = (int) entry;
                if (value2 == value)
                    return;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        throw new IllegalStateException(getClass().getSimpleName() + " is full");
    }

    @Override
    public boolean remove(int key, int value) {
        key = maskUnsetKey(key);
        long pos = indexToPos(key & capacityMask);
        long posToRemove = -1L;
        for (int i = 0; i <= capacityMask; i++) {
            long entry = bytes.readLong(pos);
            int hash2 = (int) (entry >> 32);
            if (hash2 == key) {
                int value2 = (int) entry;
                if (value2 == value) {
                    posToRemove = pos;
                    break;
                }
            } else if (hash2 == UNSET_KEY) {
                break;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        if (posToRemove < 0)
            return false;
        positions.clear(value);
        removePos(posToRemove);
        return true;
    }

    /////////////////////
    // Stateful methods

    @Override
    public boolean replace(int key, int oldValue, int newValue) {
        key = maskUnsetKey(key);
        long pos = indexToPos(key & capacityMask);
        for (int i = 0; i <= capacityMask; i++) {
            long entry = bytes.readLong(pos);
            int hash2 = (int) (entry >> 32);
            if (hash2 == key) {
                int value2 = (int) entry;
                if (value2 == oldValue) {
                    positions.clear(oldValue);
                    positions.set(newValue);
                    bytes.writeLong(pos, (((long) key) << 32) | (newValue & 0xFFFFFFFFL));
                    return true;
                }
            } else if (hash2 == UNSET_KEY) {
                break;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        return false;
    }

    private void removePos(long posToRemove) {
        long posToShift = posToRemove;
        for (int i = 0; i <= capacityMask; i++) {
            posToShift = (posToShift + ENTRY_SIZE) & capacityMask2;
            long entryToShift = bytes.readLong(posToShift);
            int hash = (int) (entryToShift >> 32);
            if (hash == UNSET_KEY)
                break;
            long insertPos = indexToPos(hash & capacityMask);
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

    @Override
    public int startSearch(int key) {
        key = maskUnsetKey(key);
        searchPos = indexToPos(key & capacityMask);
        return searchHash = key;
    }

    @Override
    public int nextPos() {
        long pos = searchPos;
        for (int i = 0; i < capacity; i++) {
            long entry = bytes.readLong(pos);
            int hash2 = (int) (entry >> 32);
            if (hash2 == UNSET_KEY) {
                searchPos = pos;
                return UNSET_VALUE;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
            if (hash2 == searchHash) {
                searchPos = pos;
                return (int) entry;
            }
        }
        // if return UNSET_VALUE, we have 2 cases in putAfterFailedSearch()
        throw new IllegalStateException(getClass().getSimpleName() + " is full");
    }

    @Override
    public void removePrevPos() {
        long prevPos = (searchPos - ENTRY_SIZE) & capacityMask2;
        long entry = bytes.readLong(prevPos);
        int value = (int) entry;
        positions.clear(value);
        removePos(prevPos);
    }

    @Override
    public void replacePrevPos(int newValue) {
        long prevPos = ((searchPos - ENTRY_SIZE) & capacityMask2);
        long oldEntry = bytes.readLong(prevPos);
        int oldValue = (int) oldEntry;
        positions.clear(oldValue);
        positions.set(newValue);
        // Don't need to overwrite searchHash, but we don't know our bytes
        // byte order, and can't determine offset of the value within entry.
        long newEntry = (((long) searchHash) << 32) | (newValue & 0xFFFFFFFFL);
        bytes.writeLong(prevPos, newEntry);
    }


    @Override
    public void putAfterFailedSearch(int value) {
        positions.set(value);
        long entry = (((long) searchHash) << 32) | (value & 0xFFFFFFFFL);
        bytes.writeLong(searchPos, entry);
    }

    public int getSearchHash() {
        return searchHash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (int i = 0, pos = 0; i < capacity; i++, pos += ENTRY_SIZE) {
            long entry = bytes.readLong(pos);
            int key = (int) (entry >> 32);
            int value = (int) entry;
            if (key != UNSET_KEY)
                sb.append(key).append('=').append(value).append(", ");
        }
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
            return sb.append(" }").toString();
        }
        return "{ }";
    }

    @Override
    public void forEach(EntryConsumer action) {
        for (int i = 0, pos = 0; i < capacity; i++, pos += ENTRY_SIZE) {
            long entry = bytes.readLong(pos);
            int key = (int) (entry >> 32);
            int value = (int) entry;
            if (key != UNSET_KEY) {
                action.accept(key, value);
            }
        }
    }

    @Override
    public DirectBitSet getPositions() {
        return positions;
    }

    @Override
    public void clear() {
        positions.clear();
        for (int pos = 0; pos < bytes.capacity(); pos += ENTRY_SIZE) {
            bytes.writeLong(pos, UNSET_ENTRY);
        }
    }
}
