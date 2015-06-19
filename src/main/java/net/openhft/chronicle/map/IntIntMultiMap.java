/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;

import static net.openhft.chronicle.map.MultiMapFactory.multiMapCapacity;
import static net.openhft.chronicle.map.MultiMapFactory.newPositions;
import static net.openhft.lang.Maths.isPowerOf2;

/**
 * Supports a simple interface for int -> int[] off heap.
 */
class IntIntMultiMap implements MultiMap {

    static final long MAX_CAPACITY = (1L << 32);

    private static final long ENTRY_SIZE = 8L;
    private static final int ENTRY_SIZE_SHIFT = 3;
    private static final long UNSET_KEY = 0L;
    private static final long MASK = 0xFFFFFFFFL;
    private static final long HASH_INSTEAD_OF_UNSET_KEY = MASK;
    private static final long UNSET_ENTRY = 0L;

    final DirectBitSet positions;
    private final long capacity;
    private final long capacityMask;
    private final long capacityMask2;
    private final Bytes bytes;

    public IntIntMultiMap(long minCapacity) {
        capacity = multiMapCapacity(minCapacity);
        capacityMask = capacity - 1L;
        capacityMask2 = indexToPos(capacityMask);
        bytes = DirectStore.allocateLazy(indexToPos(capacity)).bytes();
        positions = newPositions(capacity);
        clear();
    }

    public IntIntMultiMap(Bytes multiMapBytes, Bytes multiMapBitSetBytes) {
        capacity = multiMapBytes.capacity() / ENTRY_SIZE;
        assert isPowerOf2(capacity);
        assert (capacity / 2L) <= MAX_CAPACITY;
        capacityMask = capacity - 1L;
        capacityMask2 = indexToPos(capacityMask);
        this.bytes = multiMapBytes;
        positions = new ATSDirectBitSet(multiMapBitSetBytes);
    }

    /**
     * @param minCapacity as in {@link #IntIntMultiMap(long)} constructor
     * @return size of {@link Bytes} to provide to {@link #IntIntMultiMap(Bytes, Bytes)}
     * constructor as the first argument
     */
    public static long sizeInBytes(long minCapacity) {
        return indexToPos(multiMapCapacity(minCapacity));
    }

    private static long indexToPos(long index) {
        return index << ENTRY_SIZE_SHIFT;
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

    private static long key(long entry) {
        return entry >>> 32;
    }

    private static long value(long entry) {
        return entry & MASK;
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
    public void putPosition(long value) {
        checkValueForPut(value);
        positions.set(value);
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
    public void removePosition(long value) {
        checkValueForRemove(value);
        positions.clear(value);
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
    public void startSearch(long key, SearchState searchStateToReuse) {
        key = maskUnsetKey(key);
        searchStateToReuse.searchPos = pos(key);
        searchStateToReuse.searchHash = key;
        searchStateToReuse.putAfterFailedSearch = false;
    }

    @Override
    public long nextPos(SearchState searchState) {
        long pos = searchState.searchPos;
        for(long count = capacity; count > 0; count--) {
            long entry = bytes.readLong(pos);
            if (entry == UNSET_ENTRY) {
                searchState.searchPos = pos;
                return -1L;
            }
            pos = step(pos);
            if (key(entry) == searchState.searchHash) {
                searchState.searchPos = pos;
                return value(entry);
            }
        }
        throw new IllegalStateException();
    }

    @Override
    public void removePrevPos(SearchState searchState) {
        long prevPos = stepBack(searchState.searchPos);
        long entry = bytes.readLong(prevPos);
        positions.clear(value(entry));
        removePos(prevPos);
    }

    @Override
    public void replacePrevPos(SearchState searchState, long newValue,
                               boolean oldValueInPositions) {
        checkValueForPut(newValue);
        long prevPos = searchState.searchPos;
        if (!searchState.putAfterFailedSearch)
            prevPos = stepBack(prevPos);
        if (oldValueInPositions) {
            long oldEntry = bytes.readLong(prevPos);
            long oldValue = value(oldEntry);
            checkValueForRemove(oldValue);
            positions.clear(oldValue);
        }
        positions.set(newValue);
        bytes.writeLong(prevPos, entry(searchState.searchHash, newValue));
    }

    @Override
    public void putAfterFailedSearch(SearchState searchState, long value) {
        checkValueForPut(value);
        positions.set(value);
        bytes.writeLong(searchState.searchPos, entry(searchState.searchHash, value));
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
