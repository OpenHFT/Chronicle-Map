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

import static net.openhft.lang.Maths.isPowerOf2;

/**
 * Supports a simple interface for int -> int[] off heap.
 */
class Int24Int24MultiMap implements MultiMap {

    static final long MAX_CAPACITY = (1L << 24);

    private static final long ENTRY_SIZE = 6L;
    private static final int UNSET_KEY = 0;
    private static final long MASK = 0xFFFFFFL;
    private static final long HASH_INSTEAD_OF_UNSET_KEY = MASK;
    private static final long UNSET_ENTRY = 0L;

    final DirectBitSet positions;
    private final int capacity;
    private final long capacityMask;
    private final long capacityInBytes;
    private final Bytes bytes;

    public Int24Int24MultiMap(long minCapacity) {
        long capacity = MultiMapFactory.multiMapCapacity(minCapacity);
        this.capacity = (int) capacity;
        capacityMask = capacity - 1L;
        capacityInBytes = indexToPos(capacity);
        bytes = DirectStore.allocateLazy(indexToPos(capacity)).bytes();
        positions = MultiMapFactory.newPositions(capacity);
        clear();
    }

    public Int24Int24MultiMap(Bytes multiMapBytes, Bytes multiMapBitSetBytes) {
        long capacity = multiMapBytes.capacity() / ENTRY_SIZE;
        assert isPowerOf2(capacity);
        assert (capacity / 2L) <= MAX_CAPACITY;
        this.capacity = (int) capacity;
        capacityMask = capacity - 1L;
        capacityInBytes = indexToPos(capacity);
        this.bytes = multiMapBytes;
        positions = new ATSDirectBitSet(multiMapBitSetBytes);
    }

    /**
     * @param minCapacity as in {@link #Int24Int24MultiMap(long)} constructor
     * @return size of {@link Bytes} to provide to {@link #Int24Int24MultiMap(Bytes, Bytes)}
     * constructor as the first argument
     */
    public static long sizeInBytes(long minCapacity) {
        return indexToPos(MultiMapFactory.multiMapCapacity(minCapacity));
    }

    private static long indexToPos(long index) {
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

    private static long key(long entry) {
        return entry >>> 24;
    }

    private static long value(long entry) {
        return entry & MASK;
    }

    private static long entry(long key, long value) {
        return (key << 24) | (value & MASK);
    }

    private long pos(long key) {
        return indexToPos(key & capacityMask);
    }

    private long step(long pos) {
        long t;
        return (t = (pos += ENTRY_SIZE) - capacityInBytes) < 0L ? pos : t;
    }

    private long stepBack(long pos) {
        return (pos -= ENTRY_SIZE) >= 0L ? pos : pos + capacityInBytes;
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
            long entry = bytes.readInt48(pos);
            if (entry == UNSET_ENTRY) {
                bytes.writeInt48(pos, entry(key, value));
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
        for (long pos = pos(key); ; pos = step(pos)) {
            long entry = bytes.readInt48(pos);
            if (key(entry) == key && value(entry) == oldValue) {
                positions.clear(oldValue);
                positions.set(newValue);
                bytes.writeInt48(pos, entry(key, newValue));
                return;
            }
        }
    }

    private void removePos(long posToRemove) {
        long posToShift = posToRemove;
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
    public void startSearch(long key, SearchState searchStateToReuse) {
        key = maskUnsetKey(key);
        searchStateToReuse.searchPos = pos(key);
        searchStateToReuse.searchHash = key;
        searchStateToReuse.putAfterFailedSearch = false;
    }

    @Override
    public long nextPos(SearchState searchState) {
        long pos = searchState.searchPos;
        while (true) {
            long entry = bytes.readInt48(pos);
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
    }

    @Override
    public void removePrevPos(SearchState searchState) {
        long prevPos = stepBack(searchState.searchPos);
        long entry = bytes.readInt48(prevPos);
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
            long oldEntry = bytes.readInt48(prevPos);
            long oldValue = value(oldEntry);
            checkValueForRemove(oldValue);
            positions.clear(oldValue);
        }
        positions.set(newValue);
        bytes.writeInt48(prevPos, entry(searchState.searchHash, newValue));
    }

    @Override
    public void putAfterFailedSearch(SearchState searchState, long value) {
        checkValueForPut(value);
        positions.set(value);
        bytes.writeInt48(searchState.searchPos, entry(searchState.searchHash, value));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        long pos = 0L;
        for (int i = 0; i < capacity; i++, pos += ENTRY_SIZE) {
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
        long pos = 0L;
        for (int i = 0; i < capacity; i++, pos += ENTRY_SIZE) {
            long entry = bytes.readInt48(pos);
            if (entry != UNSET_ENTRY)
                action.accept(key(entry), value(entry));
        }
    }

    public long size() {
        long pos = 0;
        int size = 0;
        for (int count = capacity; count > 0; count--) {
            long entry = bytes.readInt48(pos);
            pos = step(pos);
            if (entry != UNSET_ENTRY)
                size++;
        }
        return size;
    }

    @Override
    public long capacity() {
        return capacity;
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
