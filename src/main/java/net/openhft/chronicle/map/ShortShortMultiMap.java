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
class ShortShortMultiMap implements MultiMap {

    static final long MAX_CAPACITY = (1L << 16);

    private static final int ENTRY_SIZE = 4;
    private static final int ENTRY_SIZE_SHIFT = 2;
    private static final int UNSET_KEY = 0;
    private static final long MASK = 0xFFFFL;
    private static final long HASH_INSTEAD_OF_UNSET_KEY = MASK;
    private static final int UNSET_ENTRY = 0;
    private final int capacity;
    private final int capacityMask;
    private final int capacityMask2;
    private final Bytes bytes;
    private DirectBitSet positions;

    public ShortShortMultiMap(long minCapacity) {
        assert minCapacity <= MAX_CAPACITY;
        long capacity = multiMapCapacity(minCapacity);
        assert isPowerOf2(capacity);
        this.capacity = (int) capacity;
        capacityMask = (int) (capacity - 1L);
        capacityMask2 = (int) (capacityMask * ENTRY_SIZE);
        bytes = DirectStore.allocateLazy(capacity * ENTRY_SIZE).bytes();
        positions = newPositions(capacity);
        clear();
    }

    public ShortShortMultiMap(Bytes multiMapBytes, Bytes multiMapBitSetBytes) {
        long capacity = multiMapBytes.capacity() / ENTRY_SIZE;
        assert isPowerOf2(capacity);
        assert (capacity / 2L) <= MAX_CAPACITY;
        this.capacity = (int) capacity;
        capacityMask = (int) (capacity - 1L);
        capacityMask2 = (int) (capacityMask * ENTRY_SIZE);
        this.bytes = multiMapBytes;
        positions = ATSDirectBitSet.wrap(multiMapBitSetBytes);
    }

    /**
     * @param minCapacity as in {@link #ShortShortMultiMap(long)} constructor
     * @return size of {@link Bytes} to provide to {@link #ShortShortMultiMap(Bytes, Bytes)}
     * constructor as the first argument
     */
    public static long sizeInBytes(long minCapacity) {
        return multiMapCapacity(minCapacity) * ENTRY_SIZE;
    }

    private static long maskUnsetKey(long key) {
        return (key &= MASK) != UNSET_KEY ? key : HASH_INSTEAD_OF_UNSET_KEY;
    }

    private static int entry(long key, long value) {
        return (int) ((key << 16) | value);
    }

    private static long key(int entry) {
        return (long) (entry >>> 16);
    }

    private static long value(int entry) {
        return (long) (entry & 0xFFFF);
    }

    private void checkValueForPut(long value) {
        assert (value & ~MASK) == 0L : "Value out of range, was " + value;
        assert positions.isClear(value) : "Shouldn't put existing value";
    }

    private void checkValueForRemove(long value) {
        assert (value & ~MASK) == 0L : "Value out of range, was " + value;
        assert positions.isSet(value) : "Shouldn't remove absent value";
    }

    private int step(int pos) {
        return (pos + ENTRY_SIZE) & capacityMask2;
    }

    private long stepBack(long pos) {
        return (pos - ENTRY_SIZE) & capacityMask2;
    }

    private int pos(long key) {
        return ((int) key & capacityMask) << ENTRY_SIZE_SHIFT;
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
        for (int pos = pos(key); ; pos = step(pos)) {
            int entry = bytes.readInt(pos);
            if (entry == UNSET_ENTRY) {
                bytes.writeInt(pos, entry(key, value));
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
        int limit = capacity;
        for (int pos = pos(key); ; pos = step(pos)) {
            int entry = bytes.readInt(pos);
            if (key(entry) == key && value(entry) == value) {
                posToRemove = pos;
                break;
            }
            if (limit-- < 0)
                throw new IllegalStateException();
        }
        positions.clear(value);
        removePos(posToRemove);
    }

    @Override
    public void replace(long key, long oldValue, long newValue) {
        key = maskUnsetKey(key);
        checkValueForRemove(oldValue);
        checkValueForPut(newValue);
        for (int pos = pos(key); ; pos = step(pos)) {
            int entry = bytes.readInt(pos);
            if (key(entry) == key && value(entry) == oldValue) {
                positions.clear(oldValue);
                positions.set(newValue);
                bytes.writeInt(pos, entry(key, newValue));
                return;
            }
        }
    }

    private void removePos(long posToRemove) {
        int posToShift = (int) posToRemove;
        while (true) {
            posToShift = step(posToShift);
            int entryToShift = bytes.readInt(posToShift);
            if (entryToShift == UNSET_ENTRY)
                break;
            long insertPos = pos(key(entryToShift));
            // see comment in IntIntMultiMap
            boolean cond1 = insertPos <= posToRemove;
            boolean cond2 = posToRemove <= posToShift;
            if ((cond1 && cond2) ||
                    // chain wrapped around capacity
                    (posToShift < insertPos && (cond1 || cond2))) {
                bytes.writeInt(posToRemove, entryToShift);
                posToRemove = posToShift;
            }
        }
        bytes.writeInt(posToRemove, UNSET_ENTRY);
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

    public long size() {
        int pos = 0;
        int size = 0;
        for (int count = capacity; count > 0; count--) {
            int entry = bytes.readInt(pos);
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
    public long nextPos(SearchState searchState) {
        int pos = (int) searchState.searchPos;
        for(int count = capacity; count > 0; count--) {
            int entry = bytes.readInt(pos);
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
        int entry = bytes.readInt(prevPos);
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
            int oldEntry = bytes.readInt(prevPos);
            long oldValue = value(oldEntry);
            checkValueForRemove(oldValue);
            positions.clear(oldValue);
        }
        positions.set(newValue);
        bytes.writeInt(prevPos, entry(searchState.searchHash, newValue));
    }

    @Override
    public void putAfterFailedSearch(SearchState searchState, long value) {
        checkValueForPut(value);
        positions.set(value);
        bytes.writeInt(searchState.searchPos, entry(searchState.searchHash, value));
        searchState.putAfterFailedSearch = true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        long pos = 0L;
        for (int i = 0; i < capacity; i++, pos += ENTRY_SIZE) {
            int entry = bytes.readInt(pos);
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
            int entry = bytes.readInt(pos);
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
