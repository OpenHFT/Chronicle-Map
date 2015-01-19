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

import static net.openhft.lang.MemoryUnit.BITS;
import static net.openhft.lang.MemoryUnit.BYTES;
import static net.openhft.lang.io.NativeBytes.UNSAFE;

class HashLookup {

    // to fit 64 bits per slot.
    static final int MAX_SEGMENT_CHUNKS = 1 << 30;
    static final int MAX_SEGMENT_ENTRIES = 1 << 29;

    static int valueBits(long actualChunksPerSegment) {
        return 64 - Long.numberOfLeadingZeros(actualChunksPerSegment - 1L);
    }

    static int keyBits(long entriesPerSegment, int valueBits) {
        // key hash cardinality is between 1.0 and 2.0 of key cardinality
        int minKeyBits = 64 - Long.numberOfLeadingZeros(entriesPerSegment - 1L);
        // This minimizes probability of hash collision => on search, additional cache line touch
        // and key comparison, that is quite high cost.
        // between 8.0 and 16.0
        minKeyBits += 3;
        int actualEntryBits = (int) BYTES.align((long) (minKeyBits + valueBits), BITS);
        // devote the rest bits for key
        return actualEntryBits - valueBits;
    }

    static int entrySize(int keyBits, int valueBits) {
        return (int) BYTES.alignAndConvert((long) (keyBits + valueBits), BITS);
    }

    static long capacityFor(long entriesPerSegment) {
        if (entriesPerSegment < 0L)
            throw new IllegalArgumentException("entriesPerSegment should be positive");
        long capacity = Maths.nextPower2(entriesPerSegment, 64L);
        if (((double) entriesPerSegment) / (double) capacity > 2./3.) {
            // hash lookup shouldn't be too dense
            capacity <<= 1L;
        }
        return capacity;
    }

    private static final long UNSET_KEY = 0L;
    private static final long UNSET_ENTRY = 0L;

    private static long mask(int bits) {
        return (1L << bits) - 1L;
    }

    private long address;

    private long capacityMask;

    private int entrySize;
    private long capacityMask2;

    private int keyBits;
    private long keyMask;
    private long valueMask;
    private long entryMask;

    // search state
    private long searchKey = UNSET_KEY;
    private long searchStartPos = -1L;
    private long searchPos = -1L;

    void reuse(long address, long capacity, int entrySize, int keyBits, int valueBits) {
        this.address = address;

        this.capacityMask = capacity - 1L;

        this.entrySize = entrySize;
        this.capacityMask2 = capacityMask * entrySize;

        this.keyBits = keyBits;
        this.keyMask = mask(keyBits);
        this.valueMask = mask(valueBits);
        this.entryMask = mask(keyBits + valueBits);
    }

    private long indexToPos(long index) {
        return index * entrySize;
    }

    private long maskUnsetKey(long key) {
        return (key &= keyMask) != UNSET_KEY ? key : keyMask;
    }

    private void checkValueForPut(long value) {
        assert (value & ~valueMask) == 0L : "Value out of range, was " + value;
    }

    private boolean empty(long entry) {
        return (entry & entryMask) == UNSET_ENTRY;
    }

    private long key(long entry) {
        return entry & keyMask;
    }

    private long value(long entry) {
        return (entry >>> keyBits) & valueMask;
    }

    private long entry(long key, long value) {
        return key | (value << keyBits);
    }

    private long pos(long key) {
        return indexToPos(key & capacityMask);
    }

    private long step(long pos) {
        return (pos += entrySize) <= capacityMask2 ? pos : 0L;
    }

    private long stepBack(long pos) {
        return (pos -= entrySize) >= 0 ? pos : capacityMask2;
    }

    private long readEntry(long pos) {
        return UNSAFE.getLong(address + pos);
    }

    private void writeEntry(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & ~entryMask) | entry(key, value);
        UNSAFE.putLong(address + pos, entry);
    }

    private void writeEntryVolatile(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & ~entryMask) | entry(key, value);
        UNSAFE.putLongVolatile(null, address + pos, entry);
    }

    private void writeEntry(long pos, long prevEntry, long anotherEntry) {
        long entry = (prevEntry & ~entryMask) | (anotherEntry & entryMask);
        UNSAFE.putLong(address + pos, entry);
    }

    private void clearEntry(long pos, long prevEntry) {
        long entry = (prevEntry & ~entryMask);
        UNSAFE.putLong(address + pos, entry);
    }

    public void init0(long key) {
        key = maskUnsetKey(key);
        searchKey = key;
        searchStartPos = pos(key);
    }

    public boolean isInit() {
        return searchKey != UNSET_KEY;
    }

    public void close0() {
        searchKey = UNSET_KEY;
    }

    public void initSearch0() {
        searchPos = searchStartPos;
    }

    public boolean isSearchInit() {
        return searchPos >= 0L;
    }

    public void closeSearch0() {
        searchPos = -1L;
    }

    public long nextPos() {
        long pos = searchPos;
        while (true) {
            long entry = readEntry(pos);
            if (empty(entry)) {
                searchPos = pos;
                return -1L;
            }
            pos = step(pos);
            if (pos == searchStartPos)
                break;
            if (key(entry) == searchKey) {
                searchPos = pos;
                return value(entry);
            }
        }
        throw new IllegalStateException("MultiMap is full, that most likely means you " +
                "misconfigured entrySize/chunkSize, and entries tend to take less chunks than " +
                "expected");
    }

    public void found() {
        searchPos = stepBack(searchPos);
    }

    public void remove() {
        // for support of patterns like context.remove().put()
        searchPos = remove0();
    }

    private long remove0() {
        return remove00(searchPos);
    }

    private long remove00(long posToRemove) {
        long entryToRemove = readEntry(posToRemove);
        long posToShift = posToRemove;
        while (true) {
            posToShift = step(posToShift);
            long entryToShift = readEntry(posToShift);
            if (empty(entryToShift))
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
                writeEntry(posToRemove, entryToRemove, entryToShift);
                posToRemove = posToShift;
                entryToRemove = entryToShift;
            }
        }
        clearEntry(posToRemove, entryToRemove);
        return posToRemove;
    }

    public void put(long value) {
        checkValueForPut(value);
        writeEntry(searchPos, readEntry(searchPos), searchKey, value);
    }

    public void putVolatile(long value) {
        checkValueForPut(value);
        writeEntryVolatile(searchPos, readEntry(searchPos), searchKey, value);
    }

    public void clear() {
        UNSAFE.setMemory(address, capacityMask2 + entrySize, (byte) 0);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        forEach(new EntryConsumer() {
            @Override
            public void accept(long key, long value) {
                sb.append(key).append('=').append(value).append(',');
            }
        });
        sb.append('}');
        return sb.toString();
    }

    static interface EntryConsumer {
        void accept(long key, long value);
    }

    public void forEach(EntryConsumer action) {
        for (long pos = 0L; pos <= capacityMask2; pos += entrySize) {
            long entry = readEntry(pos);
            if (!empty(entry))
                action.accept(key(entry), value(entry));
        }
    }

    static interface EntryPredicate {
        boolean remove(long hash, long pos);
    }

    public void forEachRemoving(EntryPredicate predicate) {
        long pos = 0L;
        while (!empty(readEntry(pos))) {
            pos = step(pos);
        }
        long startPos = pos;
        do {
            pos = step(pos);
            long entry = readEntry(pos);
            if (!empty(entry)) {
                if (predicate.remove(key(entry), value(entry))) {
                    remove00(pos);
                    pos = stepBack(pos);
                }
            }
        } while (pos != startPos);
    }
}
