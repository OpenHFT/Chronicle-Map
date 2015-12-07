/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.impl;


import net.openhft.chronicle.core.Maths;

import static net.openhft.chronicle.algo.MemoryUnit.BITS;
import static net.openhft.chronicle.algo.MemoryUnit.BYTES;

public abstract class CompactOffHeapLinearHashTable {
    // to fit 64 bits per slot.
    public static final int MAX_TIER_CHUNKS = 1 << 30;
    public static final int MAX_TIER_ENTRIES = 1 << 29;


    public static int valueBits(long actualChunksPerSegment) {
        return 64 - Long.numberOfLeadingZeros(actualChunksPerSegment - 1L);
    }

    public static int keyBits(long entriesPerSegment, int valueBits) {
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

    public static int entrySize(int keyBits, int valueBits) {
        int entrySize = (int) BYTES.alignAndConvert((long) (keyBits + valueBits), BITS);
        if (entrySize <= 4)
            return 4;
        if (entrySize <= 8)
            return 8;
        return entrySize;
    }

    public static long capacityFor(long entriesPerSegment) {
        if (entriesPerSegment < 0L)
            throw new IllegalArgumentException("entriesPerSegment should be positive");
        long capacity = Maths.nextPower2(entriesPerSegment, 64L);
        if (((double) entriesPerSegment) / (double) capacity > 2./3.) {
            // hash lookup shouldn't be too dense
            capacity <<= 1L;
        }
        return capacity;
    }

    public static long mask(int bits) {
        return (1L << bits) - 1L;
    }

    public static final long UNSET_KEY = 0L;
    public static final long UNSET_ENTRY = 0L;


    private final long capacityMask;
    final long capacityMask2;
    private final int keyBits;
    private final long keyMask;
    private final long valueMask;

    CompactOffHeapLinearHashTable(long capacity, int entrySize, int keyBits, int valueBits) {
        this.capacityMask = capacity - 1L;

        this.capacityMask2 = capacityMask * entrySize;

        this.keyBits = keyBits;
        this.keyMask = mask(keyBits);
        this.valueMask = mask(valueBits);
    }

    CompactOffHeapLinearHashTable(VanillaChronicleHash h) {
        this(h.tierHashLookupCapacity, h.tierHashLookupEntrySize, h.tierHashLookupKeyBits,
                h.tierHashLookupValueBits);
    }

    abstract long indexToPos(long index);

    public long maskUnsetKey(long key) {
        return (key &= keyMask) != UNSET_KEY ? key : keyMask;
    }

    public void checkValueForPut(long value) {
        assert (value & ~valueMask) == 0L : "Value out of range, was " + value;
    }

    public boolean empty(long entry) {
        return entry == UNSET_ENTRY;
    }

    public long key(long entry) {
        return entry & keyMask;
    }

    public long value(long entry) {
        return (entry >>> keyBits) & valueMask;
    }

    long entry(long key, long value) {
        return key | (value << keyBits);
    }

    public long hlPos(long key) {
        return indexToPos(key & capacityMask);
    }

    public abstract long step(long pos);

    public abstract long stepBack(long pos);

    public abstract long readEntry(long addr, long pos);

    public abstract void writeEntryVolatile(
            long addr, long pos, long prevEntry, long key, long value);
    
    public void putValueVolatile(long addr, long pos, long value) {
        checkValueForPut(value);
        long currentEntry = readEntry(addr, pos);
        writeEntryVolatile(addr, pos, currentEntry, key(currentEntry), value);
    }

    abstract void writeEntry(long addr, long pos, long prevEntry, long anotherEntry);

    abstract void clearEntry(long addr, long pos, long prevEntry);

    /**
     * Returns "insert" position in terms of consequent putValue()
     */
    public long remove(long addr, long posToRemove) {
        long entryToRemove = readEntry(addr, posToRemove);
        long posToShift = posToRemove;
        while (true) {
            posToShift = step(posToShift);
            long entryToShift = readEntry(addr, posToShift);
            if (empty(entryToShift))
                break;
            long insertPos = hlPos(key(entryToShift));
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
                writeEntry(addr, posToRemove, entryToRemove, entryToShift);
                posToRemove = posToShift;
                entryToRemove = entryToShift;
            }
        }
        clearEntry(addr, posToRemove, entryToRemove);
        return posToRemove;
    }
}
