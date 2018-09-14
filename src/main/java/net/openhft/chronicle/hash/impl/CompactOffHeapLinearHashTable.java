/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.Maths;

import static net.openhft.chronicle.algo.MemoryUnit.BITS;
import static net.openhft.chronicle.algo.MemoryUnit.BYTES;

public abstract class CompactOffHeapLinearHashTable {
    // to fit 64 bits per slot.
    public static final int MAX_TIER_CHUNKS = 1 << 30;
    public static final int MAX_TIER_ENTRIES = 1 << 29;
    /**
     * Normally, hashLookup capacity is chosen so that the lf varies from 1/3 to 2/3, at most
     */
    public static final double MAX_UPPER_BOUND_LOAD_FACTOR = .2 / .3;
    /**
     * If average key/value sizes are configured wrong, and Chronicle Hash bloats up (size grows
     * beyond configured .entries()), hashLookup might become too full. Stop insertion and trigger
     * segment tiering, when hashLookup load factor > 0.8, because collision chains become too long.
     */
    public static final double MAX_LOAD_FACTOR = 0.8;
    public static final long UNSET_KEY = 0L;
    public static final long UNSET_ENTRY = 0L;
    final long capacityMask2;
    private final long capacityMask;
    private final int keyBits;
    private final long keyMask;
    private final long valueMask;

    CompactOffHeapLinearHashTable(long capacity, int slotSize, int keyBits, int valueBits) {
        this.capacityMask = capacity - 1L;

        this.capacityMask2 = capacityMask * slotSize;

        this.keyBits = keyBits;
        this.keyMask = mask(keyBits);
        this.valueMask = mask(valueBits);
    }
    /**
     * Must not store {@code h} in a field, to avoid memory leaks.
     *
     * @see net.openhft.chronicle.hash.impl.stage.hash.Chaining#initMap
     */
    CompactOffHeapLinearHashTable(VanillaChronicleHash h) {
        this(h.tierHashLookupCapacity, h.tierHashLookupSlotSize, h.tierHashLookupKeyBits,
                h.tierHashLookupValueBits);
    }

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
            throw new AssertionError("entriesPerSegment should be positive: " + entriesPerSegment);
        long capacity = Maths.nextPower2(entriesPerSegment, 64L);
        if (entriesPerSegment > MAX_UPPER_BOUND_LOAD_FACTOR * capacity)
            capacity *= 2;
        return capacity;
    }

    public static long mask(int bits) {
        return (1L << bits) - 1L;
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

    public long entry(long key, long value) {
        return key | (value << keyBits);
    }

    public long hlPos(long key) {
        return indexToPos(key & capacityMask);
    }

    public abstract long step(long pos);

    public abstract long stepBack(long pos);

    /**
     * Used when we assume we are holding at least update lock, and during recovery (because we
     * don't care about concurrency during recovery)
     */
    public abstract long readEntry(long addr, long pos);

    /**
     * Used when we are under read lock and aware of possible concurrent insertions
     */
    public abstract long readEntryVolatile(long addr, long pos);

    public abstract void writeEntryVolatile(long addr, long pos, long key, long value);

    public abstract void writeEntry(long addr, long pos, long newEntry);

    public abstract void clearEntry(long addr, long pos);

    /**
     * Returns "insert" position in terms of consequent putValue()
     */
    public long remove(long addr, long posToRemove) {
        long posToShift = posToRemove;
        while (true) {
            posToShift = step(posToShift);
            // volatile read not needed because removed is performed under exclusive lock
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
                writeEntry(addr, posToRemove, entryToShift);
                posToRemove = posToShift;
            }
        }
        clearEntry(addr, posToRemove);
        return posToRemove;
    }
}
