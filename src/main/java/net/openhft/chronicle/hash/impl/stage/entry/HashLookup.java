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

package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;
import net.openhft.lang.Maths;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.lang.MemoryUnit.BITS;
import static net.openhft.lang.MemoryUnit.BYTES;
import static net.openhft.lang.io.NativeBytes.UNSAFE;

@Staged
public class HashLookup {
    // to fit 64 bits per slot.
    public static final int MAX_SEGMENT_CHUNKS = 1 << 30;
    public static final int MAX_SEGMENT_ENTRIES = 1 << 29;


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
        return (int) BYTES.alignAndConvert((long) (keyBits + valueBits), BITS);
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

    @Stage("SegmentHashLookup") long address = -1;
    @Stage("SegmentHashLookup") long capacityMask;
    @Stage("SegmentHashLookup") int hashLookupEntrySize;
    @Stage("SegmentHashLookup") long capacityMask2;
    @Stage("SegmentHashLookup") int keyBits;
    @Stage("SegmentHashLookup") long keyMask;
    @Stage("SegmentHashLookup") long valueMask;
    @Stage("SegmentHashLookup") long entryMask;

    public void initSegmentHashLookup(
            long address, long capacity, int entrySize, int keyBits, int valueBits) {
        innerInitSegmentHashLookup(address, capacity, entrySize, keyBits, valueBits);
    }

    @Stage("SegmentHashLookup")
    private void innerInitSegmentHashLookup(
            long address, long capacity, int entrySize, int keyBits, int valueBits) {
        this.address = address;

        this.capacityMask = capacity - 1L;

        this.hashLookupEntrySize = entrySize;
        this.capacityMask2 = capacityMask * entrySize;

        this.keyBits = keyBits;
        this.keyMask = mask(keyBits);
        this.valueMask = mask(valueBits);
        this.entryMask = mask(keyBits + valueBits);
    }
    
    @StageRef VanillaChronicleHashHolder<?, ?, ?> hh;
    @StageRef SegmentStages s;

    public void initSegmentHashLookup() {
        long hashLookupOffset = hh.h().segmentOffset(s.segmentIndex);
        innerInitSegmentHashLookup(hh.h().ms.address() + hashLookupOffset,
                hh.h().segmentHashLookupCapacity, hh.h().segmentHashLookupEntrySize,
                hh.h().segmentHashLookupKeyBits, hh.h().segmentHashLookupValueBits);
        
    }

    long indexToPos(long index) {
        return index * hashLookupEntrySize;
    }

    public long maskUnsetKey(long key) {
        return (key &= keyMask) != UNSET_KEY ? key : keyMask;
    }

    public void checkValueForPut(long value) {
        assert (value & ~valueMask) == 0L : "Value out of range, was " + value;
    }

    public boolean empty(long entry) {
        return (entry & entryMask) == UNSET_ENTRY;
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

    public long step(long pos) {
        return (pos += hashLookupEntrySize) <= capacityMask2 ? pos : 0L;
    }

    public long stepBack(long pos) {
        return (pos -= hashLookupEntrySize) >= 0 ? pos : capacityMask2;
    }

    public long readEntry(long pos) {
        return UNSAFE.getLong(address + pos);
    }

    public void writeEntry(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & ~entryMask) | entry(key, value);
        UNSAFE.putLong(address + pos, entry);
    }

    public void writeEntryVolatile(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & ~entryMask) | entry(key, value);
        UNSAFE.putLongVolatile(null, address + pos, entry);
    }
    
    public void putValueVolatile(long pos, long value) {
        checkValueForPut(value);
        long currentEntry = readEntry(pos);
        writeEntryVolatile(pos, currentEntry, key(currentEntry), value);
    }

    void writeEntry(long pos, long prevEntry, long anotherEntry) {
        long entry = (prevEntry & ~entryMask) | (anotherEntry & entryMask);
        UNSAFE.putLong(address + pos, entry);
    }

    void clearEntry(long pos, long prevEntry) {
        long entry = (prevEntry & ~entryMask);
        UNSAFE.putLong(address + pos, entry);
    }

    public void clearHashLookup() {
        UNSAFE.setMemory(address, capacityMask2 + hashLookupEntrySize, (byte) 0);
    }

    /**
     * Returns "insert" position in terms of consequent putValue()
     */
    public long remove(long posToRemove) {
        long entryToRemove = readEntry(posToRemove);
        long posToShift = posToRemove;
        while (true) {
            posToShift = step(posToShift);
            long entryToShift = readEntry(posToShift);
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
                writeEntry(posToRemove, entryToRemove, entryToShift);
                posToRemove = posToShift;
                entryToRemove = entryToShift;
            }
        }
        clearEntry(posToRemove, entryToRemove);
        return posToRemove;
    }

    String hashLookupToString() {
        final StringBuilder sb = new StringBuilder("{");
        forEach((key, value) -> sb.append(key).append('=').append(value).append(','));
        sb.append('}');
        return sb.toString();
    }

    void forEach(EntryConsumer action) {
        for (long pos = 0L; pos <= capacityMask2; pos += hashLookupEntrySize) {
            long entry = readEntry(pos);
            if (!empty(entry))
                action.accept(key(entry), value(entry));
        }
    }
}
