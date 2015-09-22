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

import static net.openhft.lang.io.NativeBytes.UNSAFE;

public final class LongCompactOffHeapLinearHashTable extends CompactOffHeapLinearHashTable {

    private static final long SCALE = 8L;

    LongCompactOffHeapLinearHashTable(VanillaChronicleHash h) {
        super(h);
    }

    @Override
    long indexToPos(long index) {
        return index * SCALE;
    }

    @Override
    public long step(long pos) {
        return (pos + SCALE) & capacityMask2;
    }

    @Override
    public long stepBack(long pos) {
        return (pos - SCALE) & capacityMask2;
    }

    @Override
    public long readEntry(long addr, long pos) {
        return UNSAFE.getLong(addr + pos);
    }

    @Override
    public void writeEntryVolatile(long addr, long pos, long prevEntry, long key, long value) {
        UNSAFE.putLongVolatile(null, addr + pos, entry(key, value));
    }

    @Override
    void writeEntry(long addr, long pos, long prevEntry, long anotherEntry) {
        UNSAFE.putLong(addr + pos, anotherEntry);
    }

    @Override
    void clearEntry(long addr, long pos, long prevEntry) {
        UNSAFE.putLong(addr + pos, 0L);
    }
}
