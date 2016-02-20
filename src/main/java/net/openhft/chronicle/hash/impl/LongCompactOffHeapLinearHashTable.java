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

import net.openhft.chronicle.core.OS;

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
        return OS.memory().readLong(addr + pos);
    }

    @Override
    public long readEntryVolatile(long addr, long pos) {
        return OS.memory().readVolatileLong(addr + pos);
    }

    @Override
    public void writeEntryVolatile(long addr, long pos, long key, long value) {
        OS.memory().writeVolatileLong(null, addr + pos, entry(key, value));
    }

    @Override
    public void writeEntry(long addr, long pos, long newEntry) {
        OS.memory().writeLong(addr + pos, newEntry);
    }

    @Override
    public void clearEntry(long addr, long pos) {
        OS.memory().writeLong(addr + pos, 0L);
    }
}
