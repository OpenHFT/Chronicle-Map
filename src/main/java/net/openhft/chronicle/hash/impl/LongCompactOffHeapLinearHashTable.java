/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

    /**
     * Must not store {@code h} in a field, to avoid memory leaks.
     *
     * @see net.openhft.chronicle.hash.impl.stage.hash.Chaining#initMap
     */
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
        OS.memory().writeVolatileLong(addr + pos, entry(key, value));
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
