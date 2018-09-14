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
