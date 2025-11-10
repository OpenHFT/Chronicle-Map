//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.OS;

import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertAddress;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertPosition;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class IntCompactOffHeapLinearHashTable extends CompactOffHeapLinearHashTable {

    private static final long SCALE = 4L;

    /**
     * Must not store {@code h} in a field, to avoid memory leaks.
     *
     * @see net.openhft.chronicle.hash.impl.stage.hash.Chaining#initMap
     */
    IntCompactOffHeapLinearHashTable(VanillaChronicleHash h) {
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
    public long readEntry(final long address,
                          final long pos) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        return OS.memory().readInt(address + pos);
    }

    @Override
    public long readEntryVolatile(final long address,
                                  final long pos) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        return OS.memory().readVolatileInt(address + pos);
    }

    @Override
    public void writeEntryVolatile(final long address,
                                   final long pos,
                                   final long key,
                                   final long value) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        OS.memory().writeVolatileInt(address + pos, (int) entry(key, value));
    }

    @Override
    public void writeEntry(long address, long pos, long newEntry) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        OS.memory().writeInt(address + pos, (int) newEntry);
    }

    @Override
    public void clearEntry(long address, long pos) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        OS.memory().writeInt(address + pos, 0);
    }
}
