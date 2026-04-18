/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.OS;

import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertAddress;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertPosition;

@SuppressWarnings({"rawtypes", "unchecked"})
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
    public long readEntry(final long address,
                          final long pos) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        return OS.memory().readLong(address + pos);
    }

    @Override
    public long readEntryVolatile(final long address,
                                  final long pos) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        return OS.memory().readVolatileLong(address + pos);
    }

    @Override
    public void writeEntryVolatile(final long address,
                                   final long pos,
                                   final long key,
                                   final long value) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        OS.memory().writeVolatileLong(address + pos, entry(key, value));
    }

    @Override
    public void writeEntry(final long address,
                           final long pos,
                           final long newEntry) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        OS.memory().writeLong(address + pos, newEntry);
    }

    @Override
    public void clearEntry(final long address,
                           final long pos) {
        assert SKIP_ASSERTIONS || assertAddress(address);
        assert SKIP_ASSERTIONS || assertPosition(pos);
        OS.memory().writeLong(address + pos, 0L);
    }
}
