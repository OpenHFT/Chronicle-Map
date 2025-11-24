/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.KeyHashCode;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Computes and caches the hash code for the key at the current entry during iteration or
 * recovery.
 *
 * <p>The hash is derived directly from the off-heap key bytes using the same {@link
 * LongHashFunction} variant as the main hash table, so it can be reused by other stages such as
 * lookup, relocation, or corruption checking without re-reading the key. This stage is internal
 * to Chronicle Map and is not part of the public API.
 */
@Staged
public class IterationKeyHashCode implements KeyHashCode {

    @StageRef
    VanillaChronicleHashHolder<?> hh;
    @StageRef
    SegmentStages s;
    @StageRef
    HashEntryStages<?> e;

    long keyHash = 0;

    void initKeyHash() {
        long addr = s.tierBaseAddr + e.keyOffset;
        long len = e.keySize;
        keyHash = LongHashFunction.xx_r39().hashMemory(addr, len);
    }

    @Override
    public long keyHashCode() {
        return keyHash;
    }
}
