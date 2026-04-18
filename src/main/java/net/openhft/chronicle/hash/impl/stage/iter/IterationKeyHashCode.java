/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.KeyHashCode;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

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
