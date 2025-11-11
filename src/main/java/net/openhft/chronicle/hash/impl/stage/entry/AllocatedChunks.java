/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class AllocatedChunks {

    @StageRef
    public VanillaChronicleHashHolder<?> hh;
    @StageRef
    public SegmentStages s;
    @StageRef
    public HashEntryStages<?> entry;
    @StageRef
    public Alloc alloc;

    public int allocatedChunks = 0;

    public void initAllocatedChunks(int allocatedChunks) {
        this.allocatedChunks = allocatedChunks;
    }

    /**
     * @return {@code true} is tier has changed
     */
    public boolean initEntryAndKeyCopying(
            long entrySize, long bytesToCopy, long prevPos, int prevChunks) {
        initAllocatedChunks(hh.h().inChunks(entrySize));
        long oldSegmentTierBaseAddr = s.tierBaseAddr;
        long oldKeySizeAddr = oldSegmentTierBaseAddr + entry.keySizeOffset;
        long oldKeyAddr = oldSegmentTierBaseAddr + entry.keyOffset;
        int tierBeforeAllocation = s.tier;
        long pos = alloc.alloc(allocatedChunks, prevPos, prevChunks);
        entry.copyExistingEntry(pos, bytesToCopy, oldKeyAddr, oldKeySizeAddr);
        return s.tier != tierBeforeAllocation;
    }
}
