/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Holds staging references and accounting for chunk allocations during entry relocation.
 */
@Staged
public class AllocatedChunks {

    /**
     * Chronicle hash holder for the current stage.
     */
    @StageRef
    public VanillaChronicleHashHolder<?> hh;
    /** Segment-level staging data. */
    @StageRef
    public SegmentStages s;
    /** Entry staging data for the current operation. */
    @StageRef
    public HashEntryStages<?> entry;
    /** Allocator used to reserve space. */
    @StageRef
    public Alloc alloc;

    /** Number of chunks allocated for the pending move. */
    public int allocatedChunks = 0;

    /**
     * Creates an instance for staged use.
     */
    public AllocatedChunks() {
    }

    /**
     * Records the number of chunks allocated for the current operation.
     *
     * @param allocatedChunks chunk count
     */
    public void initAllocatedChunks(int allocatedChunks) {
        this.allocatedChunks = allocatedChunks;
    }

    /**
     * Copies an entry into a newly allocated area.
     *
     * @param entrySize   size of the entry in bytes
     * @param bytesToCopy number of bytes to copy from the previous location
     * @param prevPos     previous allocation position or -1 if none
     * @param prevChunks  previous allocation size in chunks
     * @return {@code true} if the segment tier changed
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
