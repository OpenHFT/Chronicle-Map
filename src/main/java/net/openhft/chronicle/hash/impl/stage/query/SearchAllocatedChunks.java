/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.AllocatedChunks;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class SearchAllocatedChunks extends AllocatedChunks {

    @StageRef
    KeySearch<?> ks;

    /**
     * @return {@code true} if tier has changed
     */
    public boolean initEntryAndKey(long entrySize) {
        initAllocatedChunks(hh.h().inChunks(entrySize));
        int tierBeforeAllocation = s.tier;
        long pos = alloc.alloc(allocatedChunks, -1, 0);
        entry.writeNewEntry(pos, ks.inputKey);
        return s.tier != tierBeforeAllocation;
    }
}
