/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.AllocatedChunks;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Allocates space for a new entry during a query and writes the key bytes into it.
 *
 * <p>The stage wraps {@link AllocatedChunks} to request enough chunks for the entry, delegates
 * to the configured allocator, and initialises the entry at the returned position using the key
 * provided by {@link KeySearch}. It also reports whether the allocation caused the segment to
 * move to a different tier, which is useful for callers that need to update iteration or search
 * state.
 */
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
