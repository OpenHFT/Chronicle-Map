/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.hash.impl.stage.entry.Alloc;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Allocation strategy used when resizing or relocating entries during segment iteration.
 *
 * <p>The allocator frees the previous location, then searches the current tier followed by later
 * tiers until it finds space for the requested number of chunks. It deliberately never walks
 * backwards through tiers so that iterator implementations do not visit relocated entries more
 * than once.
 */
@Staged
public class IterationAlloc implements Alloc {

    @StageRef
    public SegmentStages s;

    /**
     * Move only to next tiers, to avoid double visiting of relocated entries during iteration
     */
    @Override
    public long alloc(int chunks, long prevPos, int prevChunks) {
        long ret = s.allocReturnCode(chunks);
        if (prevPos >= 0)
            s.free(prevPos, prevChunks);
        if (ret >= 0)
            return ret;
        while (true) {
            s.nextTier();
            ret = s.allocReturnCode(chunks);
            if (ret >= 0)
                return ret;
        }
    }
}
