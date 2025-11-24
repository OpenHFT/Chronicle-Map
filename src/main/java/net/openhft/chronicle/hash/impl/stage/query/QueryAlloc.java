/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.Alloc;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Allocation strategy used while serving map queries that need to materialise or grow entries.
 *
 * <p>Unlike {@code IterationAlloc}, this allocator is free to walk all tiers of the segment,
 * starting from the root, to find space for the requested number of chunks. It avoids allocating
 * twice from the same tier for a single operation and always frees any previous location once a
 * new one has been reserved.
 */
@Staged
public class QueryAlloc implements Alloc {

    @StageRef
    public SegmentStages s;

    @Override
    public long alloc(int chunks, long prevPos, int prevChunks) {
        long ret = s.allocReturnCode(chunks);
        if (prevPos >= 0)
            s.free(prevPos, prevChunks);
        if (ret >= 0)
            return ret;
        int alreadyAttemptedTier = s.tier;
        s.goToFirstTier();
        while (true) {
            if (s.tier != alreadyAttemptedTier) {
                ret = s.allocReturnCode(chunks);
                if (ret >= 0)
                    return ret;
            }
            s.nextTier();
        }
    }
}
