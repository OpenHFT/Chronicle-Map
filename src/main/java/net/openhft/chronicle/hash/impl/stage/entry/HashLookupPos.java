/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class HashLookupPos {

    public long hashLookupPos = -1;
    @StageRef
    HashLookupSearch hls;
    @StageRef
    SegmentStages s;

    public abstract boolean hashLookupPosInit();

    public void initHashLookupPos() {
        // Validation + make hashLookupPos a dependant of tier. This is needed, because after
        // tier change should re-perform hashLookupSearch, starting from the searchStartPos.
        // Not an assert statement, because segmentTier stage should be initialized regardless
        // assertions enabled or not.
        if (s.tier < 0)
            throw new AssertionError();
        s.innerReadLock.lock();
        this.hashLookupPos = hls.searchStartPos;
    }

    public void initHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }

    @Stage("HashLookupPos")
    public void setHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }

    public abstract void closeHashLookupPos();
}
