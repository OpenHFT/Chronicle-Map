/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
