/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
