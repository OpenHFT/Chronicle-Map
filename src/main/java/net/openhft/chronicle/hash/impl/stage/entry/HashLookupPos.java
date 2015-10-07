/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class HashLookupPos {

    @StageRef HashLookupSearch hls;

    public long hashLookupPos = -1;

    public abstract boolean hashLookupPosInit();

    public void initHashLookupPos() {
        // validation + make hashLookupPos a dependant of segmentTier. This is needed, because
        // after tier change should re-perform hashLookupSearch, starting from the searchStartPos
        assert s.segmentTier >= 0;
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

    @StageRef VanillaChronicleHashHolder<?, ?, ?> hh;
    @StageRef SegmentStages s;

    public void putValueVolatile(long newValue) {
        CompactOffHeapLinearHashTable hashLookup = hh.h().hashLookup;
        hashLookup.checkValueForPut(newValue);
        hashLookup.putValueVolatile(s.segmentBaseAddr, hashLookupPos, newValue);
    }
}
