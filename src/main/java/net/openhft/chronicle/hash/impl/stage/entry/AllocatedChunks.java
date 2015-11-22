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

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class AllocatedChunks {

    @StageRef public VanillaChronicleHashHolder<?> hh;
    @StageRef public SegmentStages s;
    @StageRef public HashEntryStages<?> entry;
    @StageRef public Alloc alloc;

    public int allocatedChunks = 0;
    
    public void initAllocatedChunks(int allocatedChunks) {
        this.allocatedChunks = allocatedChunks;
    }
    
    public void incrementSegmentEntriesIfNeeded() {
        // don't increment
    }

    /**
     * @return {@code true} is tier has changed
     */
    public boolean initEntryAndKeyCopying(long entrySize, long bytesToCopy) {
        initAllocatedChunks(hh.h().inChunks(entrySize));
        // call incrementSegmentEntriesIfNeeded() before entry.copyExistingEntry(), because
        // the latter clears out searchState, and it performs the search again, but in inconsistent
        // state
        incrementSegmentEntriesIfNeeded();
        long oldSegmentTierBaseAddr = s.segmentBaseAddr;
        long oldKeySizeAddr = oldSegmentTierBaseAddr + entry.keySizeOffset;
        long oldKeyAddr = oldSegmentTierBaseAddr + entry.keyOffset;
        int tierBeforeAllocation = s.segmentTier;
        long pos = alloc.alloc(allocatedChunks);
        entry.copyExistingEntry(pos, bytesToCopy, oldKeyAddr, oldKeySizeAddr);
        return s.segmentTier != tierBeforeAllocation;
    }
}
