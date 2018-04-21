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

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class AllocatedChunks {

    @StageRef
    public VanillaChronicleHashHolder<?> hh;
    @StageRef
    public SegmentStages s;
    @StageRef
    public HashEntryStages<?> entry;
    @StageRef
    public Alloc alloc;

    public int allocatedChunks = 0;

    public void initAllocatedChunks(int allocatedChunks) {
        this.allocatedChunks = allocatedChunks;
    }

    /**
     * @return {@code true} is tier has changed
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
