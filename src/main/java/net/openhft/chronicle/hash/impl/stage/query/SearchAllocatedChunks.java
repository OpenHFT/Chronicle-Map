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

package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.AllocatedChunks;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.stage.query.KeySearch.SearchState.PRESENT;

@Staged
public class SearchAllocatedChunks extends AllocatedChunks {

    @StageRef KeySearch<?> ks;

    @Override
    public void incrementSegmentEntriesIfNeeded() {
        if (ks.searchState != PRESENT) {
            // update the size before the store fence
            s.entries(s.entries() + 1L);
        }
    }

    /**
     * @return {@code true} is tier has changed
     */
    public boolean initEntryAndKey(long entrySize) {
        initAllocatedChunks(hh.h().inChunks(entrySize));
        // call incrementSegmentEntriesIfNeeded() before entry.writeNewEntry(), because the latter
        // clears out searchState, and it performs the search again, but in inconsistent state
        incrementSegmentEntriesIfNeeded();
        int tierBeforeAllocation = s.segmentTier;
        long pos = alloc.alloc(allocatedChunks);
        entry.writeNewEntry(pos, ks.inputKey);
        return s.segmentTier != tierBeforeAllocation;
    }
}
