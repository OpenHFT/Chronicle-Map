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

package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.entry.LocksInterface;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class IterationSegmentStages extends SegmentStages {

    @StageRef
    VanillaChronicleHashHolder<?> hh;
    @StageRef
    HashSegmentIteration it;
    @StageRef
    HashLookupSearch hls;

    /**
     * During iteration, nextTier() is called in doReplaceValue() -> relocation() -> alloc().
     * When the entry is relocated to the next tier, an entry should be inserted into hash
     * lookup. To insert an entry into hashLookup, should 1) locate empty slot, see {@link
     * KeySearch#initKeySearch()}, and 2) know the part of the hash code to insert, we know
     * it during iteration
     */
    @Override
    public void nextTier() {
        super.nextTier();
        if (it.hashLookupEntryInit())
            hls.initSearchKey(hh.h().hashLookup.key(it.hashLookupEntry));
    }

    public void initSegmentTier_WithBaseAddr(int tier, long tierBaseAddr, long tierIndex) {
        this.tier = tier;
        this.tierIndex = tierIndex;
        this.tierBaseAddr = tierBaseAddr;
    }

    @Override
    public void checkNestedContextsQueryDifferentKeys(
            LocksInterface innermostContextOnThisSegment) {
        // this check is relevant only for query contexts
    }
}
