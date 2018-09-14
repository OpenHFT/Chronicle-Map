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
