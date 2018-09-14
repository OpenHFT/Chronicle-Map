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

package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.algo.bitset.ReusableBitSet;
import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.iter.TierRecovery;
import net.openhft.chronicle.map.ChronicleHashCorruptionImpl;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.format;
import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.report;

@Staged
public class ReplicatedTierRecovery extends TierRecovery {

    @StageRef
    ReplicatedChronicleMapHolder<?, ?, ?> rh;
    @StageRef
    SegmentStages s;
    @StageRef
    ReplicatedMapEntryStages<?, ?> e;

    @Override
    public void removeDuplicatesInSegment(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        super.removeDuplicatesInSegment(corruptionListener, corruption);
        recoverTierDeleted(corruptionListener, corruption);
        cleanupModificationIterationBits();
    }

    private void recoverTierDeleted(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        VanillaChronicleHash<?, ?, ?, ?> h = rh.h();
        CompactOffHeapLinearHashTable hl = h.hashLookup;
        long hlAddr = s.tierBaseAddr;

        long deleted = 0;
        long hlPos = 0;
        do {
            long hlEntry = hl.readEntry(hlAddr, hlPos);
            if (!hl.empty(hlEntry)) {
                e.readExistingEntry(hl.value(hlEntry));
                if (e.entryDeleted()) {
                    deleted++;
                }
            }
            hlPos = hl.step(hlPos);
        } while (hlPos != 0);
        if (s.tierDeleted() != deleted) {
            long finalDeleted = deleted;
            report(corruptionListener, corruption, s.segmentIndex, () ->
                    format("wrong deleted counter for tier with index {}, stored: {}, should be: {}",
                            s.tierIndex, s.tierDeleted(), finalDeleted)
            );
            s.tierDeleted(deleted);
        }
    }

    private void cleanupModificationIterationBits() {
        ReplicatedChronicleMap<?, ?, ?> m = rh.m();
        ReplicatedChronicleMap<?, ?, ?>.ModificationIterator[] its =
                m.acquireAllModificationIterators();
        ReusableBitSet freeList = s.freeList;
        for (long pos = 0; pos < m.actualChunksPerSegmentTier; ) {
            long nextPos = freeList.nextSetBit(pos);
            if (nextPos > pos) {
                for (ReplicatedChronicleMap<?, ?, ?>.ModificationIterator it : its) {
                    it.clearRange0(s.tierIndex, pos, nextPos);
                }
            }
            if (nextPos > 0) {
                e.readExistingEntry(nextPos);
                if (e.entrySizeInChunks > 1) {
                    for (ReplicatedChronicleMap<?, ?, ?>.ModificationIterator it : its) {
                        it.clearRange0(s.tierIndex, nextPos + 1, nextPos + e.entrySizeInChunks);
                    }
                }
                pos = nextPos + e.entrySizeInChunks;
            } else {
                for (ReplicatedChronicleMap<?, ?, ?>.ModificationIterator it : its) {
                    it.clearRange0(s.tierIndex, pos, m.actualChunksPerSegmentTier);
                }
                break;
            }
        }
    }
}
