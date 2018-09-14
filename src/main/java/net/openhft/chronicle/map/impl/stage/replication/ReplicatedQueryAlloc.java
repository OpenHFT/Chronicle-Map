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

package net.openhft.chronicle.map.impl.stage.replication;

import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.query.QueryAlloc;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapSegmentContext;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.impl.IterationContext;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Consumer;

import static net.openhft.chronicle.hash.replication.TimeProvider.currentTime;
import static net.openhft.chronicle.hash.replication.TimeProvider.systemTimeIntervalBetween;

@Staged
public class ReplicatedQueryAlloc extends QueryAlloc {

    final CleanupAction cleanupAction = new CleanupAction();
    @StageRef
    ReplicatedChronicleMapHolder<?, ?, ?> mh;
    @StageRef
    SegmentStages s;

    /**
     * Returns {@code true} if at least one old deleted entry was removed.
     *
     * @param prevPos position to skip during cleanup (because cleaned up separately)
     */
    public boolean forcedOldDeletedEntriesCleanup(long prevPos) {
        ReplicatedChronicleMap<?, ?, ?> map = mh.m();
        if (!map.cleanupRemovedEntries)
            return false;
        try (MapSegmentContext<?, ?, ?> sc = map.segmentContext(s.segmentIndex)) {
            cleanupAction.removedCompletely = 0;
            cleanupAction.posToSkip = prevPos;
            cleanupAction.iterationContext = (IterationContext<?, ?, ?>) sc;
            ((ReplicatedHashSegmentContext<?, ?>) sc)
                    .forEachSegmentReplicableEntry(cleanupAction);
            return cleanupAction.removedCompletely > 0;
        }
    }

    @Override
    public long alloc(int chunks, long prevPos, int prevChunks) {
        long ret = s.allocReturnCode(chunks);
        if (ret >= 0) {
            if (prevPos >= 0)
                s.free(prevPos, prevChunks);
            return ret;
        }
        int firstAttemptedTier = s.tier;
        long firstAttemptedTierIndex = s.tierIndex;
        long firstAttemptedTierBaseAddr = s.tierBaseAddr;
        boolean cleanedFirstAttemptedTier = forcedOldDeletedEntriesCleanup(prevPos);
        s.goToFirstTier();
        while (true) {
            boolean visitingFirstAttemptedTier = s.tier == firstAttemptedTier;
            if (cleanedFirstAttemptedTier || !visitingFirstAttemptedTier) {
                ret = s.allocReturnCode(chunks);
                if (ret >= 0) {
                    if (prevPos >= 0) {
                        if (visitingFirstAttemptedTier) {
                            s.free(prevPos, prevChunks);
                        } else if (s.tier < firstAttemptedTier) {
                            int currentTier = s.tier;
                            long currentTierIndex = s.tierIndex;
                            long currentTierBaseAddr = s.tierBaseAddr;
                            s.initSegmentTier(firstAttemptedTier, firstAttemptedTierIndex,
                                    firstAttemptedTierBaseAddr);
                            s.free(prevPos, prevChunks);
                            s.initSegmentTier(currentTier, currentTierIndex, currentTierBaseAddr);
                        }
                    }
                    return ret;
                }
            }
            if (visitingFirstAttemptedTier && prevPos >= 0)
                s.free(prevPos, prevChunks);
            s.nextTier();
        }
    }

    private class CleanupAction implements Consumer<ReplicableEntry> {
        int removedCompletely;
        long posToSkip;
        IterationContext<?, ?, ?> iterationContext;

        @Override
        public void accept(ReplicableEntry e) {
            ReplicatedChronicleMap<?, ?, ?> map = mh.m();
            if (!(e instanceof MapAbsentEntry) || iterationContext.pos() == posToSkip)
                return;
            long currentTime = currentTime();
            if (e.originTimestamp() > currentTime)
                return; // presumably unsynchronized clocks
            long deleteTimeout = systemTimeIntervalBetween(
                    e.originTimestamp(), currentTime, map.cleanupTimeoutUnit);
            if (deleteTimeout <= map.cleanupTimeout || e.isChanged())
                return;
            e.doRemoveCompletely();
            removedCompletely++;
        }
    }
}
