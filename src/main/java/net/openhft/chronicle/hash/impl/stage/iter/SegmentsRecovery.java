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

import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.hash.impl.TierCountersArea;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.map.ChronicleHashCorruptionImpl;
import net.openhft.chronicle.map.impl.IterationContext;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.format;
import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.report;

@Staged
public abstract class SegmentsRecovery implements IterationContext {

    @StageRef
    VanillaChronicleHashHolder<?> hh;
    @StageRef
    SegmentStages s;
    @StageRef
    TierRecovery tierRecovery;

    @Override
    public void recoverSegments(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        VanillaChronicleHash<?, ?, ?, ?> h = hh.h();
        for (int segmentIndex = 0; segmentIndex < h.actualSegments; segmentIndex++) {
            s.initSegmentIndex(segmentIndex);
            resetSegmentLock(corruptionListener, corruption);
            zeroOutFirstSegmentTierCountersArea(corruptionListener, corruption);
            tierRecovery.recoverTier(segmentIndex, corruptionListener, corruption);
        }

        VanillaGlobalMutableState globalMutableState = h.globalMutableState();
        long storedExtraTiersInUse = globalMutableState.getExtraTiersInUse();
        long allocatedExtraTiers = globalMutableState.getAllocatedExtraTierBulks() * h.tiersInBulk;
        long expectedExtraTiersInUse =
                Math.max(0, Math.min(storedExtraTiersInUse, allocatedExtraTiers));
        long actualExtraTiersInUse = 0;
        long firstFreeExtraTierIndex = -1;
        for (long extraTierIndex = 0; extraTierIndex < expectedExtraTiersInUse; extraTierIndex++) {
            long tierIndex = h.extraTierIndexToTierIndex(extraTierIndex);
            // `tier` is unused in recoverTier(), 0 should be a safe value
            s.initSegmentTier(0, tierIndex);
            int segmentIndex = tierRecovery.recoverTier(
                    -1, corruptionListener, corruption);
            if (segmentIndex >= 0) {
                long tierCountersAreaAddr = s.tierCountersAreaAddr();
                int storedSegmentIndex = TierCountersArea.segmentIndex(tierCountersAreaAddr);
                if (storedSegmentIndex != segmentIndex) {
                    report(corruptionListener, corruption, segmentIndex, () ->
                            format("wrong segment index stored in tier counters area " +
                                            "of tier with index {}: {}, should be, based on entries: {}",
                                    tierIndex, storedSegmentIndex, segmentIndex)
                    );
                    TierCountersArea.segmentIndex(tierCountersAreaAddr, segmentIndex);
                }
                s.nextTierIndex(0);

                s.initSegmentIndex(segmentIndex);
                s.goToLastTier();
                s.nextTierIndex(tierIndex);

                TierCountersArea.prevTierIndex(tierCountersAreaAddr, s.tierIndex);
                TierCountersArea.tier(tierCountersAreaAddr, s.tier + 1);
                actualExtraTiersInUse = extraTierIndex + 1;
            } else {
                firstFreeExtraTierIndex = extraTierIndex;
                break;
            }
        }

        if (storedExtraTiersInUse != actualExtraTiersInUse) {
            long finalActualExtraTiersInUse = actualExtraTiersInUse;
            report(corruptionListener, corruption, -1, () ->
                    format("wrong number of actual tiers in use in global mutable state, stored: {}, " +
                            "should be: {}", storedExtraTiersInUse, finalActualExtraTiersInUse)
            );
            globalMutableState.setExtraTiersInUse(actualExtraTiersInUse);
        }

        long firstFreeTierIndex;
        if (firstFreeExtraTierIndex == -1) {
            if (allocatedExtraTiers > expectedExtraTiersInUse) {
                firstFreeTierIndex = h.extraTierIndexToTierIndex(expectedExtraTiersInUse);
            } else {
                firstFreeTierIndex = 0;
            }
        } else {
            firstFreeTierIndex = h.extraTierIndexToTierIndex(firstFreeExtraTierIndex);
        }
        if (firstFreeTierIndex > 0) {
            long lastTierIndex = h.extraTierIndexToTierIndex(allocatedExtraTiers - 1);
            h.linkAndZeroOutFreeTiers(firstFreeTierIndex, lastTierIndex);
        }
        long storedFirstFreeTierIndex = globalMutableState.getFirstFreeTierIndex();
        if (storedFirstFreeTierIndex != firstFreeTierIndex) {
            report(corruptionListener, corruption, -1, () ->
                    format("wrong first free tier index in global mutable state, stored: {}, " +
                            "should be: {}", storedFirstFreeTierIndex, firstFreeTierIndex)
            );
            globalMutableState.setFirstFreeTierIndex(firstFreeTierIndex);
        }

        removeDuplicatesInSegments(corruptionListener, corruption);
    }

    private void removeDuplicatesInSegments(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        VanillaChronicleHash<?, ?, ?, ?> h = hh.h();
        for (int segmentIndex = 0; segmentIndex < h.actualSegments; segmentIndex++) {
            s.initSegmentIndex(segmentIndex);
            s.initSegmentTier();
            s.goToLastTier();
            while (true) {
                tierRecovery.removeDuplicatesInSegment(corruptionListener, corruption);
                if (s.tier > 0) {
                    s.prevTier();
                } else {
                    break;
                }
            }
        }
    }

    private void resetSegmentLock(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        long lockState = s.segmentHeader.getLockState(s.segmentHeaderAddress);
        if (lockState != s.segmentHeader.resetLockState()) {
            report(corruptionListener, corruption, s.segmentIndex, () ->
                    format("lock of segment {} is not clear: {}",
                            s.segmentIndex, s.segmentHeader.lockStateToString(lockState))
            );
            s.segmentHeader.resetLock(s.segmentHeaderAddress);
        }
    }

    private void zeroOutFirstSegmentTierCountersArea(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        s.nextTierIndex(0);
        if (s.prevTierIndex() != 0) {
            report(corruptionListener, corruption, s.segmentIndex, () ->
                    format("stored prev tier index in first tier of segment {}: {}, should be 0",
                            s.segmentIndex, s.prevTierIndex())
            );
            s.prevTierIndex(0);
        }
        long tierCountersAreaAddr = s.tierCountersAreaAddr();
        if (TierCountersArea.segmentIndex(tierCountersAreaAddr) != 0) {
            report(corruptionListener, corruption, s.segmentIndex, () ->
                    format("stored segment index in first tier of segment {}: {}, should be 0",
                            s.segmentIndex, TierCountersArea.segmentIndex(tierCountersAreaAddr))
            );
            TierCountersArea.segmentIndex(tierCountersAreaAddr, 0);
        }
        if (TierCountersArea.tier(tierCountersAreaAddr) != 0) {
            report(corruptionListener, corruption, s.segmentIndex, () ->
                    format("stored tier in first tier of segment {}: {}, should be 0",
                            s.segmentIndex, TierCountersArea.tier(tierCountersAreaAddr))
            );
            TierCountersArea.tier(tierCountersAreaAddr, 0);
        }
    }
}
