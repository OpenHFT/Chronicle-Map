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

package net.openhft.chronicle.map.impl.stage.replication;

import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.query.QueryAlloc;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapSegmentContext;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Consumer;

@Staged
public class ReplicatedQueryAlloc extends QueryAlloc {

    @StageRef ReplicatedChronicleMapHolder<?, ?, ?> mh;
    @StageRef SegmentStages s;

    final CleanupAction cleanupAction = new CleanupAction();

    /**
     * Returns {@code true} if at least one old deleted entry was removed.
     */
    public boolean forcedOldDeletedEntriesCleanup() {
        ReplicatedChronicleMap<?, ?, ?> map = mh.m();
        try (MapSegmentContext<?, ?, ?> sc = map.segmentContext(s.segmentIndex)) {
            cleanupAction.removedCompletely = 0;
            ((ReplicatedHashSegmentContext<?, ?>) sc)
                    .forEachSegmentReplicableEntry(cleanupAction);
            return cleanupAction.removedCompletely > 0;
        }
    }

    private class CleanupAction implements Consumer<ReplicableEntry> {
        int removedCompletely;

        @Override
        public void accept(ReplicableEntry e) {
            ReplicatedChronicleMap<?, ?, ?> map = mh.m();
            if (e instanceof MapAbsentEntry) {
                long deleteTimeout = map.timeProvider.currentTime() - e.originTimestamp();
                map.timeProvider.systemTimeIntervalBetween(e.originTimestamp(),
                        map.timeProvider.currentTime(), map.cleanupTimeoutUnit);
                if (deleteTimeout > map.cleanupTimeout && !e.isChanged()) {
                    e.doRemoveCompletely();
                    removedCompletely++;
                }
            }
        }
    }

    @Override
    public long alloc(int chunks) {
        long ret = s.allocReturnCode(chunks);
        if (ret >= 0)
            return ret;
        int alreadyAttemptedTier;
        if (!forcedOldDeletedEntriesCleanup()) {
            alreadyAttemptedTier = s.segmentTier;
        } else {
            // If forced old deleted entries cleanup deleted something, don't skip the first queried
            // tier, because it could have new empty chunks now. segment tier is >= 0, so set to -1
            // to make the condition with alreadyAttemptedTier always false
            alreadyAttemptedTier = -1;
        }
        s.goToFirstTier();
        while (true) {
            if (s.segmentTier != alreadyAttemptedTier) {
                ret = s.allocReturnCode(chunks);
                if (ret >= 0)
                    return ret;
            }
            s.nextTier();
        }
    }
}
