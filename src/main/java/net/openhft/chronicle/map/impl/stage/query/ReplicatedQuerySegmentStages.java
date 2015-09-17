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

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.ReplicatedHashSegmentContext;
import net.openhft.chronicle.hash.impl.stage.query.QuerySegmentStages;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapSegmentContext;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Consumer;

@Staged
public abstract class ReplicatedQuerySegmentStages extends QuerySegmentStages {

    @StageRef
    ReplicatedChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;

    final Consumer<ReplicableEntry> cleanupAction = e -> {
        ReplicatedChronicleMap<?, ?, ?, ?, ?, ?, ?> map = mh.m();
        if (e instanceof MapAbsentEntry) {
            long deleteTimeout = map.timeProvider.currentTime() - e.originTimestamp();
            if (deleteTimeout > map.cleanupTimeout && !e.isChanged())
                e.doRemoveCompletely();
        }
    };

    @Override
    public long alloc(int chunks) {
        while (true) {
            long ret = allocReturnCode(chunks);
            if (ret >= 0) {
                return ret;
            } else {
                ReplicatedChronicleMap<?, ?, ?, ?, ?, ?, ?> map = mh.m();
                MapSegmentContext<?, ?, ?> sc = map.segmentContext(segmentIndex);
                ((ReplicatedHashSegmentContext<?, ?>) sc)
                        .forEachSegmentReplicableEntry(cleanupAction);
                ret = allocReturnCode(chunks);
                if (ret >= 0) {
                    return ret;
                } else {
                    nextTier();
                }
            }
        }
    }
}
