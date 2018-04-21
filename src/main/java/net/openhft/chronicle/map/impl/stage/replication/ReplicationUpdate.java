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

package net.openhft.chronicle.map.impl.stage.replication;

import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class ReplicationUpdate<K> implements RemoteOperationContext<K> {
    @Stage("ReplicationUpdate")
    public byte innerRemoteIdentifier = (byte) 0;
    @Stage("ReplicationUpdate")
    public long innerRemoteTimestamp;
    @Stage("ReplicationUpdate")
    public byte innerRemoteNodeIdentifier;
    @StageRef
    SegmentStages s;
    @StageRef
    ReplicatedMapEntryStages<K, ?> e;
    @StageRef
    ReplicatedChronicleMapHolder<?, ?, ?> mh;
    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;

    public abstract boolean replicationUpdateInit();

    public void initReplicationUpdate(byte identifier, long timestamp, byte remoteNodeIdentifier) {
        innerRemoteTimestamp = timestamp;
        if (identifier == 0)
            throw new IllegalStateException(mh.h().toIdentityString() + ": identifier can't be 0");
        innerRemoteIdentifier = identifier;
        if (remoteNodeIdentifier == 0) {
            throw new IllegalStateException(
                    mh.h().toIdentityString() + ": remote node identifier can't be 0");
        }
        innerRemoteNodeIdentifier = remoteNodeIdentifier;
    }

    public void dropChange() {
        mh.m().dropChange(s.tierIndex, e.pos);
    }

    public void dropChangeFor(byte remoteIdentifier) {
        mh.m().dropChangeFor(s.tierIndex, e.pos, remoteIdentifier);
    }

    public void moveChange(long oldTierIndex, long oldPos, long newPos) {
        mh.m().moveChange(oldTierIndex, oldPos, s.tierIndex, newPos);
    }

    public void updateChange() {
        if (!replicationUpdateInit()) {
            raiseChange();
        }
    }

    public void raiseChange() {
        mh.m().raiseChange(s.tierIndex, e.pos);
    }

    public void raiseChangeFor(byte remoteIdentifier) {
        mh.m().raiseChangeFor(s.tierIndex, e.pos, remoteIdentifier);
    }

    public void raiseChangeForAllExcept(byte remoteIdentifier) {
        mh.m().raiseChangeForAllExcept(s.tierIndex, e.pos, remoteIdentifier);
    }

    public boolean changed() {
        return mh.m().isChanged(s.tierIndex, e.pos);
    }

    @Override
    public long remoteTimestamp() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerRemoteTimestamp;
    }

    @Override
    public byte remoteIdentifier() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerRemoteIdentifier;
    }

    @Override
    public byte remoteNodeIdentifier() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerRemoteNodeIdentifier;
    }

    @Override
    public byte currentNodeIdentifier() {
        return mh.m().identifier();
    }
}
