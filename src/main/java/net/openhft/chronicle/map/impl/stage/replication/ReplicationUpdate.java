/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
    @StageRef SegmentStages s;
    @StageRef ReplicatedMapEntryStages<K, ?, ?> e;
    @StageRef ReplicatedChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;
    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;

    @Stage("ReplicationUpdate") public long innerRemoteTimestamp;
    @Stage("ReplicationUpdate") public byte innerRemoteIdentifier = (byte) 0;

    public abstract boolean replicationUpdateInit();

    public void initReplicationUpdate(long timestamp, byte identifier) {
        innerRemoteTimestamp = timestamp;
        if (identifier == 0)
            throw new IllegalStateException("identifier can't be 0");
        innerRemoteIdentifier = identifier;
    }
    
    public void dropChange() {
        mh.m().dropChange(s.segmentIndex, e.pos);
    }

    public void moveChange(long oldPos, long newPos) {
        mh.m().moveChange(s.segmentIndex, oldPos, newPos, e.timestamp());
    }
    
    public void updateChange() {
        if (!replicationUpdateInit()) {
            raiseChange();
        }
    }

    public void raiseChange() {
        mh.m().raiseChange(s.segmentIndex, e.pos, e.timestamp());
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
}
