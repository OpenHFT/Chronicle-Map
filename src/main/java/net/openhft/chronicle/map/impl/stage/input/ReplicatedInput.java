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

package net.openhft.chronicle.map.impl.stage.input;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.map.Replica;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.query.ReplicatedMapQuery;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class ReplicatedInput<K, V, R> implements RemoteOperationContext<K>,
        MapRemoteQueryContext<K, V, R>, Replica.QueryContext<K, V> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    ReplicatedChronicleMapHolder<K, V, R> mh;
    @StageRef
    ReplicationUpdate<K> ru;
    @StageRef
    ReplicatedMapQuery<K, V, ?> q;
    @StageRef
    SegmentStages s;
    @StageRef
    DummyValueZeroData<V> dummyValue;

    @Override
    public Data<V> dummyZeroValue() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return dummyValue;
    }

    public void processReplicatedEvent(byte remoteNodeIdentifier, Bytes replicatedInputBytes) {
        long timestamp = replicatedInputBytes.readStopBit();
        byte identifier = replicatedInputBytes.readByte();
        ru.initReplicationUpdate(identifier, timestamp, remoteNodeIdentifier);

        boolean isDeleted = replicatedInputBytes.readBoolean();
        long keySize = mh.m().keySizeMarshaller.readSize(replicatedInputBytes);
        long keyOffset = replicatedInputBytes.readPosition();

        q.initInputKey(q.getInputKeyBytesAsData(replicatedInputBytes, keyOffset, keySize));
        replicatedInputBytes.readSkip(keySize);
        if (isDeleted) {
            s.innerUpdateLock.lock();
            mh.m().remoteOperations.remove(this);
        } else {
            long valueSize = mh.m().valueSizeMarshaller.readSize(replicatedInputBytes);
            long valueOffset = replicatedInputBytes.readPosition();
            Data<V> value = q.wrapValueBytesAsData(replicatedInputBytes, valueOffset, valueSize);
            replicatedInputBytes.readSkip(valueSize);
            s.innerWriteLock.lock();
            mh.m().remoteOperations.put(this, value);
        }
    }

    @Override
    public void remotePut(
            Data<V> newValue,
            byte remoteEntryIdentifier, long remoteEntryTimestamp, byte remoteNodeIdentifier) {
        ru.initReplicationUpdate(remoteEntryIdentifier, remoteEntryTimestamp, remoteNodeIdentifier);
        s.innerUpdateLock.lock();
        mh.m().remoteOperations.put(this, newValue);
    }

    @Override
    public void remoteRemove(
            byte remoteEntryIdentifier, long remoteEntryTimestamp, byte remoteNodeIdentifier) {
        ru.initReplicationUpdate(remoteEntryIdentifier, remoteEntryTimestamp, remoteNodeIdentifier);
        s.innerWriteLock.lock();
        mh.m().remoteOperations.remove(this);
    }
}
