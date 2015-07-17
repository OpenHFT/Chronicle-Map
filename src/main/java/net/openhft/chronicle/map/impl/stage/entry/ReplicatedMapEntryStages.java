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

package net.openhft.chronicle.map.impl.stage.entry;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.map.ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES;

@Staged
public abstract class ReplicatedMapEntryStages<K, V, T> extends MapEntryStages<K, V>
        implements MapReplicableEntry<K, V> {
    
    @StageRef ReplicatedChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;
    @StageRef ReplicationUpdate ru;

    @Stage("ReplicationState") long replicationBytesOffset = -1;

    void initReplicationState() {
        replicationBytesOffset = keyEnd();
    }

    void updateReplicationState(long timestamp, byte identifier) {
        entryBytes.position(replicationBytesOffset);
        entryBytes.writeLong(timestamp);
        entryBytes.writeByte(identifier);
    }

    private long timestampOffset() {
        return replicationBytesOffset;
    }

    public long timestamp() {
        return entryBS.readLong(replicationBytesOffset);
    }

    private long identifierOffset() {
        return replicationBytesOffset + 8L;
    }

    byte identifier() {
        return entryBS.readByte(identifierOffset());
    }

    private long entryDeletedOffset() {
        return replicationBytesOffset + 9L;
    }

    public boolean entryDeleted() {
        return entryBS.readBoolean(entryDeletedOffset());
    }

    public void writeEntryPresent() {
        entryBS.writeBoolean(entryDeletedOffset(), false);
    }

    public void writeEntryDeleted() {
        entryBS.writeBoolean(entryDeletedOffset(), true);
    }

    @Override
    public byte originIdentifier() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return identifier();
    }

    @Override
    public long originTimestamp() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return timestamp();
    }

    @Override
    long countValueSizeOffset() {
        return super.countValueSizeOffset() + ADDITIONAL_ENTRY_BYTES;
    }

    @Override
    public void updateOrigin(byte newIdentifier, long newTimestamp) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerWriteLock.lock();
        updateReplicationState(newTimestamp, newIdentifier);
    }

    @Override
    public void dropChanged() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        ru.dropChange();
    }

    @Override
    public void raiseChanged() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        ru.raiseChange();
    }

    public void updatedReplicationStateOnPresentEntry() {
        if (!ru.replicationUpdateInit()) {
            s.innerWriteLock.lock();
            long timestamp;
            if (identifier() != mh.m().identifier()) {
                timestamp = Math.max(timestamp() + 1, mh.m().timeProvider.currentTime());
            } else {
                timestamp = mh.m().timeProvider.currentTime();
            }
            updateReplicationState(timestamp, mh.m().identifier());
        }
    }

    public void updatedReplicationStateOnAbsentEntry() {
        if (!ru.replicationUpdateInit()) {
            s.innerWriteLock.lock();
            updateReplicationState(mh.m().timeProvider.currentTime(), mh.m().identifier());
        }
    }

    @Override
    protected void relocation(Data<V> newValue, long newSizeOfEverythingBeforeValue) {
        long oldPos = pos;
        super.relocation(newValue, newSizeOfEverythingBeforeValue);
        ru.moveChange(oldPos, pos);
    }

    private boolean testTimeStampInSensibleRange() {
        if (mh.m().timeProvider == TimeProvider.SYSTEM) {
            long currentTime = TimeProvider.SYSTEM.currentTime();
            assert Math.abs(currentTime - timestamp()) <= 100000000 :
                    "unrealistic timestamp: " + timestamp();
            assert Math.abs(currentTime - ru.innerRemoteTimestamp) <= 100000000 :
                    "unrealistic innerRemoteTimestamp: " + ru.innerRemoteTimestamp;
        }
        return true;
    }

    @Override
    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return super.sizeOfEverythingBeforeValue(keySize, valueSize) + ADDITIONAL_ENTRY_BYTES;
    }
    
    @Override
    public void putValueDeletedEntry(Data<V> newValue) {
        throw new AssertionError("Replicated Map doesn't remove entries truly, yet");
    }
}
