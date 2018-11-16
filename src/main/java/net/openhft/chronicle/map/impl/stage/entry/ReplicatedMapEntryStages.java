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

package net.openhft.chronicle.map.impl.stage.entry;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.replication.TimeProvider.currentTime;
import static net.openhft.chronicle.map.ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES;

@Staged
public abstract class ReplicatedMapEntryStages<K, V> extends MapEntryStages<K, V>
        implements MapReplicableEntry<K, V> {

    @StageRef
    ReplicatedChronicleMapHolder<?, ?, ?> mh;
    @StageRef
    ReplicationUpdate ru;

    @Stage("ReplicationState")
    long replicationBytesOffset = -1;

    void initReplicationState() {
        replicationBytesOffset = keyEnd();
    }

    void updateReplicationState(byte identifier, long timestamp) {
        initDelayedUpdateChecksum(true);
        Bytes segmentBytes = s.segmentBytesForWrite();
        segmentBytes.writePosition(replicationBytesOffset);
        segmentBytes.writeLong(timestamp);
        segmentBytes.writeByte(identifier);
    }

    private long timestampOffset() {
        return replicationBytesOffset;
    }

    public long timestamp() {
        return s.segmentBS.readLong(replicationBytesOffset);
    }

    private long identifierOffset() {
        return replicationBytesOffset + 8L;
    }

    byte identifier() {
        return s.segmentBS.readByte(identifierOffset());
    }

    private long entryDeletedOffset() {
        return replicationBytesOffset + 9L;
    }

    @Override
    public boolean entryDeleted() {
        return s.segmentBS.readBoolean(entryDeletedOffset());
    }

    public void writeEntryPresent() {
        s.segmentBS.writeBoolean(entryDeletedOffset(), false);
    }

    public void writeEntryDeleted() {
        s.segmentBS.writeBoolean(entryDeletedOffset(), true);
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
        updateReplicationState(newIdentifier, newTimestamp);
    }

    @Override
    public void dropChanged() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        ru.dropChange();
    }

    @Override
    public void dropChangedFor(byte remoteIdentifier) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        ru.dropChangeFor(remoteIdentifier);
    }

    @Override
    public void raiseChanged() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        ru.raiseChange();
    }

    @Override
    public void raiseChangedFor(byte remoteIdentifier) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        ru.raiseChangeFor(remoteIdentifier);
    }

    @Override
    public void raiseChangedForAllExcept(byte remoteIdentifier) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        ru.raiseChangeForAllExcept(remoteIdentifier);
    }

    @Override
    public boolean isChanged() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerReadLock.lock();
        return ru.changed();
    }

    public void updatedReplicationStateOnPresentEntry() {
        if (!ru.replicationUpdateInit()) {
            s.innerWriteLock.lock();
            long timestamp = Math.max(timestamp() + 1, currentTime());
            updateReplicationState(mh.m().identifier(), timestamp);
        }
    }

    public void updatedReplicationStateOnAbsentEntry() {
        if (!ru.replicationUpdateInit()) {
            s.innerWriteLock.lock();
            updateReplicationState(mh.m().identifier(), currentTime());
        }
    }

    @Override
    protected void relocation(Data<V> newValue, long newEntrySize) {
        long oldPos = pos;
        long oldTierIndex = s.tierIndex;
        super.relocation(newValue, newEntrySize);
        ru.moveChange(oldTierIndex, oldPos, pos);
    }

    @Override
    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return super.sizeOfEverythingBeforeValue(keySize, valueSize) + ADDITIONAL_ENTRY_BYTES;
    }
}
