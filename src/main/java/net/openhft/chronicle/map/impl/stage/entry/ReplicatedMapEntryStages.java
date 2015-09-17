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

package net.openhft.chronicle.map.impl.stage.entry;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.map.ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES;

@Staged
public abstract class ReplicatedMapEntryStages<K, V, T> extends MapEntryStages<K, V>
        implements MapReplicableEntry<K, V>, MapAbsentEntry<K, V> {
    
    @StageRef ReplicatedChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;
    @StageRef ReplicationUpdate ru;

    @Stage("ReplicationState") long replicationBytesOffset = -1;

    void initReplicationState() {
        replicationBytesOffset = keyEnd();
    }

    void updateReplicationState(long timestamp, byte identifier) {
        s.segmentBytes.position(replicationBytesOffset);
        s.segmentBytes.writeLong(timestamp);
        s.segmentBytes.writeByte(identifier);
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

    @Override
    public boolean isChanged() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerReadLock.lock();
        return ru.changed();
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
        long oldTierIndex = s.tierIndex;
        super.relocation(newValue, newSizeOfEverythingBeforeValue);
        ru.moveChange(oldTierIndex, oldPos, pos);
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

    @NotNull
    @Override
    public Data<K> absentKey() {
        return key();
    }
}
