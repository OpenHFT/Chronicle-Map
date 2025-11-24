/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

/**
 * Staged adapter that applies replicated updates to a Chronicle-Map segment.
 * <p>
 * Implementations read replication events from a {@link Bytes} stream,
 * initialise {@link ReplicationUpdate} with the remote identifiers and then
 * route {@code put} or {@code remove} operations through the map's
 * {@code remoteOperations} API. The class also provides access to a dummy
 * zero value via {@link #dummyZeroValue()} for set-style semantics.
 */
@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
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
