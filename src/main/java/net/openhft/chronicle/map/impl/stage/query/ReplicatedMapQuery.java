/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.replication.ReplicationUpdate;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
import net.openhft.chronicle.set.replication.SetRemoteQueryContext;
import net.openhft.chronicle.set.replication.SetReplicableEntry;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.Nullable;

/**
 * Replication-aware {@link MapQuery} implementation.
 * <p>
 * This stage combines the standard map query behaviour with the
 * {@link MapRemoteQueryContext}, {@link SetRemoteQueryContext} and
 * {@link ReplicableEntry} contracts so that the same pipeline can be used both
 * for local API calls and for applying replicated updates. It is responsible
 * for honouring deletion markers, updating replication state and exposing the
 * correct absent-entry view.
 */
@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class ReplicatedMapQuery<K, V, R> extends MapQuery<K, V, R>
        implements MapRemoteQueryContext<K, V, R>, SetRemoteQueryContext<K, R>,
        ReplicableEntry, MapReplicableEntry<K, V>, SetReplicableEntry<K> {

    @StageRef
    ReplicatedMapEntryStages<K, V> e;
    @StageRef
    ReplicationUpdate ru;

    @StageRef
    ReplicatedMapAbsentDelegating<K, V> absentDelegating;
    @StageRef
    DummyValueZeroData<V> dummyValue;

    @Nullable
    @Override
    public Absent<K, V> absentEntry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (entryPresent()) {
            return null;
        } else {
            if (!ks.searchStatePresent()) {
                return absentDelegating;
            } else {
                assert e.entryDeleted();
                return absent;
            }
        }
    }

    @Override
    public boolean entryPresent() {
        return super.entryPresent() && !e.entryDeleted();
    }

    @Override
    public ReplicatedMapQuery<K, V, R> entry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        if (entryPresent()) {
            if (e.valueSize > dummyValue.size())
                e.innerDefaultReplaceValue(dummyValue);
            e.updatedReplicationStateOnPresentEntry();
            e.writeEntryDeleted();
            ru.updateChange();
            s.tierDeleted(s.tierDeleted() + 1);
        } else {
            throw new IllegalStateException(mh.h().toIdentityString() +
                    ": Entry is absent in the map when doRemove() is called");
        }
    }

    @Override
    public void doRemoveCompletely() {
        boolean wasDeleted = e.entryDeleted();
        super.doRemove();
        ru.dropChange();
        if (wasDeleted)
            s.tierDeleted(s.tierDeleted() - 1L);
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        super.doReplaceValue(newValue);
        e.updatedReplicationStateOnPresentEntry();
        ru.updateChange();
    }
}
