//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.replication.ReplicableEntryDelegating;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.DefaultValue;
import net.openhft.chronicle.map.impl.stage.map.WrappedValueInstanceDataHolderAccess;
import net.openhft.chronicle.set.SetAbsentEntry;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class ReplicatedMapAbsentDelegatingForIteration<K, V>
        implements MapAbsentEntry<K, V>, SetAbsentEntry<K>, ReplicableEntryDelegating {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    ReplicatedMapSegmentIteration<K, V, ?> delegate;
    @StageRef
    ReplicatedMapEntryStages<K, V> e;
    @StageRef
    DefaultValue<V> defaultValue;

    @NotNull
    @Override
    public WrappedValueInstanceDataHolderAccess<K, V, ?> context() {
        return delegate.context();
    }

    @Override
    public void doInsert(Data<V> value) {
        delegate.doInsert(value);
    }

    @Override
    public void doInsert() {
        delegate.doInsert();
    }

    @NotNull
    @Override
    public Data<V> defaultValue() {
        return defaultValue.defaultValue();
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return e.entryKey;
    }

    @Override
    public ReplicableEntry d() {
        return e;
    }
}
