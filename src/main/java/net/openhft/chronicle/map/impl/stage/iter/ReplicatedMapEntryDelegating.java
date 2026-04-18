/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.replication.ReplicableEntryDelegating;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.stage.entry.ReplicatedMapEntryStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class ReplicatedMapEntryDelegating<K, V>
        implements MapEntry<K, V>, ReplicableEntryDelegating {

    @StageRef
    ReplicatedMapSegmentIteration<K, V, ?> delegate;
    @StageRef
    ReplicatedMapEntryStages<K, V> e;

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        return delegate.context();
    }

    @NotNull
    @Override
    public Data<K> key() {
        return delegate.key();
    }

    @NotNull
    @Override
    public Data<V> value() {
        return delegate.value();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        delegate.doReplaceValue(newValue);
    }

    @Override
    public void doRemove() {
        delegate.doRemove();
    }

    @Override
    public ReplicableEntry d() {
        return e;
    }
}
