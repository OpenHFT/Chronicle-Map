/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.iter.HashSegmentIteration;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.IterationContext;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceDataHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.WrappedValueInstanceDataHolderAccess;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class MapSegmentIteration<K, V, R> extends HashSegmentIteration<K, MapEntry<K, V>>
        implements MapEntry<K, V>, IterationContext<K, V, R> {

    @StageRef
    MapEntryStages<K, V> entry;
    @StageRef
    WrappedValueInstanceDataHolder<V> wrappedValueInstanceDataHolder;
    @StageRef
    WrappedValueInstanceDataHolderAccess<K, V, ?> wrappedValueInstanceDataHolderAccess;

    @Override
    public void hookAfterEachIteration() {
        throwExceptionIfClosed();

        wrappedValueInstanceDataHolder.closeValue();
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        throwExceptionIfClosed();

        checkOnEachPublicOperation.checkOnEachPublicOperation();
        try {
            entry.innerDefaultReplaceValue(newValue);
        } finally {
            s.innerWriteLock.unlock();
        }
    }

    @NotNull
    @Override
    public WrappedValueInstanceDataHolderAccess<K, V, ?> context() {
        return wrappedValueInstanceDataHolderAccess;
    }
}
