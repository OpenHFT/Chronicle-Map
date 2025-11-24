/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceDataHolder;
import net.openhft.chronicle.set.SetContext;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * {@link MapContext} and {@link SetContext} implementation that wraps
 * on-heap values as {@link Data} instances.
 * <p>
 * The stage reuses {@link WrappedValueInstanceDataHolder} nodes to avoid
 * repeated allocation when callers request {@code wrapValueAsData}. It is used
 * by both map and set views to present a consistent data-access API to the
 * staged pipeline.
 */
@Staged
public abstract class WrappedValueInstanceDataHolderAccess<K, V, R>
        implements MapContext<K, V, R>, SetContext<K, R> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    WrappedValueInstanceDataHolder<V> wrappedValueInstanceDataHolder;

    @Override
    public Data<V> wrapValueAsData(V value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        WrappedValueInstanceDataHolder<V> wrapped = this.wrappedValueInstanceDataHolder;
        wrapped = wrapped.getUnusedWrappedValueHolder();
        wrapped.initValue(value);
        return wrapped.wrappedData;
    }
}
