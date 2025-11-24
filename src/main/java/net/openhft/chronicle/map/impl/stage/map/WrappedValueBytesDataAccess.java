/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.util.Objects;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.impl.stage.data.bytes.WrappedValueBytesData;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * {@link MapContext} implementation that wraps external bytes as map values.
 * <p>
 * The {@link #wrapValueBytesAsData(BytesStore, long, long)} method obtains a
 * reusable {@link WrappedValueBytesData} instance, initialises it to point at
 * the supplied {@link BytesStore} region and returns it as a {@code Data}
 * view. This is used heavily in replication paths where values arrive as raw
 * bytes.
 */
@Staged
public abstract class WrappedValueBytesDataAccess<K, V, R> implements MapContext<K, V, R> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    WrappedValueBytesData<V> wrappedValueBytesData;

    @Override
    public Data<V> wrapValueBytesAsData(BytesStore<?, ?> bytesStore, long offset, long size) {
        Objects.requireNonNull(bytesStore);
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        WrappedValueBytesData<V> wrapped = this.wrappedValueBytesData;
        wrapped = wrapped.getUnusedWrappedValueBytesData();
        wrapped.initWrappedValueBytesStore(bytesStore, offset, size);
        return wrapped;
    }
}
