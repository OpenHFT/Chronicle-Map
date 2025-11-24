/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import org.jetbrains.annotations.NotNull;

/**
 * {@link InstanceReturnValue} implementation that always reports {@code null}
 * as the returned value.
 * <p>
 * This holder is used for map operations that are configured to return
 * {@code null} instead of the previous value while still satisfying the
 * {@link InstanceReturnValue} contract so that staged pipelines can pass a
 * writable return container. The wrapped {@link Data} supplied via
 * {@link #returnValue(Data)} is intentionally ignored.
 * <p>
 * This type is strictly internal to Chronicle-Map and is not part of the
 * supported public API.
 */
public final class NullReturnValue<V> implements InstanceReturnValue<V> {

    private static final NullReturnValue<?> NULL_RETURN_VALUE = new NullReturnValue<>();

    private NullReturnValue() {
    }

    @SuppressWarnings("unchecked")
    public static <V> InstanceReturnValue<V> get() {
        return (InstanceReturnValue<V>) NULL_RETURN_VALUE;
    }

    @Override
    public V returnValue() {
        return null;
    }

    @Override
    public void returnValue(@NotNull Data<V> value) {
        // ignore
    }
}
