/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import org.jetbrains.annotations.NotNull;

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
