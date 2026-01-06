/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

/**
 * Base implementation of {@link InstanceReturnValue} that caches a default value from data.
 */
@Staged
public abstract class DefaultReturnValue<V> implements InstanceReturnValue<V> {
    private V defaultReturnedValue = null;

    /**
     * Protected constructor for staged subclasses.
     */
    protected DefaultReturnValue() {
    }

    abstract boolean defaultReturnedValueInit();

    private void initDefaultReturnedValue(@NotNull Data<V> value) {
        defaultReturnedValue = value.getUsing(null);
    }

    @Override
    public void returnValue(@NotNull Data<V> value) {
        initDefaultReturnedValue(value);
    }

    @Override
    public V returnValue() {
        if (defaultReturnedValueInit()) {
            return defaultReturnedValue;
        } else {
            return null;
        }
    }
}
