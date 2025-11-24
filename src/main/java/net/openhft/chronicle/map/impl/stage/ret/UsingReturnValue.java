/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

/**
 * {@link UsableReturnValue} implementation that attempts to fill a
 * caller-provided instance in place.
 * <p>
 * The {@link #initUsingReturnValue(Object)} method establishes the object to
 * reuse; when {@link #returnValue(Data)} is invoked the data are read into that
 * instance and later exposed via {@link #returnValue()}. If the returned value
 * has not been initialised the method falls back to {@code null}.
 */
@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class UsingReturnValue<V> implements UsableReturnValue<V> {

    private V usingReturnValue = (V) USING_RETURN_VALUE_UNINIT;
    private V returnedValue = null;

    @Override
    public void initUsingReturnValue(V usingReturnValue) {
        this.usingReturnValue = usingReturnValue;
    }

    abstract boolean returnedValueInit();

    private void initReturnedValue(@NotNull Data<V> value) {
        returnedValue = value.getUsing(usingReturnValue);
    }

    @Override
    public void returnValue(@NotNull Data<V> value) {
        initReturnedValue(value);
    }

    @Override
    public V returnValue() {
        if (returnedValueInit()) {
            return returnedValue;
        } else {
            return null;
        }
    }
}
