/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

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
