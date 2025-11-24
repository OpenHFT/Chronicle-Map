/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.ret;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

/**
 * {@link InstanceReturnValue} implementation that returns a default snapshot
 * of the value selected by the query.
 * <p>
 * The first call to {@link #returnValue(Data)} decodes the supplied data into
 * an object instance and stores it as the default result. Subsequent calls to
 * {@link #returnValue()} either return that instance or {@code null} if no
 * value has been initialised, mirroring the semantics of the public
 * {@code ChronicleMap} methods.
 */
@Staged
public abstract class DefaultReturnValue<V> implements InstanceReturnValue<V> {
    private V defaultReturnedValue = null;

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
