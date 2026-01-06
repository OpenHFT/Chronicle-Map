/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

/**
 * Supplies a default zero value for map entries when required.
 */
@Staged
public abstract class DefaultValue<V> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    DummyValueZeroData<V> zeroValueData;

    /**
     * Protected constructor for staged subclasses.
     */
    protected DefaultValue() {
    }

    @NotNull
    /**
     * Returns the default value data instance, after performing access checks.
     *
     * @return zero value data
     */
    public Data<V> defaultValue() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return zeroValueData;
    }
}
