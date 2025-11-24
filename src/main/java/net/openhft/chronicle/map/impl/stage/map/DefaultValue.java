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
 * Stage that exposes the default value used for absent map entries.
 * <p>
 * The default is represented by {@link DummyValueZeroData}, which reads a
 * logical value from a stream of zero bytes using the configured value
 * marshaller. This stage is typically used when a {@code ChronicleSet}
 * overlay needs a synthetic value.
 */
@Staged
public abstract class DefaultValue<V> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    DummyValueZeroData<V> zeroValueData;

    @NotNull
    public Data<V> defaultValue() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return zeroValueData;
    }
}
