/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

/**
 * {@link MapContext} implementation that delegates core entry operations to
 * the owning {@link net.openhft.chronicle.map.VanillaChronicleMap}.
 * <p>
 * The staged map contexts call into this class for {@code get}, {@code put}
 * and {@code remove} style operations, which in turn forward to the map's
 * configured entry operations and default value provider. This keeps the
 * staged code generic while allowing different maps to plug in custom
 * behaviour.
 */
@Staged
public abstract class MapEntryOperationsDelegation<K, V, R> implements MapContext<K, V, R> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    VanillaChronicleMapHolder<K, V, R> mh;

    @Override
    public R replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.replaceValue(entry, newValue);
    }

    @Override
    public R remove(@NotNull MapEntry<K, V> entry) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.remove(entry);
    }

    @Override
    public Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().defaultValueProvider.defaultValue(absentEntry);
    }

    @Override
    public R insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return mh.m().entryOperations.insert(absentEntry, value);
    }
}
