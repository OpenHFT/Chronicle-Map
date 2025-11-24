/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.set.SetAbsentEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Unified absent-entry view for Chronicle-Map and Chronicle-Set queries.
 * <p>
 * Implementations represent a key that is not currently present in the map or
 * set and provide access to the shared {@link MapAndSetContext}. They are
 * created by staged query contexts and are not intended to be stored or used
 * outside the lifetime of that context.
 */
public interface Absent<K, V> extends MapAbsentEntry<K, V>, SetAbsentEntry<K> {

    @NotNull
    @Override
    MapAndSetContext<K, V, ?> context();
}
