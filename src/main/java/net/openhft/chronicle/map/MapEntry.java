/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.set.SetEntry;
import org.jetbrains.annotations.NotNull;

/**
 * A context of a <i>present</i> entry in the {@link ChronicleMap}.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @see MapEntryOperations
 * @see MapQueryContext#entry()
 */
public interface MapEntry<K, V> extends SetEntry<K> {
    @Override
    @NotNull
    MapContext<K, V, ?> context();

    /**
     * Returns the entry value.
     *
     * @return value data
     */
    @NotNull
    Data<V> value();

    /**
     * Replaces the entry's value with the given {@code newValue}.
     * <p>
     * This method is the default implementation for {@link MapEntryOperations#replaceValue(
     *MapEntry, Data)}, which might be customized over the default.
     *
     * @param newValue the value to be put into the map instead of the {@linkplain #value() current
     *                 value}
     * @throws IllegalStateException if some locking/state conditions required to perform replace
     *                               operation are not met
     */
    void doReplaceValue(Data<V> newValue);

    /**
     * Removes the entry from the map.
     * <p>
     * This method is the default implementation for {@link MapEntryOperations#remove(MapEntry)},
     * which might be customized over the default.
     */
    @Override
    void doRemove();
}
