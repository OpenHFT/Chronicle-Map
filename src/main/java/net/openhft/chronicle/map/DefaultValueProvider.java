/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import org.jetbrains.annotations.NotNull;

/**
 * Default value computation strategy, used
 * in {@link ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)} configuration.
 *
 * @param <K> map key class
 * @param <V> map value class
 */
@FunctionalInterface
public interface DefaultValueProvider<K, V> {

    /**
     * Returns the "nil" value, which should be inserted into the map, in the given
     * {@code absentEntry} context. This is primarily used in {@link ChronicleMap#acquireUsing}
     * operation implementation, i. e. {@link MapMethods#acquireUsing}.
     * <p>
     * The default implementation simply delegates to {@link MapAbsentEntry#defaultValue()}.
     */
    Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry);
}
