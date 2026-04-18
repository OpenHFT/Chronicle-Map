/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.HashQueryContext;
import net.openhft.chronicle.map.replication.MapRemoteOperations;
import org.jetbrains.annotations.Nullable;

/**
 * A context of {@link ChronicleMap} operations with <i>individual keys</i>
 * (like during {@code get()}, {@code put()}, etc., opposed to <i>bulk</i> operations).
 * This is the main context type of {@link MapMethods} and {@link MapRemoteOperations}.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specialized for the queried map
 * @see ChronicleMap#queryContext(Object)
 */
public interface MapQueryContext<K, V, R> extends HashQueryContext<K>, MapContext<K, V, R> {

    @Override
    @Nullable
    MapEntry<K, V> entry();

    @Override
    @Nullable
    MapAbsentEntry<K, V> absentEntry();
}
