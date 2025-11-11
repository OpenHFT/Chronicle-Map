/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.HashSegmentContext;

/**
 * Context of {@link ChronicleMap}'s segment.
 *
 * @param <K> the key type of accessed {@code ChronicleMap}
 * @param <V> the value type of accessed {@code ChronicleMap}
 * @param <R> the return type of {@link MapEntryOperations} specified for the queried map
 * @see ChronicleMap#segmentContext(int)
 */
public interface MapSegmentContext<K, V, R>
        extends HashSegmentContext<K, MapEntry<K, V>>, MapContext<K, V, R> {
}
