//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ExternalHashQueryContext;

/**
 * {@link MapQueryContext} + {@link AutoCloseable}, for external {@link ChronicleMap} queries
 * in <i>try-with-resources</i> blocks.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations}, {@link
 *            ChronicleMapBuilder#entryOperations(MapEntryOperations) specified} for the queried {@code
 *            ChronicleMap}
 * @see ChronicleMap#queryContext(Object)
 */
public interface ExternalMapQueryContext<K, V, R>
        extends MapQueryContext<K, V, R>, ExternalHashQueryContext<K> {
}
