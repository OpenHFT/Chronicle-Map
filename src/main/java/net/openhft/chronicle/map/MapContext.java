/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashContext;

/**
 * Context, in which {@link MapEntry MapEntries} are accessed. {@code MapContext} allows to access
 * {@link MapEntryOperations}, configured for the accessed {@link ChronicleMap} via {@link
 * ChronicleMapBuilder#entryOperations(MapEntryOperations)}. {@code MapContext} implements {@code
 * MapEntryOperations} by delegation to the configured {@code entryOperations}. The same for
 * {@link DefaultValueProvider}: {@code MapContext} delegates to the {@code DefaultValueProvider}
 * configured for the accessed {@code ChronicleMap} via {@link
 * ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)}, or the default {@code
 * DefaultValueProvider} implementation.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specified for the queried map
 */
public interface MapContext<K, V, R>
        extends HashContext<K>, MapEntryOperations<K, V, R>, DefaultValueProvider<K, V> {
    /**
     * Returns the accessed {@code ChronicleMap}. Synonym to {@link #map()}.
     */
    @Override
    ChronicleHash<K, ?, ?, ?> hash();

    /**
     * Returns the accessed {@code ChronicleMap}. Synonym to {@link #hash()}.
     */
    ChronicleMap<K, V> map();

    /**
     * Wraps the given value as a {@code Data}. Useful when you need to pass a value
     * to some method accepting {@code Data}, for example, {@link MapEntryOperations#replaceValue(
     *MapEntry, Data)}, without allocating new objects (i. e. garbage) and {@code ThreadLocals}.
     * <p>
     * <p>The returned {@code Data} object shouldn't outlive this {@code MapContext}.
     *
     * @param value the value object to wrap
     * @return the value as {@code Data}
     */
    Data<V> wrapValueAsData(V value);

    /**
     * Wraps the given value bytes as a {@code Data}. Useful when you need to pass a value
     * to some method accepting {@code Data}, for example, {@link MapEntryOperations#replaceValue(
     *MapEntry, Data)}, without allocating manual deserialization and {@code ThreadLocals}.
     * <p>
     * <p>The returned {@code Data} object shouldn't outlive this {@code MapContext}.
     *
     * @param valueBytes the value bytes to wrap
     * @param offset     offset within the given valueBytes, the actual value bytes start from
     * @param size       length of the value bytes sequence
     * @return the value bytes as {@code Data}
     */
    Data<V> wrapValueBytesAsData(BytesStore valueBytes, long offset, long size);
}
