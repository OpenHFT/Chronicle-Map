/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.HashContext;
import net.openhft.chronicle.hash.Data;

/**
 * Context, in which {@link MapEntry MapEntries} are accessed. {@code MapContext} allows to access
 * {@link MapEntryOperations}, configured for the accessed {@link ChronicleMap} via {@link
 * ChronicleMapBuilder#entryOperations(MapEntryOperations)}. {@code MapContext} implements {@code
 * MapEntryOperations} by delegation to the configured {@code entryOperations}.
 * 
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specified for the queried map
 */
public interface MapContext<K, V, R> extends HashContext<K>, MapEntryOperations<K, V, R> {
    /**
     * Returns the accessed {@code ChronicleMap}. Synonym to {@link #map()}.
     */
    @Override
    default ChronicleMap<K, V> hash() {
        return map();
    }

    /**
     * Returns the accessed {@code ChronicleMap}. Synonym to {@link #hash()}.
     */
    ChronicleMap<K, V> map();

    /**
     * Wraps the given value as a {@code Data}. Useful when you need to pass a value
     * to some method accepting {@code Data}, for example, {@link MapEntryOperations#replaceValue(
     * MapEntry, Data)}, without allocating new objects (i. e. garbage) and {@code ThreadLocals}.
     *
     * <p>The returned {@code Data} object shouldn't outlive this {@code MapContext}.
     *
     * @param value the value object to wrap
     * @return the value as {@code Data}
     */
    Data<V> wrapValueAsData(V value);
}
