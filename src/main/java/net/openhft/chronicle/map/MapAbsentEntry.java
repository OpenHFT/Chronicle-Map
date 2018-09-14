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

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashAbsentEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Low-level operational context for the situations, when the new entry is going to be inserted
 * into the {@link ChronicleMap}.
 *
 * @param <K> type of the key in {@code ChronicleMap}
 * @param <V> type of the value in {@code ChronicleMap}
 * @see MapEntryOperations
 * @see MapQueryContext#absentEntry()
 */
public interface MapAbsentEntry<K, V> extends HashAbsentEntry<K> {

    @Override
    @NotNull
    MapContext<K, V, ?> context();

    /**
     * Inserts the new entry into the map, of {@linkplain #absentKey() the key} and the given {@code
     * value}.
     * <p>
     * <p>This method is the default implementation for {@link MapEntryOperations#insert(
     *MapAbsentEntry, Data)}, which might be customized over the default.
     *
     * @param value the value to insert into the map along with {@link #absentKey() the key}
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     *                               operation are not met
     * @see MapEntryOperations#insert(MapAbsentEntry, Data)
     */
    void doInsert(Data<V> value);

    /**
     * Returns the <i>default</i> (or <i>nil</i>) value, that should be inserted into the map in
     * this context. This is primarily used in {@link ChronicleMap#acquireUsing} operation
     * implementation, i. e. {@link MapMethods#acquireUsing}.
     * <p>
     * <p>This method if the default implementation for {@link
     * DefaultValueProvider#defaultValue(MapAbsentEntry)},
     * which might be customized over the default.
     *
     * @see DefaultValueProvider#defaultValue(MapAbsentEntry)
     */
    @NotNull
    Data<V> defaultValue();
}
