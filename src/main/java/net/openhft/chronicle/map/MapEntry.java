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
import net.openhft.chronicle.hash.HashEntry;
import org.jetbrains.annotations.NotNull;

/**
 * A context of a <i>present</i> entry in the {@link ChronicleMap}.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @see MapEntryOperations
 * @see MapQueryContext#entry()
 */
public interface MapEntry<K, V> extends HashEntry<K> {
    @Override
    @NotNull
    MapContext<K, V, ?> context();

    /**
     * Returns the entry value.
     */
    @NotNull
    Data<V> value();

    /**
     * Replaces the entry's value with the given {@code newValue}.
     * <p>
     * <p>This method is the default implementation for {@link MapEntryOperations#replaceValue(
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
     * <p>This method is the default implementation for {@link MapEntryOperations#remove(MapEntry)},
     * which might be customized over the default.
     */
    @Override
    void doRemove();
}
