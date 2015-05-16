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

import net.openhft.chronicle.hash.Value;
import org.jetbrains.annotations.NotNull;

/**
 * Low-level operational context for the situations, when the new entry is going to be inserted
 * into the {@link ChronicleMap}.
 *
 * @param <K> type of the key in {@code ChronicleMap}
 * @param <V> type of the value in {@code ChronicleMap}
 * 
 * @see MapAbsentEntryOperations
 * @see MapQueryContext#absentEntry() 
 */
public interface MapAbsentEntry<K, V> {

    /**
     * Returns the context, in which the entry is going to be inserted into the map.
     */
    @NotNull MapContext<K, V> context();

    /**
     * Returns the key is going to be inserted into the {@code ChronicleMap}.
     */
    @NotNull Value<K, ?> absentKey();

    /**
     * Inserts the new entry into the map, of {@link #absentKey() the key} and the given value.
     * Returns {@code true} if the insertion  was successful, {@code false} if it failed for
     * any reason.
     *
     * <p>This method if the default implementation for {@link MapAbsentEntryOperations#insert},
     * which might be customized over the default.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform insertion
     * operation are not met
     */
    boolean defaultInsert(Value<V, ?> value);

    /**
     * Returns the "nil" value, which should be inserted into the map in this context. This
     * is primarily used in {@link ChronicleMap#acquireUsing} operation implementation, i. e. {@link
     * MapMethods#acquireUsing}.
     *
     * <p>This method if the default implementation for {@link
     * MapAbsentEntryOperations#defaultValue}, which might be customized over the default.
     */
    @NotNull Value<V, ?> defaultValue();
}
